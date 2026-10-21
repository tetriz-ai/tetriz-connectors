// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/ratelimiter/metrics"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

type SlidingWindowLogWithRedisImpl struct {
	redisClient *redis.Client
}

func NewSlidingWindowLogWithRedis(redisClient *redis.Client) *SlidingWindowLogWithRedisImpl {
	return &SlidingWindowLogWithRedisImpl{redisClient: redisClient}
}

var _ ratelimiter.Store = &SlidingWindowLogWithRedisImpl{}

// lua redis scripts run atomically (in a txn block) and help in conditional updates, which is essential
// for preventing race conditions while accessing and updating same sorted set
// Doc : https://redis.io/commands/eval
var (
	// Using redis.NewScript instead of direct lua script because
	// Eval loads and compiles the same script body on each run while script.
	// Run uses a cached instance of script each time and reduces the overall run time by avoiding load and compile steps.
	// https://redis.io/commands/eval#bandwidth-and-evalsha
	// https://redis.io/commands/script-load
	rateLimiterLuaScript = redis.NewScript(`local setname = KEYS[1]
	local from = ARGV[1]
	local to = ARGV[2]
	local score = tonumber(ARGV[3])
	local member = tostring(ARGV[4])
	local limit = tonumber(ARGV[5])
	local windowsize = redis.call('ZCOUNT', setname, from, to)
	if windowsize < limit then
	redis.call('ZADD', setname, score, member)
	end
	return limit - windowsize`)

	multiRateLimitLuaScript = redis.NewScript(`
local identifier = ARGV[1]
for i = 1, #KEYS do
    local score = tonumber(ARGV[(i-1)*4 + 2])
    local fromTime = tonumber(ARGV[(i-1)*4 + 3])
    local toTime = tonumber(ARGV[(i-1)*4 + 4])
    local maxRequest = tonumber(ARGV[(i-1)*4 + 5])
 	local key = KEYS[i]
    local currentCount = redis.call("ZCOUNT", key, fromTime, toTime)

    if tonumber(currentCount) >= maxRequest then
        return i -- Rate limit exceeded, return index for logging/debugging
    end
end
-- rate limit is not reached, add the timestamps to the respective sets
for i = 1, #KEYS do
    local score = tonumber(ARGV[(i-1)*4 + 2])
    local fromTime = tonumber(ARGV[(i-1)*4 + 3])
    local toTime = tonumber(ARGV[(i-1)*4 + 4])
    local maxRequest = tonumber(ARGV[(i-1)*4 + 5])
 	local key = KEYS[i]
	redis.call("ZADD", key, score, identifier)
end
return 0 -- All within rate limit, return 0 for success
`)

	bulkAddLua = redis.NewScript(`local setName = KEYS[1]
local arguments = ARGV
local periodNanos = tonumber(arguments[1])
local rateLimit = tonumber(arguments[2])
local members = {}
for i = 1 , #arguments - 2 do
	members[i] = arguments[i+2]
end
local curTime = redis.call('TIME')
if type(curTime) == 'table' and curTime.err then
	return 0
end
local curNanos = (curTime[1]*1000000 + curTime[2]) * 1000
local fromTimeNanos = curNanos - periodNanos
local windowsize = redis.call('ZCOUNT', setName, tostring(fromTimeNanos), tostring(curNanos))
if type(windowsize) == 'table' and windowsize.err then
	return 0
end
local toBook = rateLimit - windowsize
if toBook > #members then
	toBook = #members
end
local ret = {}
ret[1] = curNanos
if toBook > 0 then
	local tab = {}
	tab[1] = "ZADD"
	tab[2] = setName
	local index = 3
	for i = 1 , toBook do
		tab[index] = curNanos
		tab[index+1] = members[i]
		index = index + 2
	end
	local added = redis.call(unpack(tab))
	if type(added) == 'table' and added.err then
		return 0
	end
	ret[2] = added
	return ret
end
ret[2] = 0
return ret`)

	bulkRemoveLua = redis.NewScript(`
	local setName = KEYS[1]
	local score = ARGV[1]
	local requiredRemoval = tonumber(ARGV[2])
	local uuidList = redis.call("ZRANGEBYSCORE", setName, score, score)
	if type(uuidList) == 'table' and uuidList.err then
	  return 0
	end
	-- remove all elements from sorted set which have score as score passed
	local removed = redis.call("ZREMRANGEBYSCORE", setName, score, score)
	if type(removed) == 'table' and removed.err then
	  return 0
	end
	-- Add extra removed elements back to sorted set
	local toAdd = removed - requiredRemoval
	if toAdd <= 0 then
		return removed
	end
	if toAdd > 0 then
		local tab = {}
		tab[1] = "ZADD"
		tab[2] = setName
		local index = 3
		for i = 1, toAdd do
			tab[index] = score
			tab[index+1] = uuidList[i]
			index = index + 2
		end
		local added = redis.call(unpack(tab))
		if type(added) == 'table' and added.err then
			return removed
		end
		return removed - added
	end`)

	/*
		addToSlidingWindowLogWithPriorityLua implements sliding window log to book slots for requests with certain
		priority. It maintains one sorted set per resourceKey <> priority combination to track the logs of requests
		within a window and accordingly allow and disallow the requests.

		While booking slots for a request with certain priority if slots are booked at the same level then slots
		are blocked from lower level if available, however, vice-versa is prohibited. This algorithm favours higher
		priority requests and hence there is a possibility of lower priority getting starved.

		KEYS = [RESOURCE_KEY]
		ARGV = [FROM_TIME, TO_TIME, SCORE, MEMBER, PRIORITY_LIMIT_MAP, PRIORITY]
		Returns 0 when nothing was booked, else 1 + the priority whose set was booked.
	*/
	addToSlidingWindowLogWithPriorityLua = redis.NewScript(`
	local resource_key = KEYS[1]
	local from = ARGV[1]
	local to = ARGV[2]
	local score = tonumber(ARGV[3])
	local member = tostring(ARGV[4])
	local priority_limit_map = cjson.decode(ARGV[5])
	local priority = tonumber(ARGV[6])

	local function block_slot_with_priority(priority)
		priority = tostring(priority)
		local resource_key_with_priority = table.concat({resource_key, priority}, ":")
		local window_size = redis.call('ZCOUNT', resource_key_with_priority, from, to)
		if window_size < priority_limit_map[priority] then
			return redis.call('ZADD', resource_key_with_priority, score, member)
		end
		return 0
	end

	if priority == 0 then
		local booked = block_slot_with_priority(0)
		if booked == 1 then
			return 1
		end
	end

	if priority <= 1 then
		local booked = block_slot_with_priority(1)
		if booked == 1 then
			return 2
		end
	end

	if priority <= 2 then
		local booked = block_slot_with_priority(2)
		if booked == 1 then
			return 3
		end
	end

	return 0
	`)

	cleanAndExpireLuaScript = redis.NewScript(`
-- Lua script to set different TTLs for multiple keys in Redis and remove keys older than specified time thresholds
-- KEYS: { key1, key2, ... }
-- ARGV: { ttl1, timeThreshold1, ttl2, timeThreshold2, ... }

local num_keys = #KEYS
local num_args = #ARGV

-- Ensure the number of arguments is even
if num_args % 2 ~= 0 then
  return redis.error_reply("The number of arguments must be even.")
end

-- Ensure the number of keys matches the number of TTLs and time thresholds
if num_keys * 2 ~= num_args then
  return redis.error_reply("The number of keys must match the number of TTLs and time thresholds.")
end

for i = 1, num_keys do
  local key = KEYS[i]
  local ttl = tonumber(ARGV[i * 2 - 1])
  local timeThreshold = tonumber(ARGV[i * 2])  -- Time threshold for removing keys in seconds

  if ttl <= 0 then
    return redis.error_reply("TTL must be a positive integer.")
  end

  -- Set TTL for the key
  if redis.call("EXISTS", key) == 1 then
    redis.call("EXPIRE", key, ttl)
  end

  -- Remove keys older than the specified time threshold
  redis.call("ZREMRANGEBYSCORE", key, "-inf", timeThreshold)
end

return redis.status_reply("OK")
`)
)

/* Redis sorted set contains elements for a set in the form of {score, member} where each member has a score
// NOTE : Members are string always and scores can be number/integers etc. We are using timestamps at which the request
// arrives as score and member as unique UUID so that if concurrent requests come we are able to create a new element
// in our sorted set for all these new requests. We did this to leverage the ZREMRANGEBYSCORE and ZCOUNT functions of redis
// ZCOUNT counts number of elements which have score in the range {min, max} which is exactly we want since our
// scores are timestamps at which requests arrive.
*/

// RemoveKeysBefore Remove all the keys from the sorted set which are no longer in our range of consideration from the set
func (r *SlidingWindowLogWithRedisImpl) RemoveKeysBefore(ctx context.Context, key string, removeBefore time.Time) error {
	curTime := time.Now()
	// Remove all the keys from the set which are expired
	_, remErr := r.redisClient.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(removeBefore.UnixNano(), 10)).Result()
	if remErr != nil {
		return errors.Wrap(remErr, fmt.Sprintf("error removing old keys from redis for key : %v", key))
	}
	metrics.RecordKeyRemoveTime(float64(time.Since(curTime).Milliseconds()))
	return nil
}

// IsWindowCountReached run the redis lua script atomically to get window count and add key if permissible
func (r *SlidingWindowLogWithRedisImpl) IsWindowCountReached(ctx context.Context, key string, fromTime, toTime time.Time, maxRequest int64) (bool, error) {
	curTime := time.Now()
	defer func() { metrics.RecordIsWindowBreachedTime(float64(time.Since(curTime).Milliseconds())) }()
	diff, err := rateLimiterLuaScript.Run(ctx, r.redisClient, []string{key}, strconv.FormatInt(fromTime.UnixNano(), 10),
		strconv.FormatInt(toTime.UnixNano(), 10), float64(toTime.UnixNano()),
		uuid.New().String(), maxRequest).Result()
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("error checking window count and adding key to sorted set for %v", key))
	}
	val, ok := diff.(int64)
	if ok {
		if val > 0 {
			return true, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("cannot type cast diff to int64 for %v", key)
}

// RefreshKeyTTL To avoid ever growing set of keys in redis, we set TTL at a key level. In case requests keep on coming
// TTL will keep on getting refreshed, in case requests do not come for the defined duration key (sorted set) gets
// deleted from redis
func (r *SlidingWindowLogWithRedisImpl) RefreshKeyTTL(ctx context.Context, key string, period time.Duration) error {
	curTime := time.Now()
	defer func() { metrics.RecordKeyExpireTime(float64(time.Since(curTime).Milliseconds())) }()

	_, err := r.redisClient.Expire(ctx, key, period).Result()
	if err != nil {
		return errors.Wrap(err, "error setting expiry for key in redis")
	}

	return nil
}

// AddMaxN N slots are added as tuples of {member, score} inside the redis sorted set. score is the current time nanoseconds
// generated inside the lua script itself to avoid overbooking due to concurrent calls from client in multiple go routines
// member is a random unique UUID passed to the lua script as samples of list size addN.
func (r *SlidingWindowLogWithRedisImpl) AddMaxN(ctx context.Context, key string, addN, maxRequestAllowed, durationNanos int64) (int64, int64, error) {
	var argList []string
	// window duration, ARGV[1] , rate limit configured, ARGV[2]
	argList = append(argList, strconv.FormatInt(durationNanos, 10), strconv.FormatInt(maxRequestAllowed, 10))
	// Add UUID's to be used as members
	for i := int64(0); i < addN; i++ {
		argList = append(argList, uuid.New().String())
	}
	res, err := bulkAddLua.Run(ctx, r.redisClient,
		// sorted set name, KEY
		[]string{key},
		// ARGV
		argList).
		Result()
	if err != nil {
		return 0, 0, errors.Wrap(err, fmt.Sprintf("error checking window count and adding bulk slots to sorted set for %v", key))
	}
	ret, ok := res.([]interface{})
	if !ok || len(ret) < 2 {
		return 0, 0, fmt.Errorf("unexpected lua result shape for %v: %T", key, res)
	}
	used, usedOK := ret[1].(int64)
	added, addedOK := ret[0].(int64)
	if !usedOK || !addedOK {
		return 0, 0, fmt.Errorf("unexpected lua result element types for %v", key)
	}
	return used, added, nil
}

// RemoveMaxN N slots are removed from sorted set named config key in the form of tuple {addedTimeNanos, uuid}
// The lua script gets all the available tuples for the score first. Since sorted set does not support partial deletion on
// score we remove all the entries and add back the needed entries. For example If score s has 10 entries and we need to remove
// only 4, we first remove 10 and then add back 6 to the set.
// Since this method executes lua script atomically for all the parallel threads we do not experience contention problem
func (r *SlidingWindowLogWithRedisImpl) RemoveMaxN(ctx context.Context, key string, addedTimeNanos int64, removeN int64) (int64, error) {
	removed, err := bulkRemoveLua.Run(ctx, r.redisClient,
		// sorted set name, KEY
		[]string{key},
		// Arguments
		strconv.FormatInt(addedTimeNanos, 10),
		strconv.FormatInt(removeN, 10)).
		Result()
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("error removing bulk slots from sorted set for %v", key))
	}
	removedCount, ok := removed.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected lua result type for %v: %T", key, removed)
	}
	return removedCount, nil
}

func (r *SlidingWindowLogWithRedisImpl) AddWithPriority(
	ctx context.Context,
	key string,
	fromTime, toTime time.Time,
	maxRequestForPriority map[ratelimiter.Priority]int64,
	priority ratelimiter.Priority,
	expiry time.Duration,
) (bool, error) {
	curTime := time.Now()
	defer func() { metrics.RecordAddWithPriorityTime(float64(time.Since(curTime).Milliseconds())) }()

	// we need to serialise this map as redis doesn't expos any better way to pass map as ARGV
	serializedMaxReqForPriority, err := json.Marshal(maxRequestForPriority)
	if err != nil {
		return false, errors.Wrap(err, "failed to serialise max request for priority map")
	}

	booked, err := addToSlidingWindowLogWithPriorityLua.Run(ctx, r.redisClient, []string{key}, strconv.FormatInt(fromTime.UnixNano(), 10),
		strconv.FormatInt(toTime.UnixNano(), 10), float64(toTime.UnixNano()),
		uuid.New().String(), serializedMaxReqForPriority, int(priority)).Result()
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("error checking window count and adding key to sorted set for %v", key))
	}
	val, ok := booked.(int64)
	if !ok {
		return false, fmt.Errorf("cannot type cast booked to int64 for %v", key)
	}

	if val > 0 {
		// The script returns 1 + the priority whose set it booked, which is
		// lower than the requested one when the request spilled down. That set
		// is the one written, so it is the one whose TTL is refreshed; refreshing
		// the requested priority's set instead would leave the spill set with
		// no expiry.
		bookedKey := getKeyWithPriority(key, ratelimiter.Priority(val-1))
		// This can be done in separate go routine as it does not affect functionality of rate limiter
		goroutine.RunWithDefaultTimeout(reqctx.CloneCtx(ctx), func(ctx context.Context) {
			if refreshErr := r.RefreshKeyTTL(ctx, bookedKey, expiry); refreshErr != nil {
				logger.Error(ctx, "error refreshing key expiry in store", zap.String("key", key), zap.Error(refreshErr))
			}
		})

		return true, nil
	}

	return false, nil
}

func (r *SlidingWindowLogWithRedisImpl) IsWindowsCountReached(ctx context.Context, windowsParams []*ratelimiter.WindowParams, enableLogging bool) (bool, error) {
	var keys []string
	var args []interface{}
	// appending the first argument, which will be the unique identifier of this bulk call
	args = append(args, uuid.NewString())
	for _, wp := range windowsParams {
		keys = append(keys, wp.Key)
		args = append(args, strconv.FormatInt(wp.ToTime.UnixNano(), 10), // score (set as to time)
			strconv.FormatInt(wp.FromTime.UnixNano(), 10), // from time
			strconv.FormatInt(wp.ToTime.UnixNano(), 10),   // to time
			wp.MaxRequest) // max requests between from and to time
	}
	diff, err := multiRateLimitLuaScript.Run(ctx, r.redisClient, keys, args...).Result()
	if err != nil {
		return false, fmt.Errorf("error checking window count and adding keys to sorted set for %v: error: %w", keys, err)
	}
	val, ok := diff.(int64)
	if !ok {
		return false, fmt.Errorf("cannot type cast diff to int64 for diff: %v, keys: %v", val, keys)
	}

	// val is zero in case window count is not reached.
	windowCountBreached := val != 0
	if enableLogging && windowCountBreached && int(val-1) < len(keys) {
		// redis script returns index value that starts from 1
		logger.Info(ctx, "rate limiting window count breached", zap.String(logger.RedisKey, keys[val-1]), zap.Int64(logger.ArrayIndex, val-1))
	}
	return windowCountBreached, nil
}

// CleanAndExpire sets time to live for a key and removes all the elements in the respective
// set that are older than cleanBefore time. Multiple keys are passed and the script is run atomically.
func (r *SlidingWindowLogWithRedisImpl) CleanAndExpire(ctx context.Context, params []*ratelimiter.CleanAndExpireParams) error {
	var keys []string
	var args []interface{}
	for _, param := range params {
		keys = append(keys, param.Key)
		// args for the lua script are in the format ttl1, cleanUpTill1, ttl2, cleanUpTill2...
		args = append(args, getTTLToStore(param.TTL), param.CleanBefore.UnixNano())
	}
	diff, err := cleanAndExpireLuaScript.Run(ctx, r.redisClient, keys, args...).Result()
	if err != nil || diff != "OK" {
		return fmt.Errorf("could not clean and expire keys: %v error: %w", keys, err)
	}
	return nil
}

func getKeyWithPriority(key string, priority ratelimiter.Priority) string {
	return key + ":" + priority.String()
}

func getTTLToStore(ttl time.Duration) int64 {
	ttlStore := int64(ttl / time.Second)
	if ttlStore == 0 {
		return 1
	}
	return ttlStore
}
