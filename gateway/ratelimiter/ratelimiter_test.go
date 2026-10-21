// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
)

var rateLimiterTs *RateLimiterTestSuite

type RateLimiterTestSuite struct {
	store ratelimiter.Store
}

func NewRateLimiterTestSuite(store ratelimiter.Store) *RateLimiterTestSuite {
	return &RateLimiterTestSuite{store: store}
}

func TestRateLimiterImpl_Allow(t *testing.T) {
	type mockRequestWithPriority struct {
		enable         bool
		high           int64
		medium         int64
		low            int64
		rangeStartTime time.Time
		rangeEndTime   time.Time
	}

	type args struct {
		resource          string
		requestIdentifier string
		priority          ratelimiter.Priority
	}
	tests := []struct {
		name                    string
		rateLimitConf           *ratelimiter.RateLimitConfig
		mockRequestWithPriority *mockRequestWithPriority
		clk                     clockwork.Clock
		args                    args
		booked                  bool
		wantErr                 bool
	}{
		{
			name: "should book slots for higher priority request only",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           5,
				medium:         2,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityHigh,
			},
			booked:  true,
			wantErr: false,
		},
		{
			name: "should book slots for medium priority request only",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           6,
				medium:         1,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityMedium,
			},
			booked:  true,
			wantErr: false,
		},
		{
			name: "should book slots for low priority request only",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           6,
				medium:         2,
				low:            1,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityLow,
			},
			booked:  true,
			wantErr: false,
		},
		{
			name: "should fail to book slots irrespective of priority as all the requests slots are filled",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           6,
				medium:         2,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityHigh,
			},
			booked:  false,
			wantErr: false,
		},
		{
			name: "should book slot from medium priority incase slots for higher priority request are filled",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           6,
				medium:         1,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityHigh,
			},
			booked:  true,
			wantErr: false,
		},
		{
			name: "should book from low priority incase slots for higher and medium priority request are filled",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           6,
				medium:         2,
				low:            1,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityHigh,
			},
			booked:  true,
			wantErr: false,
		},
		{
			name: "shouldn't book slot for medium priority with higher priority slot being available",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           5,
				medium:         2,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityMedium,
			},
			booked:  false,
			wantErr: false,
		},
		{
			name: "shouldn't book slot for low priority with higher priority slot being available",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: time.Second,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   60,
							MediumPercent: 20,
							LowPercent:    20,
						},
					},
				},
				Namespace: "test",
			},
			mockRequestWithPriority: &mockRequestWithPriority{
				enable:         true,
				high:           5,
				medium:         1,
				low:            2,
				rangeStartTime: time.Date(2022, time.July, 7, 14, 12, 0, 0, testZone),
				rangeEndTime:   time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone),
			},
			clk: clockwork.NewFakeClockAt(time.Date(2022, time.July, 7, 14, 12, 1, 0, testZone)),
			args: args{
				resource:          "r1",
				requestIdentifier: "abc",
				priority:          ratelimiter.PriorityLow,
			},
			booked:  false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			genCfg := tt.rateLimitConf

			if tt.mockRequestWithPriority.enable {
				key := genCfg.GetKeyWithNamespace(tt.args.resource, tt.args.requestIdentifier)
				cleanUp := populateRatelimitSlots(key, 0, tt.mockRequestWithPriority.rangeStartTime, tt.mockRequestWithPriority.rangeEndTime, tt.mockRequestWithPriority.high)
				defer cleanUp()
				cleanUp1 := populateRatelimitSlots(key, 1, tt.mockRequestWithPriority.rangeStartTime, tt.mockRequestWithPriority.rangeEndTime, tt.mockRequestWithPriority.medium)
				defer cleanUp1()
				cleanUp2 := populateRatelimitSlots(key, 2, tt.mockRequestWithPriority.rangeStartTime, tt.mockRequestWithPriority.rangeEndTime, tt.mockRequestWithPriority.low)
				defer cleanUp2()
			}

			r := ratelimiter.NewRateLimiter(rateLimiterTs.store, genCfg, ratelimiter.WithClock(tt.clk))
			got, err := r.AcquireWithPriority(context.Background(), tt.args.resource, tt.args.requestIdentifier, tt.args.priority)
			if (err != nil) != tt.wantErr {
				t.Errorf("Allow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.booked {
				t.Errorf("Allow() got = %v, booked %v", got, tt.booked)
			}
		})
	}
}

func TestRateLimiterImpl_AllowAtMaxNSequence(t *testing.T) {
	type callSequence struct {
		wait               time.Duration
		resource           string
		resourceIdentifier string
		addN               int64
		want               int64
	}

	tests := []struct {
		name          string
		opts          []ratelimiter.Option
		sequence      []*callSequence
		rateLimitConf *ratelimiter.RateLimitConfig
		clk           clockwork.Clock
	}{
		{
			name: "test empty slot with cleanup step",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   10,
						Period: 300 * time.Millisecond,
					},
				},
				Namespace: "AllowAtMaxNSequenceTest",
			},
			opts: []ratelimiter.Option{
				ratelimiter.WithCleanupInterval(0),
				ratelimiter.WithCleanupBuffer(0),
			},
			sequence: []*callSequence{
				{
					wait:               0,
					addN:               10,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               10,
				},
				{
					wait:               10 * time.Millisecond,
					addN:               10,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               0,
				},
				{
					wait:               300 * time.Millisecond,
					addN:               10,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)
			ratelimitGenCfg := tt.rateLimitConf
			r := ratelimiter.NewRateLimiter(rateLimiterTs.store, ratelimitGenCfg, tt.opts...)
			for _, seq := range tt.sequence {
				time.Sleep(seq.wait)
				booked, _, err := r.AcquireUptoN(context.Background(), seq.resource, seq.resourceIdentifier, seq.addN, false)
				a.NoError(err, "failed to book slots")
				a.Equal(seq.want, booked)
			}
		})
	}
}

func TestRateLimiterImpl_AllowSequence(t *testing.T) {
	type callSequence struct {
		wait               time.Duration
		resource           string
		resourceIdentifier string
		want               bool
	}

	tests := []struct {
		name          string
		opts          []ratelimiter.Option
		sequence      []*callSequence
		rateLimitConf *ratelimiter.RateLimitConfig
	}{
		{
			name: "test empty slot with cleanup step",
			rateLimitConf: &ratelimiter.RateLimitConfig{
				ResourceMap: map[string]*ratelimiter.RateLimit{
					"r1": {
						Rate:   1,
						Period: 300 * time.Millisecond,
						PriorityDistribution: &ratelimiter.PriorityDistribution{
							Enabled:       true,
							HighPercent:   0,
							MediumPercent: 0,
							LowPercent:    100,
						},
					},
				},
				Namespace: "AllowSequenceTest",
			},
			opts: []ratelimiter.Option{
				ratelimiter.WithCleanupInterval(0),
				ratelimiter.WithCleanupBuffer(0),
			},
			sequence: []*callSequence{
				{
					wait:               0,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               true,
				},
				{
					wait:               10 * time.Millisecond,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               false,
				},
				{
					wait:               300 * time.Millisecond,
					resource:           "r1",
					resourceIdentifier: "ID1",
					want:               true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)
			ratelimitGenCfg := tt.rateLimitConf
			r := ratelimiter.NewRateLimiter(rateLimiterTs.store, ratelimitGenCfg, tt.opts...)
			for _, seq := range tt.sequence {
				time.Sleep(seq.wait)
				booked, err := r.AcquireWithPriority(context.Background(), seq.resource, seq.resourceIdentifier, ratelimiter.PriorityLow)
				a.NoError(err, "failed to book slots")
				a.Equal(seq.want, booked)
			}
		})
	}
}

func populateRatelimitSlots(key string, prioritySlot int64, startTime, endTime time.Time, slots int64) func() {
	if slots == 0 {
		return func() {}
	}

	keyWithPriority := fmt.Sprintf("%s:%d", key, prioritySlot)

	for i := int64(0); i < slots; i++ {
		randGen := rand.New(rand.NewSource(time.Now().UnixNano())) // nolint:gosec
		redisClient.ZAdd(context.Background(), keyWithPriority, redis.Z{
			Score:  float64(randTime(randGen, startTime, endTime)),
			Member: uuid.NewString(),
		})
	}

	return func() {
		redisClient.Del(context.Background(), keyWithPriority)
	}
}

func randTime(randGen *rand.Rand, start, end time.Time) int64 {
	diff := end.UnixNano() - start.UnixNano()
	nanoSeconds := randGen.Int63n(diff)
	return start.Add(time.Nanosecond * time.Duration(nanoSeconds)).UnixNano()
}

// testZone is a fixed +02:00 zone, so the fixtures read the same on any host.
var testZone = time.FixedZone("UTC+0200", 2*3600)
