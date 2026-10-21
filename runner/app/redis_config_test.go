// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TETRIZ_REDIS_URL takes precedence over TETRIZ_REDIS_ADDR and
// TETRIZ_REDIS_PASSWORD, and carries a user, a password, a database and TLS.
func TestRedisOptionsFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name, addr, password, url string
		wantAddr, wantUser        string
		wantPass                  string
		wantDB                    int
		wantTLS                   bool
	}{
		{name: "addr only", addr: "cache:6379", wantAddr: "cache:6379"},
		{name: "addr and password", addr: "cache:6379", password: "s3cret", wantAddr: "cache:6379", wantPass: "s3cret"},
		{name: "default addr", wantAddr: "localhost:56379"},
		{name: "url wins over addr and password", addr: "cache:6379", password: "ignored", url: "redis://alice:pw@redis.internal:6380/3",
			wantAddr: "redis.internal:6380", wantUser: "alice", wantPass: "pw", wantDB: 3},
		{name: "rediss is TLS", url: "rediss://:pw@secure.example:6390/0", wantAddr: "secure.example:6390", wantPass: "pw", wantTLS: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeClearEnv(t)
			t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
			t.Setenv("TETRIZ_REDIS_ADDR", tc.addr)
			t.Setenv("TETRIZ_REDIS_PASSWORD", tc.password)
			t.Setenv("TETRIZ_REDIS_URL", tc.url)
			cfg, err := FromEnv()
			require.NoError(t, err)
			o, err := cfg.RedisOptions()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAddr, o.Addr)
			assert.Equal(t, tc.wantUser, o.Username)
			assert.Equal(t, tc.wantPass, o.Password)
			assert.Equal(t, tc.wantDB, o.DB)
			assert.Equal(t, tc.wantTLS, o.TLSConfig != nil)
		})
	}
}

// A TETRIZ_REDIS_URL that does not parse, or a TETRIZ_NAMESPACE with a
// character asynq keys cannot carry, fails FromEnv and
// FromEnvWithoutCredentials.
func TestBadRedisURLOrNamespaceFailsFromEnv(t *testing.T) {
	for _, tc := range []struct{ name, url, ns, want string }{
		{"http scheme", "http://redis:6379", "", "TETRIZ_REDIS_URL"},
		{"bad db", "redis://redis:6379/notanumber", "", "TETRIZ_REDIS_URL"},
		{"namespace with a colon", "", "a:b", "TETRIZ_NAMESPACE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeClearEnv(t)
			t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
			t.Setenv("TETRIZ_REDIS_URL", tc.url)
			t.Setenv("TETRIZ_NAMESPACE", tc.ns)
			_, err := FromEnv()
			require.ErrorContains(t, err, tc.want)
			_, err = FromEnvWithoutCredentials()
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// The client built from the options authenticates to a Redis that requires
// a password, both from the URL and from TETRIZ_REDIS_PASSWORD.
func TestRedisOptionsAuthenticate(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cret")
	for _, cfg := range []Config{
		{RedisURL: "redis://:s3cret@" + mr.Addr() + "/0"},
		{RedisAddr: mr.Addr(), RedisPassword: "s3cret"},
	} {
		o, err := cfg.RedisOptions()
		require.NoError(t, err)
		c := redis.NewClient(o)
		assert.NoError(t, c.Ping(context.Background()).Err())
		_ = c.Close()
	}
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = c.Close() }()
	assert.Error(t, c.Ping(context.Background()).Err(), "without the password the server refuses")
}
