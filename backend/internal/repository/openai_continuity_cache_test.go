package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestOpenAIContinuityCacheFencing(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := &gatewayCache{rdb: client}
	ctx := context.Background()
	ok, err := cache.AcquireContinuityLease(ctx, 1, "openai:u1:session", "first", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.AcquireContinuityLease(ctx, 1, "openai:u1:session", "second", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = cache.CommitContinuityBinding(ctx, 1, "openai:u1:session", "first", 10, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.AcquireContinuityLease(ctx, 1, "openai:u1:session", "second", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.CommitContinuityBinding(ctx, 1, "openai:u1:session", "first", 10, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = cache.CommitContinuityBinding(ctx, 1, "openai:u1:session", "second", 20, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.CommitContinuityBinding(ctx, 1, "openai:u1:session", "first", 10, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	id, err := cache.GetSessionAccountID(ctx, 1, "openai:u1:session")
	require.NoError(t, err)
	require.EqualValues(t, 20, id)
	// A different user with the same client session never shares the binding.
	_, err = cache.GetSessionAccountID(ctx, 1, "openai:u2:session")
	require.Error(t, err)
}
func TestOpenAIContinuityExpiredLeaseCannotCommit(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := &gatewayCache{rdb: client}
	ctx := context.Background()
	ok, err := cache.AcquireContinuityLease(ctx, 1, "s", "old", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	server.FastForward(2 * time.Second)
	ok, err = cache.CommitContinuityBinding(ctx, 1, "s", "old", 1, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = cache.AcquireContinuityLease(ctx, 1, "s", "new", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseContinuityLease(ctx, 1, "s", "old"))
	ok, err = cache.RenewContinuityLease(ctx, 1, "s", "new", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestOpenAIContinuityConcurrentFirstTurnClaimsOneOwner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	cache := &gatewayCache{rdb: client}
	results := make(chan bool, 16)
	for range 16 {
		go func() {
			ok, err := cache.AcquireContinuityLease(context.Background(), 1, "session", time.Now().String(), time.Minute)
			results <- ok && err == nil
		}()
	}
	winners := 0
	for range 16 {
		if <-results {
			winners++
		}
	}
	require.Equal(t, 1, winners)
}
