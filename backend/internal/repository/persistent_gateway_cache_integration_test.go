//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func persistentContinuityFixture(t *testing.T) (*persistentGatewayCache, *miniredis.Miniredis, string) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	user := mustCreateUser(t, testEntClient(t), &service.User{Email: uuid.NewString() + "@example.com"})
	key := fmt.Sprintf("openai:u%d:%s", user.ID, service.DeriveSessionHashFromSeed(uuid.NewString()))
	cache, ok := NewPersistentGatewayCache(rdb, integrationDB).(*persistentGatewayCache)
	require.True(t, ok)
	return cache, mr, key
}

func TestPersistentContinuitySurvivesTTLAndStaleRedis(t *testing.T) {
	c, mr, key := persistentContinuityFixture(t)
	ctx := context.Background()
	ok, err := c.AcquireContinuityLease(ctx, 0, key, "first", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.CommitContinuityBinding(ctx, 0, key, "first", 101, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	mr.FastForward(25 * time.Hour)
	require.False(t, mr.Exists(buildSessionKey(0, key)))
	// A new service instance has no process-local state and still recovers.
	restarted := NewPersistentGatewayCache(c.rdb, integrationDB)
	id, err := restarted.GetSessionAccountID(ctx, 0, key)
	require.NoError(t, err)
	require.EqualValues(t, 101, id)
	leases, supportsLeases := restarted.(service.OpenAIContinuityCache)
	require.True(t, supportsLeases)
	ok, err = leases.AcquireContinuityLease(ctx, 0, key, "second", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = leases.CommitContinuityBinding(ctx, 0, key, "second", 202, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	// A delayed mirror write cannot supersede the newer durable owner.
	require.NoError(t, c.gatewayCache.SetSessionAccountID(ctx, 0, key, 101, time.Hour))
	id, err = c.GetSessionAccountID(ctx, 0, key)
	require.NoError(t, err)
	require.EqualValues(t, 202, id)
	mr.FlushAll()
	id, err = c.GetSessionAccountID(ctx, 0, key)
	require.NoError(t, err)
	require.EqualValues(t, 202, id)
	_, err = c.GetSessionAccountID(ctx, 1, key)
	require.ErrorIs(t, err, service.ErrStickySessionNotFound)
	// Successful ownership is available even with Redis disconnected.
	require.NoError(t, c.rdb.Close())
	id, err = c.GetSessionAccountID(ctx, 0, key)
	require.NoError(t, err)
	require.EqualValues(t, 202, id)
}

func TestPersistentContinuityFencesExpiredAndLateCompletions(t *testing.T) {
	c, mr, key := persistentContinuityFixture(t)
	ctx := context.Background()
	ok, err := c.AcquireContinuityLease(ctx, 0, key, "old", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	user, hash, _ := persistentOpenAISession(key)
	_, err = integrationDB.ExecContext(ctx, `UPDATE openai_session_bindings
		SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE user_id=$1 AND session_hash=$2`, user, hash)
	require.NoError(t, err)
	ok, err = c.RenewContinuityLease(ctx, 0, key, "old", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = c.CommitContinuityBinding(ctx, 0, key, "old", 101, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = c.CheckContinuityBinding(ctx, 0, key, "old", 101)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = c.AcquireContinuityLease(ctx, 0, key, "new", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	mr.FlushAll() // Cache loss cannot remove the fencing token.
	require.NoError(t, c.ReleaseContinuityLease(ctx, 0, key, "old"))
	ok, err = c.CheckContinuityBinding(ctx, 0, key, "new", 202)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.CommitContinuityBinding(ctx, 0, key, "new", 202, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.CommitContinuityBinding(ctx, 0, key, "old", 101, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = c.AcquireContinuityLease(ctx, 0, key, "failed", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, c.ReleaseContinuityLease(ctx, 0, key, "failed"))
	// Even a failed newer request fences an earlier completion forever.
	ok, err = c.CommitContinuityBinding(ctx, 0, key, "new", 101, time.Hour)
	require.NoError(t, err)
	require.False(t, ok)
	id, err := c.GetSessionAccountID(ctx, 0, key)
	require.NoError(t, err)
	require.EqualValues(t, 202, id, "failed recovery must preserve the last success")
}

func TestPersistentContinuityConcurrentRecoveryAndDBFailure(t *testing.T) {
	c, _, key := persistentContinuityFixture(t)
	ctx := context.Background()
	const workers = 16
	type outcome struct {
		ok  bool
		err error
	}
	results := make(chan outcome, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.AcquireContinuityLease(ctx, 0, key, uuid.NewString(), time.Minute)
			results <- outcome{ok, err}
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for result := range results {
		require.NoError(t, result.err)
		if result.ok {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	closedDB, err := sql.Open("postgres", "")
	require.NoError(t, err)
	require.NoError(t, closedDB.Close())
	c.db = closedDB
	require.NoError(t, c.gatewayCache.SetSessionAccountID(ctx, 0, key, 999, time.Hour))
	_, err = c.GetSessionAccountID(ctx, 0, key)
	require.Error(t, err, "DB failure must not trust a potentially stale Redis owner")
	ok, err := c.AcquireContinuityLease(ctx, 0, key, "outage", time.Minute)
	require.Error(t, err)
	require.False(t, ok)
}

func TestPersistentContinuityLegacyHistoryIsolation(t *testing.T) {
	c, _, _ := persistentContinuityFixture(t)
	ctx := context.Background()
	client := testEntClient(t)
	repo := newUsageLogRepositoryWithSQL(client, integrationDB)
	first := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com"})
	other := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com"})
	group := mustCreateGroup(t, client, &service.Group{Name: uuid.NewString()})
	accountA := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString()})
	accountB := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString()})
	session := uuid.NewString()
	now := time.Now().UTC()
	for _, record := range []struct {
		user, account int64
		group         *int64
	}{
		{first.ID, accountA.ID, nil},
		{first.ID, accountB.ID, nil}, // Latest ID wins if timestamps tie.
		{other.ID, accountA.ID, nil},
		{first.ID, accountA.ID, &group.ID},
	} {
		key := mustCreateApiKey(t, client, &service.APIKey{UserID: record.user, Key: uuid.NewString(), Name: "history"})
		_, err := repo.Create(ctx, &service.UsageLog{UserID: record.user, APIKeyID: key.ID,
			AccountID: record.account, GroupID: record.group, RequestID: uuid.NewString(), Model: "gpt-5",
			SessionID: &session, InputTokens: 1, OutputTokens: 1, CreatedAt: now})
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		user, group, want int64
	}{
		{first.ID, 0, accountB.ID},
		{other.ID, 0, accountA.ID},
		{first.ID, group.ID, accountA.ID},
	} {
		id, err := c.RecoverOpenAIContinuityAccount(ctx, tc.user, tc.group, session)
		require.NoError(t, err)
		require.Equal(t, tc.want, id)
	}
	_, err := c.RecoverOpenAIContinuityAccount(ctx, other.ID, group.ID, session)
	require.ErrorIs(t, err, service.ErrStickySessionNotFound)
	_, err = c.RecoverOpenAIContinuityAccount(ctx, first.ID, 0, session+"' OR 1=1 --")
	require.ErrorIs(t, err, service.ErrStickySessionNotFound)
	var count int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM openai_session_bindings WHERE user_id=$1`, first.ID).Scan(&count))
	require.Zero(t, count, "history lookup alone must not create a durable binding")
}
