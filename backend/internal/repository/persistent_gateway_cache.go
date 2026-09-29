package repository

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

// PostgreSQL owns authenticated OpenAI session bindings and their fencing tokens.
// Redis remains a compatibility mirror; its TTL cannot erase successful ownership.
// Embedding preserves the unrelated GatewayCache capabilities (Live, windows, etc.).
type persistentGatewayCache struct {
	*gatewayCache
	db *sql.DB
}

func NewPersistentGatewayCache(rdb *redis.Client, db *sql.DB) service.GatewayCache {
	return &persistentGatewayCache{gatewayCache: &gatewayCache{rdb: rdb}, db: db}
}

var _ service.OpenAIContinuityCache = (*persistentGatewayCache)(nil)
var _ service.OpenAIContinuityHistory = (*persistentGatewayCache)(nil)

func persistentOpenAISession(key string) (int64, string, bool) {
	if !strings.HasPrefix(key, "openai:u") {
		return 0, "", false
	}
	user, hash, ok := strings.Cut(strings.TrimPrefix(key, "openai:u"), ":")
	if !ok || len(hash) != 16 {
		return 0, "", false
	}
	id, err := strconv.ParseInt(user, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != user {
		return 0, "", false
	}
	if _, err := hex.DecodeString(hash); err != nil || strings.ToLower(hash) != hash {
		return 0, "", false
	}
	return id, hash, true
}

func (c *persistentGatewayCache) GetSessionAccountID(ctx context.Context, group int64, key string) (int64, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.GetSessionAccountID(ctx, group, key)
	}
	var account sql.NullInt64
	err := c.db.QueryRowContext(ctx, `SELECT account_id FROM openai_session_bindings
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3`, user, group, hash).Scan(&account)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err // A database outage must never trigger speculative migration.
	}
	if account.Valid {
		return account.Int64, nil
	}
	// Rollout compatibility: a pre-upgrade user-scoped Redis binding is a hint.
	// It becomes durable only after a successful, fenced upstream turn.
	return c.gatewayCache.GetSessionAccountID(ctx, group, key)
}

func (c *persistentGatewayCache) AcquireContinuityLease(ctx context.Context, group int64, key, owner string, ttl time.Duration) (bool, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.AcquireContinuityLease(ctx, group, key, owner, ttl)
	}
	result, err := c.db.ExecContext(ctx, `INSERT INTO openai_session_bindings
		(user_id, group_id, session_hash, lease_owner, lease_expires_at)
		VALUES ($1,$2,$3,$4,clock_timestamp()+$5*interval '1 millisecond')
		ON CONFLICT (user_id,group_id,session_hash) DO UPDATE SET
		lease_owner=EXCLUDED.lease_owner, lease_expires_at=clock_timestamp()+$5*interval '1 millisecond', committed_owner=NULL
		WHERE openai_session_bindings.lease_owner IS NULL
		OR openai_session_bindings.lease_expires_at <= clock_timestamp()`, user, group, hash, owner, ttl.Milliseconds())
	return continuityRowChanged(result, err)
}

func (c *persistentGatewayCache) RenewContinuityLease(ctx context.Context, group int64, key, owner string, ttl time.Duration) (bool, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.RenewContinuityLease(ctx, group, key, owner, ttl)
	}
	result, err := c.db.ExecContext(ctx, `UPDATE openai_session_bindings
		SET lease_expires_at=clock_timestamp()+$5*interval '1 millisecond'
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND lease_owner=$4
		AND lease_expires_at > clock_timestamp()`, user, group, hash, owner, ttl.Milliseconds())
	return continuityRowChanged(result, err)
}

func (c *persistentGatewayCache) CommitContinuityBinding(ctx context.Context, group int64, key, owner string, accountID int64, ttl time.Duration) (bool, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.CommitContinuityBinding(ctx, group, key, owner, accountID, ttl)
	}
	result, err := c.db.ExecContext(ctx, `UPDATE openai_session_bindings SET account_id=$5,
		last_success_at=clock_timestamp(), committed_owner=$4, lease_owner=NULL, lease_expires_at=NULL
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND
		((lease_owner=$4 AND lease_expires_at > clock_timestamp())
		OR (lease_owner IS NULL AND committed_owner=$4 AND account_id=$5))`, user, group, hash, owner, accountID)
	changed, err := continuityRowChanged(result, err)
	if err != nil || !changed {
		return changed, err
	}
	// DB reads are authoritative even if this mirror write races with a later
	// commit. A Redis failure cannot undo an already committed successful turn.
	_ = c.SetSessionAccountID(ctx, group, key, accountID, ttl)
	return true, nil
}

func (c *persistentGatewayCache) ReleaseContinuityLease(ctx context.Context, group int64, key, owner string) error {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.ReleaseContinuityLease(ctx, group, key, owner)
	}
	// A failed first request should not leave a permanent empty binding. Both
	// statements are owner-conditional; neither can affect a newer lease.
	_, err := c.db.ExecContext(ctx, `DELETE FROM openai_session_bindings
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND lease_owner=$4 AND account_id IS NULL`, user, group, hash, owner)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `UPDATE openai_session_bindings SET lease_owner=NULL, lease_expires_at=NULL
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND lease_owner=$4`, user, group, hash, owner)
	return err
}

func (c *persistentGatewayCache) CheckContinuityBinding(ctx context.Context, group int64, key, owner string, accountID int64) (bool, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.gatewayCache.CheckContinuityBinding(ctx, group, key, owner, accountID)
	}
	var valid bool
	err := c.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM openai_session_bindings
		WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND
		((lease_owner=$4 AND lease_expires_at > clock_timestamp())
		OR (lease_owner IS NULL AND committed_owner=$4 AND account_id=$5)))`, user, group, hash, owner, accountID).Scan(&valid)
	return valid, err
}

// Usage records are recovery candidates, not proof of a completed upstream turn.
// Revalidate the latest candidate through the normal account policy/scheduler;
// never search backwards for a different, more convenient account.
func (c *persistentGatewayCache) RecoverOpenAIContinuityAccount(ctx context.Context, user, group int64, sessionID string) (int64, error) {
	if user <= 0 || strings.TrimSpace(sessionID) == "" {
		return 0, service.ErrStickySessionNotFound
	}
	var account int64
	err := c.db.QueryRowContext(ctx, `SELECT account_id FROM usage_logs
		WHERE user_id=$1 AND COALESCE(group_id,0)=$2 AND session_id=$3
		ORDER BY created_at DESC, id DESC LIMIT 1`, user, group, sessionID).Scan(&account)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, service.ErrStickySessionNotFound
	}
	return account, err
}

func continuityRowChanged(result sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}
