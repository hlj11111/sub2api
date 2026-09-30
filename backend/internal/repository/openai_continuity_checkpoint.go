package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.OpenAIContinuityCheckpointStore = (*persistentGatewayCache)(nil)

func (c *persistentGatewayCache) GetContinuityCheckpoint(ctx context.Context, group int64, key string) ([]byte, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return nil, nil
	}
	var sealed []byte
	err := c.db.QueryRowContext(ctx, `SELECT ciphertext FROM openai_session_checkpoints
 WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND expires_at>clock_timestamp()`, user, group, hash).Scan(&sealed)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return sealed, err
}

func (c *persistentGatewayCache) CommitContinuityCheckpoint(ctx context.Context, group int64, key, owner string, accountID int64, bindingTTL time.Duration, sealed []byte, retention time.Duration) (bool, error) {
	user, hash, ok := persistentOpenAISession(key)
	if !ok {
		return c.CommitContinuityBinding(ctx, group, key, owner, accountID, bindingTTL)
	}
	if len(sealed) == 0 || len(sealed) > 4195328 || retention <= 0 || retention > 24*time.Hour {
		return false, fmt.Errorf("invalid checkpoint bounds")
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE openai_session_bindings SET account_id=$5,
 last_success_at=clock_timestamp(), committed_owner=$4, lease_owner=NULL, lease_expires_at=NULL
 WHERE user_id=$1 AND group_id=$2 AND session_hash=$3 AND
 ((lease_owner=$4 AND lease_expires_at>clock_timestamp()) OR
 (lease_owner IS NULL AND committed_owner=$4 AND account_id=$5))`, user, group, hash, owner, accountID)
	changed, err := continuityRowChanged(result, err)
	if err != nil || !changed {
		return false, err
	}
	// The UPDATE row lock prevents a newer lease/binding from interleaving.
	_, err = tx.ExecContext(ctx, `INSERT INTO openai_session_checkpoints(user_id,group_id,session_hash,ciphertext,expires_at)
 VALUES($1,$2,$3,$4,clock_timestamp()+$5*interval '1 millisecond')
 ON CONFLICT(user_id,group_id,session_hash) DO UPDATE SET ciphertext=EXCLUDED.ciphertext,expires_at=EXCLUDED.expires_at`, user, group, hash, sealed, retention.Milliseconds())
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	_ = c.SetSessionAccountID(ctx, group, key, accountID, bindingTTL)
	// Bounded, opportunistic deletion; expired payloads are never returned even
	// when the deployment has no traffic to trigger physical cleanup.
	_, _ = c.db.ExecContext(ctx, `DELETE FROM openai_session_checkpoints WHERE (user_id,group_id,session_hash) IN
 (SELECT user_id,group_id,session_hash FROM openai_session_checkpoints WHERE expires_at<=clock_timestamp()
 ORDER BY expires_at LIMIT 100) AND expires_at<=clock_timestamp()`)
	return true, nil
}
