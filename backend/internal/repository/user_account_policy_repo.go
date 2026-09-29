package repository

import (
	"context"
	"database/sql"
	"errors"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type userAccountPolicyRepository struct{ db *sql.DB }

func NewUserAccountPolicyRepository(db *sql.DB) service.UserAccountPolicyRepository {
	return &userAccountPolicyRepository{db: db}
}

func (r *userAccountPolicyRepository) Get(ctx context.Context, userID, groupID int64) (*service.UserAccountPolicy, error) {
	var exists, restricted bool
	var ids pq.Int64Array
	err := r.db.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM users u, groups g WHERE u.id=$1 AND g.id=$2 AND u.deleted_at IS NULL AND g.deleted_at IS NULL),
 EXISTS(SELECT 1 FROM user_group_account_policies WHERE user_id=$1 AND group_id=$2),
 ARRAY(SELECT account_id FROM user_group_account_policy_accounts WHERE user_id=$1 AND group_id=$2 ORDER BY account_id)`, userID, groupID).Scan(&exists, &restricted, &ids)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, infraerrors.NotFound("ACCOUNT_POLICY_SUBJECT_NOT_FOUND", "User or group not found")
	}
	p := &service.UserAccountPolicy{Mode: "all", AccountIDs: []int64{}}
	if restricted {
		p.Mode = "allowlist"
		p.AccountIDs = append(p.AccountIDs, ids...)
	}
	return p, nil
}

func (r *userAccountPolicyRepository) Set(ctx context.Context, userID, groupID int64, p service.UserAccountPolicy) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Lock the parent so concurrent replaces (including clearing a policy) serialize.
	var id int64
	if err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, userID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return infraerrors.NotFound("USER_NOT_FOUND", "User not found")
		}
		return err
	}
	if err = tx.QueryRowContext(ctx, `SELECT id FROM groups WHERE id=$1 AND deleted_at IS NULL FOR SHARE`, groupID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return infraerrors.NotFound("GROUP_NOT_FOUND", "Group not found")
		}
		return err
	}
	if p.Mode == "all" {
		_, err = tx.ExecContext(ctx, `DELETE FROM user_group_account_policies WHERE user_id=$1 AND group_id=$2`, userID, groupID)
	} else {
		var count int
		err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_groups ag JOIN accounts a ON a.id=ag.account_id AND a.deleted_at IS NULL WHERE ag.group_id=$1 AND ag.account_id=ANY($2)`, groupID, pq.Array(p.AccountIDs)).Scan(&count)
		if err != nil {
			return err
		}
		if count != len(p.AccountIDs) {
			return infraerrors.BadRequest("INVALID_ACCOUNT_POLICY", "Every selected account must belong to this group")
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO user_group_account_policies(user_id,group_id) VALUES($1,$2) ON CONFLICT(user_id,group_id) DO UPDATE SET updated_at=NOW()`, userID, groupID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM user_group_account_policy_accounts WHERE user_id=$1 AND group_id=$2`, userID, groupID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO user_group_account_policy_accounts(user_id,group_id,account_id) SELECT $1,$2,unnest($3::bigint[])`, userID, groupID, pq.Array(p.AccountIDs))
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r *userAccountPolicyRepository) Allowed(ctx context.Context, userID int64, groups []int64, accountID int64) (bool, error) {
	ids, err := r.Filter(ctx, userID, groups, []int64{accountID})
	return len(ids) == 1, err
}

func (r *userAccountPolicyRepository) Filter(ctx context.Context, userID int64, groups, accountIDs []int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT a.id FROM accounts a WHERE a.id=ANY($3) AND a.deleted_at IS NULL AND a.status='active'
 AND NOT EXISTS (
  SELECT 1 FROM user_group_account_policies p WHERE p.user_id=$1 AND p.group_id=ANY($2)
  AND NOT EXISTS (
   SELECT 1 FROM user_group_account_policy_accounts pa
   JOIN account_groups ag ON ag.account_id=pa.account_id AND ag.group_id=pa.group_id
   WHERE pa.user_id=p.user_id AND pa.group_id=p.group_id AND pa.account_id=a.id
  )
 )`, userID, pq.Array(groups), pq.Array(accountIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]int64, 0, len(accountIDs))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
