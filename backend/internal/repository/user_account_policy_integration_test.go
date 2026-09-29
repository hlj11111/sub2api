package repository

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Uses an explicitly supplied disposable database; never falls back to app credentials.
func TestUserAccountPolicyPostgres(t *testing.T) {
	dsn := os.Getenv("ACCOUNT_POLICY_TEST_DSN")
	if dsn == "" {
		t.Skip("ACCOUNT_POLICY_TEST_DSN is not set")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE users(id BIGINT PRIMARY KEY, deleted_at TIMESTAMPTZ);
 CREATE TEMP TABLE groups(id BIGINT PRIMARY KEY, deleted_at TIMESTAMPTZ);
 CREATE TEMP TABLE accounts(id BIGINT PRIMARY KEY, deleted_at TIMESTAMPTZ, status TEXT DEFAULT 'active');
 CREATE TEMP TABLE account_groups(account_id BIGINT, group_id BIGINT, PRIMARY KEY(account_id,group_id));
 INSERT INTO users VALUES(1,NULL),(2,NULL);
 INSERT INTO groups VALUES(10,NULL),(20,NULL);
 INSERT INTO accounts(id,deleted_at) VALUES(100,NULL),(200,NULL),(300,NULL);
 INSERT INTO account_groups VALUES(100,10),(200,10),(200,20),(300,20);`)
	require.NoError(t, err)
	migration, err := os.ReadFile("../../migrations/241_user_group_account_policies.sql")
	require.NoError(t, err)
	// Temporary tables keep the whole fixture isolated, including its policy marker.
	sqlText := strings.ReplaceAll(string(migration), "CREATE TABLE ", "CREATE TEMP TABLE ")
	_, err = db.Exec(sqlText)
	require.NoError(t, err)
	repo := &userAccountPolicyRepository{db: db}
	ctx := context.Background()
	svc := service.NewUserAccountPolicyService(repo, nil)
	require.NoError(t, svc.Set(ctx, 1, 10, service.UserAccountPolicy{Mode: "allowlist", AccountIDs: []int64{100, 200}}))
	require.NoError(t, svc.Set(ctx, 1, 20, service.UserAccountPolicy{Mode: "allowlist", AccountIDs: []int64{200, 300}}))
	ids, err := repo.Filter(ctx, 1, []int64{10, 20}, []int64{100, 200, 300})
	require.NoError(t, err)
	require.Equal(t, []int64{200}, ids)
	ids, err = repo.Filter(ctx, 2, []int64{10}, []int64{100, 200})
	require.NoError(t, err)
	require.Len(t, ids, 2)
	require.Error(t, svc.Set(ctx, 1, 10, service.UserAccountPolicy{Mode: "allowlist", AccountIDs: []int64{300}}))
	_, err = db.Exec(`DELETE FROM account_groups WHERE account_id=200 AND group_id=10`)
	require.NoError(t, err)
	ok, err := repo.Allowed(ctx, 1, []int64{10}, 200)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = db.Exec(`DELETE FROM accounts WHERE id=100`)
	require.NoError(t, err)
	p, err := repo.Get(ctx, 1, 10)
	require.NoError(t, err)
	require.Equal(t, "allowlist", p.Mode)
	ok, err = repo.Allowed(ctx, 1, []int64{10}, 300)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, svc.Set(ctx, 1, 10, service.UserAccountPolicy{Mode: "allowlist"}))
	p, err = repo.Get(ctx, 1, 10)
	require.NoError(t, err)
	require.Empty(t, p.AccountIDs)
	require.Equal(t, "allowlist", p.Mode)
	_, err = db.Exec("UPDATE accounts SET status='disabled' WHERE id=200")
	require.NoError(t, err)
	ok, err = repo.Allowed(ctx, 1, []int64{20}, 200)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = db.Exec("UPDATE accounts SET status='active' WHERE id=200")
	require.NoError(t, err)
	require.NoError(t, svc.Set(ctx, 1, 10, service.UserAccountPolicy{Mode: "all"}))
	ok, err = repo.Allowed(ctx, 1, []int64{10}, 200)
	require.NoError(t, err)
	require.True(t, ok)
}
