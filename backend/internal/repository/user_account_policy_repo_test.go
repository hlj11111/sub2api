package repository

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUserAccountPolicyRepositoryEmptyMarkerAndMissingSubject(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	repo := &userAccountPolicyRepository{db: db}
	mock.ExpectQuery("SELECT").WithArgs(int64(1), int64(2)).WillReturnRows(sqlmock.NewRows([]string{"exists", "restricted", "ids"}).AddRow(true, true, "{}"))
	p, err := repo.Get(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, "allowlist", p.Mode)
	require.Empty(t, p.AccountIDs)
	mock.ExpectQuery("SELECT").WithArgs(int64(1), int64(2)).WillReturnRows(sqlmock.NewRows([]string{"exists", "restricted", "ids"}).AddRow(false, false, "{}"))
	_, err = repo.Get(context.Background(), 1, 2)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestUserAccountPolicyRepositoryRejectsOtherGroupAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	repo := &userAccountPolicyRepository{db: db}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM users").WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery("SELECT id FROM groups").WithArgs(int64(2)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM account_groups")).WithArgs(int64(2), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectRollback()
	require.Error(t, repo.Set(context.Background(), 1, 2, service.UserAccountPolicy{Mode: "allowlist", AccountIDs: []int64{99}}))
	require.NoError(t, mock.ExpectationsWereMet())
}
