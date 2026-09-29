//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestUsageLog_SessionIDPersistence proves session_id round-trips from insert to
// read and is omitted (NULL) when absent.
func TestUsageLog_SessionIDPersistence(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := newUsageLogRepositoryWithSQL(client, integrationDB)

	user := mustCreateUser(t, client, &service.User{Email: "session-id-" + uuid.NewString() + "@example.com"})
	apiKey := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-session-" + uuid.NewString(), Name: "k"})
	account := mustCreateAccount(t, client, &service.Account{Name: "acc-session-" + uuid.NewString()})

	sessionID := "sess-" + uuid.NewString()

	withSession := &service.UsageLog{
		UserID:       user.ID,
		APIKeyID:     apiKey.ID,
		AccountID:    account.ID,
		RequestID:    uuid.NewString(),
		Model:        "claude-3",
		InputTokens:  10,
		OutputTokens: 5,
		TotalCost:    1.0,
		ActualCost:   1.0,
		SessionID:    &sessionID,
		CreatedAt:    time.Now().UTC(),
	}
	_, err := repo.Create(ctx, withSession)
	require.NoError(t, err)
	require.NotZero(t, withSession.ID)

	withoutSession := &service.UsageLog{
		UserID:       user.ID,
		APIKeyID:     apiKey.ID,
		AccountID:    account.ID,
		RequestID:    uuid.NewString(),
		Model:        "claude-3",
		InputTokens:  7,
		OutputTokens: 3,
		TotalCost:    0.5,
		ActualCost:   0.5,
		CreatedAt:    time.Now().UTC(),
	}
	_, err = repo.Create(ctx, withoutSession)
	require.NoError(t, err)

	// Round-trip: session id survives insert → read.
	got, err := repo.GetByID(ctx, withSession.ID)
	require.NoError(t, err)
	require.NotNil(t, got.SessionID)
	require.Equal(t, sessionID, *got.SessionID)

	// Omission: absent session id reads back as nil (NULL), not empty string.
	gotNone, err := repo.GetByID(ctx, withoutSession.ID)
	require.NoError(t, err)
	require.Nil(t, gotNone.SessionID)
}

func TestUsageLog_SessionFilterAcrossAccountsAndStatistics(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	client := tx.Client()
	repo := newUsageLogRepositoryWithSQL(client, tx)
	user := mustCreateUser(t, client, &service.User{Email: "session-filter-" + uuid.NewString() + "@example.com"})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "session-filter"})
	first := mustCreateAccount(t, client, &service.Account{Name: "first"})
	second := mustCreateAccount(t, client, &service.Account{Name: "second"})
	session := "session-" + uuid.NewString()
	other := session + "-other"
	now := time.Now().UTC()
	for i, id := range []*string{&session, &session, &other, nil} {
		accountID := first.ID
		if i == 1 {
			accountID = second.ID
		}
		_, err := repo.Create(ctx, &service.UsageLog{UserID: user.ID, APIKeyID: key.ID, AccountID: accountID,
			RequestID: uuid.NewString(), Model: "gpt-5", SessionID: id, InputTokens: 10, OutputTokens: 2,
			CacheReadTokens: 30, CacheCreationTokens: 5, TotalCost: 0.5, ActualCost: 0.25, CreatedAt: now})
		require.NoError(t, err)
	}
	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	filters := usagestats.UsageLogFilters{SessionID: " " + session + " ", StartTime: &start, EndTime: &end}
	logs, page, err := repo.ListWithFilters(ctx, pagination.PaginationParams{Page: 1, PageSize: 1}, filters)
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Len(t, logs, 1)
	logs, _, err = repo.ListWithFilters(ctx, pagination.PaginationParams{Page: 1, PageSize: 10}, filters)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.ElementsMatch(t, []int64{first.ID, second.ID}, []int64{logs[0].AccountID, logs[1].AccountID})
	stats, err := repo.GetStatsWithFilters(ctx, filters)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.TotalRequests)
	require.EqualValues(t, 60, stats.TotalCacheReadTokens)
	require.EqualValues(t, 10, stats.TotalCacheCreationTokens)
	trend, err := repo.GetUsageTrendWithUsageFilters(ctx, start, end, "day", filters)
	require.NoError(t, err)
	require.Len(t, trend, 1)
	require.EqualValues(t, 2, trend[0].Requests)
	require.EqualValues(t, 60, trend[0].CacheReadTokens)
	models, err := repo.GetModelStatsWithUsageFiltersBySource(ctx, start, end, filters, usagestats.ModelSourceRequested)
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.EqualValues(t, 2, models[0].Requests)
	groups, err := repo.GetGroupStatsWithUsageFilters(ctx, start, end, filters)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.EqualValues(t, 2, groups[0].Requests)
	filters.SessionID = session + "' OR 1=1 --"
	logs, page, err = repo.ListWithFilters(ctx, pagination.PaginationParams{Page: 1, PageSize: 10}, filters)
	require.NoError(t, err)
	require.Empty(t, logs)
	require.Zero(t, page.Total)
}
