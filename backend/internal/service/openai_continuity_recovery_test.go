package service

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type continuityHistoryTestCache struct {
	*schedulerTestGatewayCache
	account int64
	err     error
	calls   int
	user    int64
	group   int64
	session string
}

func (c *continuityHistoryTestCache) RecoverOpenAIContinuityAccount(_ context.Context, user, group int64, session string) (int64, error) {
	c.calls++
	c.user, c.group, c.session = user, group, session
	return c.account, c.err
}

func TestOpenAIContinuityRecoversExpiredSessionThroughNormalGuards(t *testing.T) {
	for _, mode := range []string{"recovered", "revoked", "disabled", "wrong_group", "missing", "db_error"} {
		t.Run(mode, func(t *testing.T) {
			group := int64(9)
			cache := &continuityHistoryTestCache{
				schedulerTestGatewayCache: &schedulerTestGatewayCache{sessionBindings: map[string]int64{}},
				account: 1,
			}
			accounts := []Account{
				{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
				{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
			}
			policy := &policyTestRepo{policies: map[int64]map[int64]bool{group: {1: true, 2: true}}}
			wantErr := error(nil)
			switch mode {
			case "revoked":
				delete(policy.policies[group], 1)
				wantErr = ErrAccountAccessDenied
			case "disabled":
				accounts[0].Schedulable = false
				wantErr = ErrOpenAIContextIncomplete
			case "wrong_group":
				accounts[0].GroupIDs = []int64{10}
				wantErr = ErrOpenAIContextIncomplete
			case "missing":
				cache.account, cache.err = 0, ErrStickySessionNotFound
				wantErr = ErrOpenAIContextIncomplete
			case "db_error":
				cache.account, cache.err = 0, errors.New("database unavailable")
				wantErr = ErrOpenAIContinuityUnavailable
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			c.Request.Header.Set("session-id", "expired-session")
			ctx := context.WithValue(c.Request.Context(), ctxkey.UserID, int64(7))
			ctx = WithUserAccountPolicy(ctx, NewUserAccountPolicyService(policy, nil), 7, &group)
			c.Request = c.Request.WithContext(ctx)
			svc := &OpenAIGatewayService{cfg: newSchedulerTestOpenAIWSV2Config(), cache: cache,
				accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts},
				rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
			hash := svc.GenerateSessionHash(c, []byte(`{"model":"gpt-5.1","input":[{"type":"reasoning","encrypted_content":"opaque"}]}`))
			ctx = c.Request.Context()
			selection, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", hash, "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.Equal(t, 1, cache.calls)
			require.EqualValues(t, 7, cache.user)
			require.Equal(t, group, cache.group)
			require.Equal(t, "expired-session", cache.session)
			require.Empty(t, cache.sessionBindings, "selecting or failing recovery must not bind")
			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
				require.Nil(t, selection, "opaque state must not escape to another available account")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.EqualValues(t, 1, selection.Account.ID)
			selection.ReleaseFunc()
			CompleteOpenAIContinuity(ctx, selection.Account)
			require.EqualValues(t, 1, cache.sessionBindings["openai:u7:"+hash])
		})
	}
}

func TestOpenAIContinuityHistoryNeverUsesDifferentIdentityOrSharedLegacyOwner(t *testing.T) {
	cache := &continuityHistoryTestCache{
		schedulerTestGatewayCache: &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:unrelated": 99}},
		account: 1,
	}
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(7))
	ctx = context.WithValue(ctx, openAIContinuityKey{}, &openAIContinuityState{enabled: true, clientSessionID: "original"})
	svc := &OpenAIGatewayService{cache: cache}
	_, err := svc.getStickySessionAccountID(ctx, nil, "unrelated")
	require.ErrorIs(t, err, ErrStickySessionNotFound)
	require.Zero(t, cache.calls)
	// A durable owner wins without consulting usage (which may be delayed).
	cache.sessionBindings["openai:u7:unrelated"] = 2
	id, err := svc.getStickySessionAccountID(ctx, nil, "unrelated")
	require.NoError(t, err)
	require.EqualValues(t, 2, id)
	require.Zero(t, cache.calls)
}
