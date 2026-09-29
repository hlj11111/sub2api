package service

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOpenAIContinuityScopesRoutingWithoutChangingCacheIdentity(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Request.Header.Set("session_id", "same")
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.UserID, int64(7)))
	svc := &OpenAIGatewayService{}
	hash := svc.GenerateSessionHash(c, []byte(`{"model":"gpt-5","prompt_cache_key":"cache"}`))
	require.NotNil(t, continuityState(c.Request.Context()))
	require.Equal(t, "u7:"+hash, scopedOpenAISessionHash(c.Request.Context(), hash))
	require.Equal(t, hash, svc.GenerateSessionHash(c, []byte(`{"model":"gpt-6","prompt_cache_key":"cache"}`)))
	other := context.WithValue(c.Request.Context(), ctxkey.UserID, int64(8))
	require.NotEqual(t, scopedOpenAISessionHash(other, hash), scopedOpenAISessionHash(c.Request.Context(), hash))
}
func TestOpenAIContinuityMissingHistoryCannotMoveEvenWithToolCoverage(t *testing.T) {
	svc := &OpenAIGatewayService{}
	core, logs := observer.New(zap.WarnLevel)
	ctx := logger.IntoContext(context.Background(), zap.New(core).With(zap.String("request_id", "test-request")))
	ctx = context.WithValue(ctx, openAIContinuityKey{}, &openAIContinuityState{})
	_, _, handled, err := svc.selectContinuityAccount(ctx, OpenAIAccountScheduleRequest{
		Platform: PlatformOpenAI, PreviousResponseID: "resp_missing", PreviousResponseCanMove: true,
	})
	require.True(t, handled)
	require.ErrorIs(t, err, ErrOpenAIContextIncomplete)
	require.Len(t, logs.All(), 1)
	fields := logs.All()[0].ContextMap()
	require.Equal(t, "test-request", fields["request_id"])
	require.Equal(t, "previous_response_owner_missing", fields["reason"])
	require.Equal(t, true, fields["previous_response_present"])
	require.NotContains(t, fields, "previous_response_id")
}

func TestOpenAIContinuityDiagnosticsDistinguishMissingSessionAndBinding(t *testing.T) {
	for _, tc := range []struct {
		hash   string
		reason string
	}{
		{"", "session_identity_missing"},
		{"s", "session_binding_missing"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			ctx := logger.IntoContext(context.Background(), zap.New(core))
			ctx = context.WithValue(ctx, openAIContinuityKey{}, &openAIContinuityState{
				migrationUnsafe: true, nonPortableReason: "encrypted_content",
			})
			cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{}}
			svc := &OpenAIGatewayService{cache: cache}
			selection, _, handled, err := svc.selectContinuityAccount(ctx, OpenAIAccountScheduleRequest{
				Platform: PlatformOpenAI, SessionHash: tc.hash,
			})
			require.Nil(t, selection)
			require.True(t, handled)
			require.ErrorIs(t, err, ErrOpenAIContextIncomplete)
			require.Len(t, logs.All(), 1)
			require.Equal(t, tc.reason, logs.All()[0].ContextMap()["reason"])
			require.Empty(t, cache.sessionBindings, "diagnostics must not create or repair a binding")
		})
	}
}

func TestOpenAIContinuityDiagnosticsDoNotRetainPrivatePayload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		reason string
	}{
		{"encrypted", `{"input":[{"type":"reasoning","encrypted_content":"private-ciphertext"}]}`, "encrypted_content"},
		{"tool_output", `{"input":[{"type":"function_call_output","call_id":"private-call","output":"private-output"}]}`, "unmatched_tool_output"},
		{"conversation", `{"conversation":"private-conversation"}`, "conversation_reference"},
		{"item", `{"input":[{"type":"item_reference","id":"private-item"}]}`, "item_reference"},
		{"file", `{"input":[{"content":[{"type":"input_file","file_id":"private-file"}]}]}`, "file_id"},
		{"container", `{"tools":[{"type":"code_interpreter","container":"private-container"}]}`, "container_reference"},
		{"portable", `{"input":"private-message"}`, ""},
		{"paired_tools", `{"input":[{"type":"function_call","call_id":"private-call","name":"test","arguments":"{}"},{"type":"function_call_output","call_id":"private-call","output":"private-output"}]}`, ""},
		{"ws_envelope", `{"type":"response.create","response":{"input":[{"type":"reasoning","encrypted_content":"private-ciphertext"}]}}`, "encrypted_content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			c.Request.Header.Set("session_id", "private-session")
			c.Request = c.Request.WithContext(logger.IntoContext(c.Request.Context(), zap.New(core)))
			svc := &OpenAIGatewayService{}
			hash := svc.GenerateSessionHash(c, []byte(tc.body))
			ctx := c.Request.Context()
			require.NoError(t, svc.beginContinuity(ctx, nil, hash))
			require.Equal(t, tc.reason, continuityState(ctx).nonPortableReason)
			require.Equal(t, tc.reason != "", continuityState(ctx).migrationUnsafe)
			require.ErrorIs(t, openAIContextIncomplete(ctx, "bound_account_not_selectable", 42), ErrOpenAIContextIncomplete)
			require.Len(t, logs.All(), 1)
			fields := logs.All()[0].ContextMap()
			require.Equal(t, int64(42), fields["account_id"])
			require.Equal(t, hash, fields["session_hash"])
			require.Equal(t, tc.reason, fields["non_portable_reason"])
			require.NotContains(t, logs.All()[0].ContextMap(), "request_body")
			for _, value := range fields {
				if text, ok := value.(string); ok {
					require.NotContains(t, text, "private-")
				}
			}
		})
	}
}

func TestOpenAIContinuityBusyAccountWaitsDespiteEscapeAndWeightedSettings(t *testing.T) {
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{})
	group := int64(9)
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:s": 1}}
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.OpenAIScheduler.StickyEscapeEnabled = true
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = time.Second
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 2
	svc := &OpenAIGatewayService{cfg: cfg, cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts},
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquireResults: map[int64]bool{1: false, 2: true}}),
	}
	selection, decision, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.EqualValues(t, 1, selection.Account.ID)
	require.True(t, decision.StickySessionHit)
	require.NotNil(t, selection.WaitPlan)
	require.Equal(t, time.Second, selection.WaitPlan.Timeout)
	require.EqualValues(t, 1, cache.sessionBindings["openai:s"])
	require.True(t, ClaimOpenAIContinuityWaitMigration(ctx))
	migrated, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", map[int64]struct{}{1: {}}, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.EqualValues(t, 2, migrated.Account.ID)
	require.EqualValues(t, 1, cache.sessionBindings["openai:s"], "waiting or selecting must not replace the binding")
	CompleteOpenAIContinuity(ctx, migrated.Account)
	require.EqualValues(t, 2, cache.sessionBindings["openai:s"])
	migrated.ReleaseFunc()
	require.False(t, ClaimOpenAIContinuityWaitMigration(ctx), "only one capacity migration is permitted")
}

func TestOpenAIContinuityMigrationCommitsOnlySuccessfulAccount(t *testing.T) {
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{})
	group := int64(9)
	reset := time.Now().Add(time.Minute)
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, RateLimitResetAt: &reset},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:s": 1}}
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 5 * time.Millisecond
	svc := &OpenAIGatewayService{cfg: cfg, cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts}, rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	selection, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	defer selection.ReleaseFunc()
	require.EqualValues(t, 2, selection.Account.ID)
	require.EqualValues(t, 1, cache.sessionBindings["openai:s"], "a failed attempt must not change the previous binding")
	CompleteOpenAIContinuity(ctx, selection.Account)
	require.EqualValues(t, 2, cache.sessionBindings["openai:s"])
	// Recovery of the old account must not pull this conversation back.
	accounts[0].RateLimitResetAt = nil
	next := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{})
	result, _, err := svc.SelectAccountWithScheduler(next, &group, "", "s", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Account.ID)
	result.ReleaseFunc()
}

func TestOpenAIContinuityCooldownRecoveryKeepsUserScopedBinding(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(7))
	ctx = context.WithValue(ctx, openAIContinuityKey{}, &openAIContinuityState{})
	group := int64(9)
	reset := time.Now().Add(10 * time.Millisecond)
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, RateLimitResetAt: &reset},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:u7:s": 1, "openai:s": 2}}
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 30 * time.Millisecond
	svc := &OpenAIGatewayService{cfg: cfg, cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts}, rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	for i := 0; i < 2; i++ {
		selection, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
		require.NoError(t, err)
		require.EqualValues(t, 1, selection.Account.ID)
		CompleteOpenAIContinuity(ctx, selection.Account)
		selection.ReleaseFunc()
	}
	require.EqualValues(t, 2, cache.sessionBindings["openai:s"])
	require.NotContains(t, cache.sessionBindings, "openai:u7:u7:s")
}

func TestOpenAIContinuityHTTPBridgeRejectsUnprovenHistoryBeforeSending(t *testing.T) {
	for _, kind := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{enabled: true})
			upstream := &httpUpstreamRecorder{}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			result, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, nil, &Account{ID: 1, Platform: PlatformOpenAI, Type: kind}, "token", []byte("{\"previous_response_id\":\"resp_unknown\",\"input\":\"next\"}"), 0, "gpt-5", "", "", "", "", 1, func([]byte) error { return nil })
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrOpenAIContextIncomplete)
			require.Nil(t, upstream.lastBody)
		})
	}
}

func TestOpenAIContinuityRejectsOpaqueStateWithoutBinding(t *testing.T) {
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{migrationUnsafe: true})
	svc := &OpenAIGatewayService{}
	_, _, handled, err := svc.selectContinuityAccount(ctx, OpenAIAccountScheduleRequest{Platform: PlatformOpenAI})
	require.True(t, handled)
	require.ErrorIs(t, err, ErrOpenAIContextIncomplete)
}

type continuityWaitingCache struct {
	schedulerTestConcurrencyCache
	calls, queued, dequeued int
	allowWait               bool
	onWait                  func()
}

func (c *continuityWaitingCache) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	c.calls++
	return c.calls > 1, nil
}
func (c *continuityWaitingCache) IncrementAccountWaitCount(context.Context, int64, int) (bool, error) {
	c.queued++
	if c.onWait != nil {
		c.onWait()
	}
	return c.allowWait, nil
}
func (c *continuityWaitingCache) DecrementAccountWaitCount(context.Context, int64) error {
	c.dequeued++
	return nil
}
func TestOpenAIContinuityWebSocketWaitRechecksRevocation(t *testing.T) {
	for _, mode := range []string{"recovered", "revoked", "queue_full"} {
		t.Run(mode, func(t *testing.T) {
			group := int64(1)
			repo := &policyTestRepo{policies: map[int64]map[int64]bool{group: {1: true}}}
			ctx := WithUserAccountPolicy(context.Background(), NewUserAccountPolicyService(repo, nil), 7, &group)
			ctx = context.WithValue(ctx, openAIContinuityKey{}, &openAIContinuityState{enabled: true})
			cache := &continuityWaitingCache{allowWait: mode != "queue_full"}
			if mode == "revoked" {
				cache.onWait = func() { delete(repo.policies[group], 1) }
			}
			svc := &OpenAIGatewayService{concurrencyService: NewConcurrencyService(cache)}
			release, acquired, err := svc.AcquireOpenAIWebSocketAccountSlot(ctx, 1, 1, &AccountWaitPlan{Timeout: time.Second, MaxWaiting: 2})
			if mode == "recovered" {
				require.NoError(t, err)
				require.True(t, acquired)
				release()
			} else {
				require.Error(t, err)
				require.False(t, acquired)
				require.Nil(t, release)
			}
			if mode == "revoked" {
				require.ErrorIs(t, err, ErrAccountAccessDenied)
				require.Equal(t, 1, cache.calls)
			}
			require.Equal(t, 1, cache.queued)
			if mode != "queue_full" {
				require.Equal(t, 1, cache.dequeued)
			}
		})
	}
}

func TestOpenAIContinuityAccountScopedReferencesAreNotPortable(t *testing.T) {
	for _, tc := range []struct {
		body   string
		unsafe bool
	}{
		{"{\"input\":\"hello\"}", false},
		{"{\"conversation\":\"conv_1\"}", true},
		{"{\"input\":[{\"type\":\"item_reference\",\"id\":\"item_1\"}]}", true},
		{"{\"input\":[{\"content\":[{\"type\":\"input_file\",\"file_id\":\"file_1\"}]}]}", true},
		{"{\"input\":[{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}]}", true},
		{"{\"tools\":[{\"type\":\"file_search\",\"vector_store_ids\":[\"vs_1\"]}]}", true},
		{"{\"tools\":[{\"type\":\"function\",\"parameters\":{\"properties\":{\"file_id\":{\"type\":\"string\"}}}}]}", false},
		{"{\"tools\":[{\"type\":\"code_interpreter\",\"container\":\"auto\"}]}", false},
	} {
		require.Equal(t, tc.unsafe, openAIRequestHasNonPortableState([]byte(tc.body)), tc.body)
	}
}

func TestOpenAIContinuityContentFallbackSurvivesModelChangeWithoutChangingCacheKey(t *testing.T) {
	svc := &OpenAIGatewayService{}
	var routes, cacheKeys []string
	for _, model := range []string{"gpt-5.1", "gpt-5.2"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.UserID, int64(7)))
		body := []byte("{\"model\":\"" + model + "\",\"input\":\"same conversation\"}")
		hash := svc.GenerateSessionHash(c, body)
		legacyHash, _ := deriveOpenAISessionHashes(deriveOpenAIContentSessionSeed(body))
		require.Equal(t, legacyHash, hash)
		cacheKeys = append(cacheKeys, hash)
		routes = append(routes, scopedOpenAISessionHash(c.Request.Context(), hash))
	}
	require.NotEqual(t, cacheKeys[0], cacheKeys[1])
	require.Equal(t, routes[0], routes[1])
}

func TestOpenAIContinuityWaitMigrationRequiresPortableHistory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   *openAIContinuityState
		allowed bool
	}{
		{name: "uninitialized", state: &openAIContinuityState{}, allowed: false},
		{name: "complete request", state: &openAIContinuityState{enabled: true}, allowed: true},
		{name: "previous response", state: &openAIContinuityState{enabled: true, previousResponseID: "resp_1"}, allowed: false},
		{name: "opaque reference", state: &openAIContinuityState{enabled: true, migrationUnsafe: true}, allowed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), openAIContinuityKey{}, tc.state)
			require.Equal(t, tc.allowed, ClaimOpenAIContinuityWaitMigration(ctx))
			require.False(t, ClaimOpenAIContinuityWaitMigration(ctx))
		})
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{enabled: true}))
	cancel()
	require.False(t, ClaimOpenAIContinuityWaitMigration(ctx))
}

func TestOpenAIContinuityWebSocketWaitTimeoutIsRetryable(t *testing.T) {
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{enabled: true})
	cache := &continuityWaitingCache{allowWait: true}
	svc := &OpenAIGatewayService{concurrencyService: NewConcurrencyService(cache)}
	release, acquired, err := svc.AcquireOpenAIWebSocketAccountSlot(ctx, 1, 1, &AccountWaitPlan{Timeout: time.Millisecond, MaxWaiting: 2})
	require.Nil(t, release)
	require.False(t, acquired)
	require.ErrorIs(t, err, ErrOpenAIContinuityUnavailable)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, cache.dequeued)
}

func TestOpenAIContinuityRecoveryRetriesBeforeSafeMigration(t *testing.T) {
	group := int64(9)
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{explicitSession: true})
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{}}
	svc := &OpenAIGatewayService{cfg: newSchedulerTestOpenAIWSV2Config(), cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts},
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	require.NoError(t, svc.beginContinuity(ctx, &group, "s"))
	failure := &UpstreamFailoverError{StatusCode: 502}
	PrepareOpenAIContinuityRecovery(ctx, &accounts[1], failure)
	require.True(t, failure.RetryableOnSameAccount)
	require.True(t, failure.RequestScopedTransient)
	require.Equal(t, 2, failure.SameAccountRetryMax)
	RetryOpenAIContinuityAccount(ctx, 2)
	retry, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.EqualValues(t, 2, retry.Account.ID, "retry must not pick the other account before any successful binding")
	retry.ReleaseFunc()
	require.Zero(t, cache.sessionBindings["openai:s"])
	require.NoError(t, OpenAIContinuityMigrationError(ctx))
	migrated, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", "s", "gpt-5.1", map[int64]struct{}{2: {}}, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, migrated.Account.ID)
	require.Zero(t, cache.sessionBindings["openai:s"])
	CompleteOpenAIContinuity(ctx, migrated.Account)
	require.EqualValues(t, 1, cache.sessionBindings["openai:s"])
	migrated.ReleaseFunc()
}

func TestOpenAIContinuityRecoveryPreservesUnsafeHistoryAndErrorPolicies(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI}
	for _, status := range []int{400, 401, 403, 413, 429} {
		ctx := context.WithValue(context.Background(), openAIContinuityKey{}, &openAIContinuityState{enabled: true, explicitSession: true, hash: "s"})
		failure := &UpstreamFailoverError{StatusCode: status}
		PrepareOpenAIContinuityRecovery(ctx, account, failure)
		require.False(t, failure.RetryableOnSameAccount)
	}
	for _, tc := range []struct {
		state    *openAIContinuityState
		expected error
	}{
		{&openAIContinuityState{enabled: true, explicitSession: true, hash: "s", migrationUnsafe: true}, ErrOpenAIContextIncomplete},
		{&openAIContinuityState{enabled: true, explicitSession: true, hash: "s", previousResponseID: "resp_old"}, ErrOpenAIContinuityUnavailable},
	} {
		ctx := context.WithValue(context.Background(), openAIContinuityKey{}, tc.state)
		failure := &UpstreamFailoverError{StatusCode: 503}
		PrepareOpenAIContinuityRecovery(ctx, account, failure)
		require.True(t, failure.RetryableOnSameAccount, "retrying the original upstream is still safe")
		require.ErrorIs(t, OpenAIContinuityMigrationError(ctx), tc.expected)
	}
}
