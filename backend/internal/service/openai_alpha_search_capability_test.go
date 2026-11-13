package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAccountStandaloneSearchSwitch(t *testing.T) {
	for _, accountType := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, capabilities := range []any{nil, []any{"chat_completions"}, []any{"chat_completions", "alpha_search"}} {
			for _, enabled := range []any{nil, true, false} {
				account := &Account{Platform: PlatformOpenAI, Type: accountType, Credentials: map[string]any{
					"openai_capabilities": capabilities,
				}}
				if enabled != nil {
					account.Credentials["openai_alpha_search_enabled"] = enabled
				}
				require.Equal(t, enabled != false, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityAlphaSearch))
				require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityChatCompletions))
				require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityResponses))
			}
		}
	}
	// The new switch must not widen an existing embeddings-only restriction.
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"openai_capabilities": []any{"embeddings"}, "openai_alpha_search_enabled": true,
	}}
	require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityAlphaSearch))
	require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityEmbeddings))
}

func TestSelectAccountStandaloneSearchDisabled(t *testing.T) {
	for _, advanced := range []string{"false", "true"} {
		t.Run("advanced_"+advanced, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
			groupID := int64(10125)
			disabled := Account{ID: 38001, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
				Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID},
				Credentials: map[string]any{"openai_alpha_search_enabled": false},
			}
			enabled := disabled
			enabled.ID, enabled.Priority, enabled.Credentials = 38002, 5, nil
			newService := func(accounts []Account) *OpenAIGatewayService {
				return &OpenAIGatewayService{
					accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts},
					cache: &schedulerTestGatewayCache{sessionBindings: map[string]int64{
						"openai:search-session": disabled.ID,
					}},
					cfg: &config.Config{}, rateLimitService: newOpenAIAdvancedSchedulerRateLimitService(advanced),
					concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
				}
			}
			selectAccount := func(svc *OpenAIGatewayService, capability OpenAIEndpointCapability) (*AccountSelectionResult, error) {
				selection, _, err := svc.SelectAccountWithSchedulerForCapability(context.Background(), &groupID,
					"", "search-session", "gpt-6-astra", nil, OpenAIUpstreamTransportHTTPSSE, capability, false, false, false)
				if selection != nil && selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				return selection, err
			}
			selection, err := selectAccount(newService([]Account{disabled, enabled}), OpenAIEndpointCapabilityAlphaSearch)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, enabled.ID, selection.Account.ID, "search must bypass even a sticky disabled account")
			selection, err = selectAccount(newService([]Account{disabled}), OpenAIEndpointCapabilityAlphaSearch)
			require.Error(t, err)
			require.Nil(t, selection)
			selection, err = selectAccount(newService([]Account{disabled}), OpenAIEndpointCapabilityChatCompletions)
			require.NoError(t, err)
			require.Equal(t, disabled.ID, selection.Account.ID, "normal conversations must remain eligible")
		})
	}
}

func TestForwardAlphaSearchDisabledDoesNotContactUpstream(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	svc := &OpenAIGatewayService{}
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"openai_alpha_search_enabled": false}}
	result, err := svc.ForwardAlphaSearch(context.Background(), c, account, []byte(`{"model":"gpt-6-astra"}`))
	require.Nil(t, result)
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.True(t, failover.EndpointUnsupported)
	require.False(t, c.Writer.Written())
}

func TestAlphaSearchUnsupportedResponseClassification(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	for _, tc := range []struct {
		status      int
		body        string
		unsupported bool
	}{
		{500, `{"error":{"message":"channel does not support /v1/alpha/search"}}`, true},
		{400, `{"detail":"/v1/alpha/search is not supported"}`, true},
		{501, `{"message":"/alpha/search is not implemented"}`, true},
		{500, `{"error":{"message":"unsupported endpoint /v1/alpha/search"}}`, true},
		{500, `{"error":{"message":"internal server error"}}`, false},
		{500, `{"error":{"message":"channel does not support /v1/responses"}}`, false},
		{500, `{"error":{"message":"/alpha/search temporarily unavailable"}}`, false},
		{500, `{"input":"channel does not support /v1/alpha/search","error":{"message":"server error"}}`, false},
		{401, `{"error":{"message":"channel does not support /v1/alpha/search"}}`, false},
		{429, `{"error":{"message":"channel does not support /v1/alpha/search"}}`, false},
	} {
		t.Run(fmt.Sprintf("%d_%s", tc.status, tc.body), func(t *testing.T) {
			require.Equal(t, tc.unsupported, isOpenAIAlphaSearchEndpointUnsupported(account, tc.status, []byte(tc.body)))
		})
	}
}

func TestAlphaSearchUnsupportedDoesNotPenalizeModel(t *testing.T) {
	for _, pool := range []bool{false, true} {
		t.Run(fmt.Sprintf("pool_%t", pool), func(t *testing.T) {
			upstream := &httpUpstreamRecorder{}
			repo := &alphaSearchAccountStateRepo{}
			cfg := &config.Config{}
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, accountRepo: repo,
				rateLimitService: NewRateLimitService(repo, nil, cfg, nil, nil)}
			account := &Account{ID: 42, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
				Status: StatusActive, Schedulable: true, Credentials: map[string]any{
					"api_key": "sk-test", "base_url": "https://relay.example", "pool_mode": pool,
				}}
			for range 3 {
				body := []byte(`{"model":"gpt-6-astra","commands":{"search_query":[{"q":"news"}]}}`)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(body))
				upstream.resp = &http.Response{StatusCode: 500, Header: http.Header{},
					Body: io.NopCloser(strings.NewReader(`{"error":{"message":"channel does not support /v1/alpha/search"}}`))}
				result, err := svc.ForwardAlphaSearch(context.Background(), c, account, body)
				require.Nil(t, result)
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.True(t, failover.EndpointUnsupported)
				require.False(t, failover.RetryableOnSameAccount)
				require.True(t, failover.ShouldRetryNextAccount())
				require.False(t, failover.ShouldReportAccountScheduleFailure())
				require.False(t, svc.ReportOpenAIAccountScheduleResult(account, "gpt-6-astra", false, nil, err))
				require.False(t, c.Writer.Written())
			}
			require.Zero(t, svc.getOpenAIAccountModelTransientState().size())
			require.Zero(t, repo.setErrorCalls)
			require.True(t, account.Schedulable)
			require.Equal(t, StatusActive, account.Status)
		})
	}
}

func TestAlphaSearchUnsupportedDoesNotAffectSchedulerStats(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	svc := &OpenAIGatewayService{rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true")}
	require.NotNil(t, svc.getOpenAIAccountScheduler(context.Background()))
	account := &Account{ID: 42}
	// Existing model failure state must neither be strengthened nor cleared.
	svc.recordOpenAIAccountModelTransientFailure(account, "gpt-6-astra", time.Now())
	svc.recordOpenAIAccountModelTransientFailure(account, "gpt-6-astra", time.Now())
	err := fmt.Errorf("search failed: %w", &UpstreamFailoverError{StatusCode: 500, EndpointUnsupported: true})
	svc.ReportOpenAIAccountScheduleResult(account, "gpt-6-astra", false, nil, err)
	rate, _, _ := svc.openaiAccountStats.snapshot(account.ID)
	require.Zero(t, rate)
	require.True(t, svc.isOpenAIAccountModelRuntimeBlocked(account, "gpt-6-astra"))
	svc.ReportOpenAIAccountScheduleResult(account, "gpt-6-astra", false, nil, &UpstreamFailoverError{StatusCode: 500})
	rate, _, _ = svc.openaiAccountStats.snapshot(account.ID)
	require.Greater(t, rate, 0.0, "ordinary server failures must still affect scheduler health")
}
