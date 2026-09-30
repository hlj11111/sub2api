//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type balancedRecoveryUpstream struct {
	service.HTTPUpstream
	accounts  []int64
	bodies    []string
	failFirst bool
	failAll   bool
}

func (u *balancedRecoveryUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	u.accounts = append(u.accounts, accountID)
	u.bodies = append(u.bodies, string(body))
	status, payload := 200, `{"id":"resp_ok","object":"response","status":"completed","model":"gpt-5.1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	if u.failAll || (u.failFirst && len(u.accounts) == 1) {
		status, payload = 520, `{"error":{"message":"temporary provider failure"}}`
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(payload))}, nil
}

const balancedHandlerBody = `{"model":"gpt-5.1","stream":false,"input":[{"role":"user","content":"original question"},{"type":"reasoning","encrypted_content":"keep-on-original"},{"role":"assistant","content":"original answer"},{"role":"user","content":"next question"}]}`

func TestResponsesBalancedFailoverPreservesOriginalThenDropsAuxiliaryReasoning(t *testing.T) {
	for _, fail := range []bool{false, true} {
		upstream := &balancedRecoveryUpstream{failFirst: fail}
		cache := testutil.NewRedisGatewayCache(t)
		h := newOpenAIResponsesFailoverTestHandlerWithCache(t, upstream, cache)
		c, rec := newOpenAIResponsesFailoverTestContext(t, context.Background())
		c.Request.Body = io.NopCloser(strings.NewReader(balancedHandlerBody))
		c.Request.ContentLength = int64(len(balancedHandlerBody))
		c.Request.Header.Set("session_id", "balanced-handler")
		hash := h.gatewayService.GenerateSessionHash(c, []byte(balancedHandlerBody))
		require.NoError(t, cache.SetSessionAccountID(c.Request.Context(), 3131, "openai:"+hash, 1, time.Hour))
		h.Responses(c)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Contains(t, upstream.bodies[0], "keep-on-original")
		bound, err := cache.GetSessionAccountID(c.Request.Context(), 3131, "openai:u100:"+hash)
		require.NoError(t, err)
		if fail {
			require.EqualValues(t, 2, bound, "successful recovery must bind the replacement channel")
			require.Equal(t, []int64{1, 2}, upstream.accounts)
			require.NotContains(t, upstream.bodies[1], "keep-on-original")
			for _, text := range []string{"original question", "original answer", "next question"} {
				require.Contains(t, upstream.bodies[1], text)
			}
		} else {
			require.EqualValues(t, 1, bound)
			require.Equal(t, []int64{1}, upstream.accounts)
		}
	}
}

func TestResponsesBalancedMissingBindingTriesAvailableBackups(t *testing.T) {
	upstream := &balancedRecoveryUpstream{failAll: true}
	h := newOpenAIResponsesFailoverTestHandler(t, upstream)
	c, rec := newOpenAIResponsesFailoverTestContext(t, context.Background())
	c.Request.Body = io.NopCloser(strings.NewReader(balancedHandlerBody))
	c.Request.ContentLength = int64(len(balancedHandlerBody))
	c.Request.Header.Set("session_id", "missing-binding")
	h.Responses(c)
	require.Equal(t, []int64{1, 2}, upstream.accounts, "portable history should try the remaining available channel")
	for _, body := range upstream.bodies {
		require.NotContains(t, body, "keep-on-original")
		for _, text := range []string{"original question", "original answer", "next question"} {
			require.Contains(t, body, text)
		}
	}
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "UPSTREAM_REQUEST_FAILED", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	require.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
}

func TestContinuityErrorsAreChineseTypedAndPreservedInSSE(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		c, rec := newGinContextForEndpoint(t, EndpointResponses)
		h := &OpenAIGatewayHandler{}
		require.True(t, h.handleContinuitySelectionError(c, service.ErrOpenAIReplayIncomplete, streaming))
		var code, kind, message string
		if streaming {
			_, detail := parseResponsesFailedSSE(t, rec.Body.String())
			code, _ = detail["code"].(string)
			message, _ = detail["message"].(string)
			marked, ok := service.GetOpsStreamError(c)
			require.True(t, ok)
			kind = marked.ErrType
			require.Equal(t, "LOCAL_SESSION_REPLAY_INCOMPLETE", marked.Code)
		} else {
			code = gjson.GetBytes(rec.Body.Bytes(), "error.code").String()
			kind = gjson.GetBytes(rec.Body.Bytes(), "error.type").String()
			message = gjson.GetBytes(rec.Body.Bytes(), "error.message").String()
		}
		require.Equal(t, "LOCAL_SESSION_REPLAY_INCOMPLETE", code)
		require.Equal(t, "local_validation_error", kind)
		require.Contains(t, message, "本地上下文校验未通过")
	}
}

func TestLocalReplayRejectionIsNotMisclassifiedAsEarlierUpstreamFailure(t *testing.T) {
	c, _ := newGinContextForEndpoint(t, EndpointResponses)
	service.SetOpsUpstreamError(c, 503, "provider unavailable", "")
	phase, _, owner, source := classifyOpsErrorLog(c, "local_validation_error", "本地校验未通过", "LOCAL_SESSION_REPLAY_INCOMPLETE", 409)
	require.Equal(t, "request", phase)
	require.Equal(t, "gateway", owner)
	require.Equal(t, "gateway", source)
	phase, _, owner, source = classifyOpsErrorLog(c, "upstream_error", "上游服务暂时不可用", "UPSTREAM_UNAVAILABLE", 502)
	require.Equal(t, "upstream", phase)
	require.Equal(t, "provider", owner)
	require.Equal(t, "upstream_http", source)
}

func TestOpenAIClientErrorHasDistinctUpstreamAndLocalCodes(t *testing.T) {
	for _, tc := range []struct{ input, code, text string }{
		{"Upstream service temporarily unavailable", "UPSTREAM_UNAVAILABLE", "上游"},
		{"Upstream rate limit exceeded, please retry later", "UPSTREAM_RATE_LIMITED", "上游"},
		{"Failed to parse request body", "LOCAL_REQUEST_JSON_INVALID", "本地"},
		{"No available accounts", "LOCAL_NO_AVAILABLE_ACCOUNTS", "本地"},
	} {
		_, code, msg := openAIClientError("api_error", "", tc.input)
		require.Equal(t, tc.code, code)
		require.Contains(t, msg, tc.text)
	}
	_, code, msg := openAIClientError("upstream_error", "provider_custom", "custom provider detail")
	require.Equal(t, "provider_custom", code)
	require.Equal(t, "custom provider detail", msg)
}

func TestBalancedRecoveryWindowNeverOverridesConfiguredRetryCount(t *testing.T) {
	for _, limit := range []int{0, 1, 3} {
		h := newOpenAIResponsesFailoverTestHandler(t, nil)
		c, _ := newOpenAIResponsesFailoverTestContext(t, context.Background())
		h.gatewayService.GenerateSessionHash(c, []byte(`{"input":"hello"}`))
		finish := service.ManageOpenAIResponsesContinuity(c.Request.Context(), []byte(`{"input":"hello"}`), true)
		account := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Credentials: map[string]any{"pool_mode": true, "pool_mode_retry_count": limit}}
		failure := &service.UpstreamFailoverError{StatusCode: 503, RetryableOnSameAccount: true}
		service.PrepareOpenAIContinuityRecovery(c.Request.Context(), account, failure)
		require.False(t, failure.SameAccountRetryDeadline.IsZero())
		effective := effectiveSameAccountRetryLimit(failure, account)
		require.False(t, sameAccountRetryAllowed(failure, limit, effective), "deadline must not increase retry count")
		if limit > 0 {
			require.True(t, sameAccountRetryAllowed(failure, limit-1, effective))
		}
		finish()
	}
}
