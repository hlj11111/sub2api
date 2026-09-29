package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func cacheRotationContext(userID int64) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.UserID, userID))
	return c
}

func TestOpenAIContinuityRotatingCacheKeyKeepsSelectedAccount(t *testing.T) {
	for _, format := range []string{"responses", "chat", "websocket envelope"} {
		t.Run(format, func(t *testing.T) {
			group := int64(9)
			accounts := []Account{
				{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 10, GroupIDs: []int64{group}},
				{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{group}},
			}
			cache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
			svc := &OpenAIGatewayService{cfg: newSchedulerTestOpenAIWSV2Config(), cache: cache,
				accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
			}
			history := []map[string]string{{"role": "user", "content": "first user turn"}}
			var firstRoute, lastCacheHash string
			for turn := 0; turn < 3; turn++ {
				c := cacheRotationContext(7)
				model := "gpt-5.1"
				if turn == 2 {
					model = "gpt-5.2"
				}
				cacheKey := fmt.Sprintf("cache-turn-%d", turn)
				payload := map[string]any{"model": model, "prompt_cache_key": cacheKey}
				if format == "chat" {
					payload["messages"] = history
				} else {
					payload["input"] = history
				}
				if format == "websocket envelope" {
					payload = map[string]any{"type": "response.create", "response": payload}
				}
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				original := append([]byte(nil), body...)
				hash := svc.GenerateSessionHash(c, body)
				require.Equal(t, DeriveSessionHashFromSeed(cacheKey), hash, "upstream cache identity stays compatible")
				require.Equal(t, cacheKey, svc.ExtractSessionID(c, body))
				require.Equal(t, original, body, "routing must not rewrite the request")
				require.NotEqual(t, lastCacheHash, hash)
				route := scopedOpenAISessionHash(c.Request.Context(), hash)
				if turn == 0 {
					firstRoute = route
				} else {
					require.Equal(t, firstRoute, route)
				}
				var excluded map[int64]struct{}
				if turn == 0 {
					excluded = map[int64]struct{}{2: {}}
				}
				selection, decision, err := svc.SelectAccountWithScheduler(c.Request.Context(), &group, "", hash, model, excluded, OpenAIUpstreamTransportAny, false)
				require.NoError(t, err)
				require.NotNil(t, selection)
				require.EqualValues(t, 1, selection.Account.ID, "a better-scored account must not steal a bound conversation")
				if turn > 0 {
					require.True(t, decision.StickySessionHit)
				}
				CompleteOpenAIContinuity(c.Request.Context(), selection.Account)
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				require.EqualValues(t, 1, cache.sessionBindings["openai:"+firstRoute])
				lastCacheHash = hash
				history = append(history, map[string]string{"role": "assistant", "content": "answer"}, map[string]string{"role": "user", "content": fmt.Sprintf("next %d", turn)})
			}
		})
	}
}

func TestOpenAIContinuityExplicitIdentitySurvivesCacheAndInputChanges(t *testing.T) {
	for _, signal := range []string{"session header", "thread header", "turn metadata header", "body thread", "body session", "body conversation", "metadata session", "body turn metadata"} {
		t.Run(signal, func(t *testing.T) {
			var routes []string
			for turn := 0; turn < 2; turn++ {
				c := cacheRotationContext(7)
				payload := map[string]any{"model": "gpt-5.1", "prompt_cache_key": fmt.Sprintf("rotating-%d", turn), "input": fmt.Sprintf("incremental input %d", turn)}
				switch signal {
				case "session header":
					c.Request.Header.Set("session_id", "session-one")
				case "thread header":
					c.Request.Header.Set("thread-id", "thread-one")
				case "turn metadata header":
					c.Request.Header.Set("x-codex-turn-metadata", `{"thread_id":"thread-one","turn_id":"rotating"}`)
				case "body thread":
					payload["client_metadata"] = map[string]string{"thread_id": "thread-one"}
				case "body session":
					payload["client_metadata"] = map[string]string{"session_id": "session-one"}
				case "body conversation":
					payload["client_metadata"] = map[string]string{"conversation_id": "conversation-one"}
				case "metadata session":
					payload["metadata"] = map[string]string{"session_id": "session-one"}
				case "body turn metadata":
					payload["client_metadata"] = map[string]string{"x-codex-turn-metadata": `{"thread_id":"thread-one","turn_id":"rotating"}`}
				}
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				svc := &OpenAIGatewayService{}
				hash := svc.GenerateSessionHash(c, body)
				routes = append(routes, scopedOpenAISessionHash(c.Request.Context(), hash))
			}
			require.Equal(t, routes[0], routes[1])
		})
	}
}

func TestOpenAIContinuityCacheHintDoesNotMergeConversations(t *testing.T) {
	route := func(userID int64, header, thread, input string) string {
		c := cacheRotationContext(userID)
		if header != "" {
			c.Request.Header.Set("session-id", header)
		}
		body, err := json.Marshal(map[string]any{"model": "gpt-5.1", "prompt_cache_key": "shared-cache", "input": input, "client_metadata": map[string]string{"thread_id": thread}})
		require.NoError(t, err)
		hash := (&OpenAIGatewayService{}).GenerateSessionHash(c, body)
		return scopedOpenAISessionHash(c.Request.Context(), hash)
	}
	require.NotEqual(t, route(7, "", "thread-a", "hi"), route(7, "", "thread-b", "hi"))
	require.NotEqual(t, route(7, "", "", "first A"), route(7, "", "", "first B"))
	require.NotEqual(t, route(7, "", "thread-a", "hi"), route(8, "", "thread-a", "hi"))
	require.NotEqual(t, route(7, "session-a", "same", "hi"), route(7, "session-b", "same", "hi"))
	require.Equal(t, route(7, "explicit", "thread-a", "first"), route(7, "explicit", "thread-b", "later"), "explicit session header remains authoritative for account affinity")
}

func TestOpenAIContinuityWebSocketCacheRotationKeepsInitialRoute(t *testing.T) {
	svc := &OpenAIGatewayService{}
	c := cacheRotationContext(7)
	first := []byte(`{"type":"response.create","response":{"model":"gpt-5.1","prompt_cache_key":"first-cache","input":"initial question"}}`)
	firstHash := svc.GenerateSessionHashWithFallback(c, first, "connection-a")
	firstRoute := scopedOpenAISessionHash(c.Request.Context(), firstHash)
	for _, body := range []string{
		`{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"next-cache","input":"only the next user turn"}`,
		`{"type":"response.create","response":{"model":"gpt-5.2","prompt_cache_key":"third-cache","input":[{"type":"function_call_output","call_id":"call_1","output":"result"}]}}`,
	} {
		hash := svc.GenerateSessionHash(c, []byte(body))
		require.NotEqual(t, firstHash, hash)
		require.Equal(t, firstRoute, scopedOpenAISessionHash(c.Request.Context(), hash))
	}
	require.True(t, continuityState(c.Request.Context()).migrationUnsafe, "routing stability must not make incomplete tool history portable")
}

func TestOpenAIContinuityWebSocketUnanchoredConnectionsRemainIsolated(t *testing.T) {
	svc := &OpenAIGatewayService{}
	var routes []string
	for _, connection := range []string{"connection-a", "connection-b"} {
		c := cacheRotationContext(7)
		hash := svc.GenerateSessionHashWithFallback(c, []byte(`{"model":"gpt-5.1","prompt_cache_key":"shared-cache"}`), connection)
		routes = append(routes, scopedOpenAISessionHash(c.Request.Context(), hash))
	}
	require.NotEqual(t, routes[0], routes[1], "model/cache hints alone cannot establish shared conversation identity")
}

func TestOpenAIExecutionScopeIgnoresRotatingCacheHints(t *testing.T) {
	c := cacheRotationContext(7)
	for _, body := range []string{
		`{"prompt_cache_key":"cache-a","input":"first"}`,
		`{"type":"response.create","response":{"prompt_cache_key":"cache-b","input":"next"}}`,
	} {
		scope, _ := resolveOpenAIWSExecutionScope(c, []byte(body), 11)
		require.Empty(t, scope, "cache keys cannot preempt unrelated sessions")
	}
	first, thread := resolveOpenAIWSExecutionScope(c, []byte(`{"type":"response.create","response":{"prompt_cache_key":"cache-a","client_metadata":{"thread_id":"thread-a"}}}`), 11)
	next, _ := resolveOpenAIWSExecutionScope(c, []byte(`{"prompt_cache_key":"cache-b","client_metadata":{"thread_id":"thread-a"}}`), 11)
	require.Equal(t, "thread-a", thread)
	require.NotEmpty(t, first)
	require.Equal(t, first, next)
	bodySession, _ := resolveOpenAIWSExecutionScope(c, []byte(`{"client_metadata":{"session_id":"session-a"}}`), 11)
	c.Request.Header.Set("session-id", "session-a")
	headerSession, _ := resolveOpenAIWSExecutionScope(c, []byte(`{}`), 11)
	require.NotEmpty(t, bodySession)
	require.Equal(t, headerSession, bodySession)
}

func TestOpenAIContinuityCacheLookupDoesNotClearMigrationSafety(t *testing.T) {
	c := cacheRotationContext(7)
	c.Request.Header.Set("session-id", "session-one")
	svc := &OpenAIGatewayService{}
	svc.GenerateSessionHash(c, []byte(`{"type":"response.create","response":{"input":[{"type":"item_reference","id":"item-account-scoped"}]}}`))
	require.True(t, continuityState(c.Request.Context()).migrationUnsafe)
	svc.GenerateSessionHash(c, nil)
	require.True(t, continuityState(c.Request.Context()).migrationUnsafe, "header-only cache lookup must not mark incomplete context portable")
}

func TestOpenAIContinuityWrappedInputWithoutCacheKeyUsesContentRoute(t *testing.T) {
	svc := &OpenAIGatewayService{}
	flat := cacheRotationContext(7)
	wrapped := cacheRotationContext(7)
	body := []byte(`{"model":"gpt-5.1","input":"first question"}`)
	event := []byte(`{"type":"response.create","response":{"model":"gpt-5.1","input":"first question"}}`)
	flatHash := svc.GenerateSessionHash(flat, body)
	wrappedHash := svc.GenerateSessionHash(wrapped, event)
	require.NotEmpty(t, wrappedHash)
	require.Equal(t, flatHash, wrappedHash)
	require.Equal(t, scopedOpenAISessionHash(flat.Request.Context(), flatHash), scopedOpenAISessionHash(wrapped.Request.Context(), wrappedHash))
}
