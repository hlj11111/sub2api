//go:build unit

package handler

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOpenAIContinuityAdmissionTimeoutDoesNotWriteBeforeSafeReselection(t *testing.T) {
	for _, previous := range []string{"", "resp_history"} {
		t.Run("previous="+previous, func(t *testing.T) {
			h := newOpenAIResponsesFailoverTestHandler(t, nil)
			h.concurrencyHelper = NewConcurrencyHelper(service.NewConcurrencyService(&helperConcurrencyCacheStub{}), SSEPingFormatNone, 0)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			c.Request.Header.Set("session_id", "timeout-session")
			hash := h.gatewayService.GenerateSessionHash(c, []byte("{}"))
			initial, _, err := h.gatewayService.SelectAccountWithScheduler(c.Request.Context(), nil, previous, hash, "gpt-5.1", nil, service.OpenAIUpstreamTransportAny, false)
			if previous == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, service.ErrOpenAIContextIncomplete)
			}
			if initial != nil && initial.ReleaseFunc != nil {
				initial.ReleaseFunc()
			}
			selection := &service.AccountSelectionResult{Account: profitSlotTestAccount(1, 1), WaitPlan: &service.AccountWaitPlan{AccountID: 1, MaxConcurrency: 1, Timeout: time.Millisecond, MaxWaiting: 2}}
			started := false
			release, result := h.acquireResponsesAccountSlot(c, nil, hash, selection, false, &started, zap.NewNop())
			require.Nil(t, release)
			if previous == "" {
				require.Equal(t, openAISlotAcquireContinuityRetry, result)
				require.Zero(t, recorder.Body.Len(), "no error before attempting an authorized replacement")
				_, result = h.acquireResponsesAccountSlot(c, nil, hash, selection, false, &started, zap.NewNop())
			}
			require.Equal(t, openAISlotAcquireFailed, result, "history IDs and exhausted migration budgets must stop")
			require.NotZero(t, recorder.Body.Len())
		})
	}
}

func TestOpenAIWSIngressFallbackIsConnectionScoped(t *testing.T) {
	group := int64(9)
	first := openAIWSIngressFallbackSessionSeed(7, 11, &group)
	next := openAIWSIngressFallbackSessionSeed(7, 11, &group)
	require.NotEmpty(t, first)
	require.NotEqual(t, first, next, "missing conversation IDs must not merge all sockets belonging to one key")
}
