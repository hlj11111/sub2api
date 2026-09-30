package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func checkpointTestService() *OpenAIGatewayService {
	cfg := &config.Config{}
	cfg.Totp.EncryptionKey = strings.Repeat("ab", 32)
	cfg.Totp.EncryptionKeyConfigured = true
	return &OpenAIGatewayService{cfg: cfg}
}

func TestContinuityCheckpointEncryptionScopesAndTampering(t *testing.T) {
	svc := checkpointTestService()
	aead, err := svc.continuityCheckpointCipher()
	require.NoError(t, err)
	cp := &continuityCheckpoint{Version: 1, Prefixes: map[string][]json.RawMessage{"digest": {json.RawMessage(`{"role":"user","content":"private history"}`)}}, Order: []string{"digest"}}
	sealed, err := sealContinuityCheckpoint(aead, "v1:2:u1:hash", cp)
	require.NoError(t, err)
	require.NotContains(t, string(sealed), "private history")
	restored, err := openContinuityCheckpoint(aead, "v1:2:u1:hash", sealed)
	require.NoError(t, err)
	require.Equal(t, cp, restored)
	_, err = openContinuityCheckpoint(aead, "v1:2:u2:hash", sealed)
	require.Error(t, err)
	_, err = openContinuityCheckpoint(aead, "v1:3:u1:hash", sealed)
	require.Error(t, err)
	sealed[len(sealed)-1] ^= 1
	_, err = openContinuityCheckpoint(aead, "v1:2:u1:hash", sealed)
	require.Error(t, err)
	svc.cfg.Totp.EncryptionKeyConfigured = false
	aead, err = svc.continuityCheckpointCipher()
	require.NoError(t, err)
	require.Nil(t, aead)
}

func TestContinuityCheckpointCapturedThroughResponseHandlers(t *testing.T) {
	const response = `{"id":"resp_compact","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"keep this decision"}]},{"type":"compaction","encrypted_content":"exact-handler-result"}],"usage":{"input_tokens":5,"output_tokens":3}}`
	sse := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":" + response + "}\n\n"
	for _, mode := range []string{"json", "sse", "sse_to_json", "passthrough_json", "passthrough_sse", "passthrough_sse_to_json"} {
		t.Run(mode, func(t *testing.T) {
			svc := checkpointTestService()
			svc.cfg.Gateway.MaxLineSize = defaultMaxLineSize
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			c.Request.Header.Set("session-id", "handler-checkpoint")
			hash := svc.GenerateSessionHash(c, []byte(balancedTranscript))
			ctx := c.Request.Context()
			defer ManageOpenAIResponsesContinuity(ctx, []byte(balancedTranscript), true)()
			require.NoError(t, svc.beginContinuity(ctx, nil, hash))
			payload, contentType := response, "application/json"
			if strings.Contains(mode, "sse") {
				payload, contentType = sse, "text/event-stream"
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(payload))}
			defer func() { _ = resp.Body.Close() }()
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
			var err error
			switch mode {
			case "json", "sse_to_json":
				_, err = svc.handleNonStreamingResponse(ctx, resp, c, account, "gpt-5.1", "gpt-5.1")
			case "sse":
				_, err = svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "gpt-5.1", "gpt-5.1")
			case "passthrough_json", "passthrough_sse_to_json":
				_, err = svc.handleNonStreamingResponsePassthrough(ctx, resp, c, account, "gpt-5.1", "gpt-5.1")
			case "passthrough_sse":
				_, err = svc.handleStreamingResponsePassthrough(ctx, resp, c, account, time.Now(), "gpt-5.1", "gpt-5.1")
			}
			require.NoError(t, err)
			st := continuityState(ctx)
			sealed, err := st.sealedCheckpoint()
			require.NoError(t, err)
			require.NotEmpty(t, sealed)
			aead, err := svc.continuityCheckpointCipher()
			require.NoError(t, err)
			cp, err := openContinuityCheckpoint(aead, checkpointScope(st.groupID, st.hash), sealed)
			require.NoError(t, err)
			replay, ok := restoreContinuityCompaction([]byte(`{"input":[{"type":"compaction","encrypted_content":"exact-handler-result"},{"role":"user","content":"next turn"}]}`), cp)
			require.True(t, ok)
			for _, text := range []string{"keep this decision", "keep my question", "keep my result", "next turn"} {
				require.Contains(t, string(replay), text)
			}
		})
	}
}

func TestStandaloneCompactionRecoveryMatchesWholeWindowWithoutDuplicates(t *testing.T) {
	svc := checkpointTestService()
	st := &openAIContinuityState{gateway: svc, hash: "u7:standalone", ingressBody: []byte(balancedTranscript), checkpointStandalone: true,
		checkpointOutput: []byte(`[{"role":"user","content":"keep my question"},{"role":"user","content":"continue"},{"type":"compaction","encrypted_content":"standalone-result"}]`)}
	sealed, err := st.sealedCheckpoint()
	require.NoError(t, err)
	require.NotEmpty(t, sealed)
	aead, err := svc.continuityCheckpointCipher()
	require.NoError(t, err)
	cp, err := openContinuityCheckpoint(aead, checkpointScope(st.groupID, st.hash), sealed)
	require.NoError(t, err)
	// Retained user messages precede the compaction. JSON key order is immaterial.
	body := []byte(`{"input":[{"role":"developer","content":"current instructions"},{"content":"keep my question","role":"user"},{"role":"user","content":"continue"},{"type":"compaction","encrypted_content":"standalone-result"},{"role":"user","content":"new turn"}]}`)
	replay, ok := restoreContinuityCompaction(body, cp)
	require.True(t, ok)
	require.True(t, openAIBalancedToolHistoryComplete(replay))
	for _, text := range []string{"keep my question", "keep my answer", "keep my result", "current instructions", "new turn"} {
		require.Equal(t, 1, strings.Count(string(replay), text), "retained items must not be duplicated")
	}
	require.NotContains(t, string(replay), "standalone-result")
	for _, text := range []string{"keep my question", "continue"} {
		changed := strings.Replace(string(body), text, "changed retained item", 1)
		_, restored := restoreContinuityCompaction([]byte(changed), cp)
		require.False(t, restored, "a changed compacted window must not be overwritten")
	}
	_, ok = restoreContinuityCompaction([]byte(`{"input":[{"type":"compaction","encrypted_content":"standalone-result"},{"role":"user","content":"new turn"}]}`), cp)
	require.False(t, ok, "missing retained messages do not establish the complete window")
}

type checkpointMemoryCache struct {
	*balancedLeaseCache
	sealed []byte
}

func (c *checkpointMemoryCache) GetContinuityCheckpoint(context.Context, int64, string) ([]byte, error) {
	return c.sealed, nil
}
func (c *checkpointMemoryCache) CommitContinuityCheckpoint(ctx context.Context, group int64, key, owner string, account int64, ttl time.Duration, sealed []byte, _ time.Duration) (bool, error) {
	ok, err := c.CommitContinuityBinding(ctx, group, key, owner, account, ttl)
	if ok {
		c.sealed = append([]byte(nil), sealed...)
	}
	return ok, err
}

func TestContinuityCompactionRecoveryAcrossTurnsAndChannelSwitch(t *testing.T) {
	svc := checkpointTestService()
	cache := &checkpointMemoryCache{balancedLeaseCache: &balancedLeaseCache{schedulerTestGatewayCache: &schedulerTestGatewayCache{}, released: make(chan struct{})}}
	svc.cache = cache
	newTurn := func(body string) (context.Context, func()) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/responses", nil)
		c.Request.Header.Set("session-id", "checkpoint-session")
		hash := svc.GenerateSessionHash(c, []byte(body))
		ctx := c.Request.Context()
		finish := ManageOpenAIResponsesContinuity(ctx, []byte(body), true)
		require.NoError(t, svc.beginContinuity(ctx, nil, hash))
		svc.loadContinuityCheckpoint(ctx)
		return ctx, finish
	}
	ctx, finish := newTurn(balancedTranscript)
	captureContinuityCheckpointOutput(ctx, []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":"important decision"},{"type":"compaction","encrypted_content":"compressed-turn-1"}]}`))
	CompleteOpenAIContinuity(ctx, &Account{ID: 1})
	finish()
	require.NotEmpty(t, cache.sealed)
	// Wait for the old lease cleanup; stale cleanup is owner-conditional.
	select {
	case <-cache.released:
	case <-time.After(time.Second):
		t.Fatal("lease cleanup")
	}
	body := `{"model":"gpt-5.1","input":[{"type":"compaction","encrypted_content":"compressed-turn-1"},{"role":"user","content":"continue the same task"},{"type":"compaction_trigger"}]}`
	ctx, finish = newTurn(body)
	defer finish()
	original, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 1}, []byte(body))
	require.NoError(t, err)
	require.Equal(t, body, string(original))
	require.True(t, allowOpenAIBalancedMigration(ctx))
	replay, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 2}, []byte(body))
	require.NoError(t, err)
	require.NotContains(t, string(replay), "compressed-turn-1")
	require.Contains(t, string(replay), "important decision")
	require.Contains(t, string(replay), "keep my question")
	require.Contains(t, string(replay), "keep my result")
	require.Contains(t, string(replay), "continue the same task")
	require.True(t, HasCompactionTriggerInInput(replay))
	require.True(t, openAIBalancedToolHistoryComplete(replay))
	// A new compaction after migration remains recoverable with the new prefix.
	captureContinuityCheckpointOutput(ctx, []byte(`{"output":[{"type":"compaction","encrypted_content":"compressed-turn-2"}]}`))
	CompleteOpenAIContinuity(ctx, &Account{ID: 2})
	aead, err := svc.continuityCheckpointCipher()
	require.NoError(t, err)
	st := continuityState(ctx)
	cp, err := openContinuityCheckpoint(aead, checkpointScope(st.groupID, st.hash), cache.sealed)
	require.NoError(t, err)
	next := []byte(`{"input":[{"type":"compaction","encrypted_content":"compressed-turn-2"},{"role":"user","content":"third turn"}]}`)
	restored, ok := restoreContinuityCompaction(next, cp)
	require.True(t, ok)
	require.Contains(t, string(restored), "keep my result")
}

func TestContinuityCheckpointDoesNotGuessMissingOrPartialHistory(t *testing.T) {
	cp := &continuityCheckpoint{Version: 1, Prefixes: map[string][]json.RawMessage{}}
	for _, body := range []string{
		`{"input":[{"type":"compaction","encrypted_content":"unknown"},{"role":"user","content":"continue"}]}`,
		`{"input":[{"role":"user","content":"unrelated"},{"type":"compaction","encrypted_content":"unknown"}]}`,
	} {
		_, ok := restoreContinuityCompaction([]byte(body), cp)
		require.False(t, ok)
	}
	for _, body := range []string{
		`{"input":[{"role":"user","content":"continue"},{"type":"function_call_output","call_id":"missing","output":"result"}]}`,
		`{"input":[{"role":"user","content":[{"type":"input_file","file_id":"private"}]}]}`,
		`{"input":[{"type":"compaction","encrypted_content":"unknown"}]}`,
	} {
		_, ok := portableContinuityReplay([]byte(body))
		require.False(t, ok)
	}
	st := &openAIContinuityState{gateway: checkpointTestService(), ingressBody: []byte(`{"input":[{"type":"compaction","encrypted_content":"unknown"},{"role":"user","content":"continue"}]}`), checkpointOutput: []byte(`[{"type":"compaction","encrypted_content":"new"}]`)}
	sealed, err := st.sealedCheckpoint()
	require.NoError(t, err)
	require.Empty(t, sealed)
}

func TestContinuityBackupBudgetCountsAccountsNotRetries(t *testing.T) {
	st := &openAIContinuityState{enabled: true, balanced: true, migrationUnsafe: true, canDropReasoning: true, backupLimit: 3}
	ctx := context.WithValue(context.Background(), openAIContinuityKey{}, st)
	for _, id := range []int64{2, 3, 4} {
		require.NoError(t, OpenAIContinuityMigrationError(ctx))
		replay, err := prepareOpenAIBalancedForward(ctx, &Account{ID: id}, []byte(balancedTranscript))
		require.NoError(t, err)
		require.True(t, gjson.ValidBytes(replay))
		_, err = prepareOpenAIBalancedForward(ctx, &Account{ID: id}, []byte(balancedTranscript))
		require.NoError(t, err)
	}
	require.ErrorIs(t, OpenAIContinuityMigrationError(ctx), ErrOpenAIRecoveryExhausted)
	_, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 5}, []byte(balancedTranscript))
	require.ErrorIs(t, err, ErrOpenAIRecoveryExhausted)
}

func TestContinuityDrainBoundsOrphanAndAllowsTerminalCommitGrace(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, finish := withContinuityDrainGrace(parent, 30*time.Millisecond)
	defer finish()
	upstream, _ := detachUpstreamContext(ctx)
	cancel()
	require.NoError(t, upstream.Err(), "terminal commit retains its short grace")
	select {
	case <-upstream.Done():
	case <-time.After(time.Second):
		t.Fatal("orphaned request did not terminate")
	}
	require.ErrorIs(t, upstream.Err(), context.Canceled)
}
