package service

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const balancedTranscript = `{"model":"gpt-5.1","metadata":{"n":9007199254740993},"input":[{"role":"user","content":"keep my question"},{"type":"reasoning","id":"rs_original","encrypted_content":"private-cipher"},{"type":"function_call","call_id":"call_1","name":"read_file","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"keep my result"},{"role":"assistant","content":"keep my answer"},{"role":"user","content":"continue"}]}`

func TestBalancedReplayPreservesVisibleTranscriptAndOnlyDropsReasoning(t *testing.T) {
	got, ok := openAIBalancedReplayBody([]byte(balancedTranscript))
	require.True(t, ok)
	require.NotContains(t, string(got), "private-cipher")
	require.NotContains(t, string(got), "rs_original")
	require.Equal(t, "9007199254740993", gjson.GetBytes(got, "metadata.n").Raw)
	want := gjson.Get(balancedTranscript, "input").Array()
	actual := gjson.GetBytes(got, "input").Array()
	require.Len(t, actual, len(want)-1)
	for i, item := range append(want[:1:1], want[2:]...) {
		require.JSONEq(t, item.Raw, actual[i].Raw)
	}
}

func TestBalancedReplayRejectsUnreconstructableAndUnfinishedHistory(t *testing.T) {
	for _, tc := range []struct {
		path  string
		value any
	}{
		{"previous_response_id", "resp_original"},
		{"conversation", "conv_original"},
		{"input.1.type", "compaction"},
		{"input.3.call_id", "unknown"},
		{"input.0.role", "assistant"},
		{"input.3", map[string]any{"role": "user", "content": "missing tool result"}},
		{"input.5", map[string]any{"type": "item_reference", "id": "old"}},
		{"input.5.content", []any{map[string]any{"type": "input_file", "file_id": "private-file"}}},
		{"input.5", map[string]any{"type": "unknown_state", "encrypted_content": "opaque"}},
		{"tools", []any{map[string]any{"type": "file_search", "vector_store_ids": []string{"private"}}}},
	} {
		body, err := sjson.Set(balancedTranscript, tc.path, tc.value)
		require.NoError(t, err)
		_, ok := openAIBalancedReplayBody([]byte(body))
		require.False(t, ok, tc.path)
	}
	for _, body := range []string{`{"input":[{"type":"reasoning","encrypted_content":"x"}]}`, `{"input":"next"}`, `not-json`} {
		_, ok := openAIBalancedReplayBody([]byte(body))
		require.False(t, ok)
	}
}

func TestBalancedOriginalKeepsCipherAndBackupCannotFanOut(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	svc := &OpenAIGatewayService{}
	hash := svc.GenerateSessionHash(c, []byte(balancedTranscript))
	ctx := c.Request.Context()
	defer ManageOpenAIResponsesContinuity(ctx, []byte(balancedTranscript), true)()
	require.NoError(t, svc.beginContinuity(ctx, nil, hash))
	original, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 1}, []byte(balancedTranscript))
	require.NoError(t, err)
	require.Equal(t, balancedTranscript, string(original))
	require.NoError(t, OpenAIContinuityMigrationError(ctx))
	backup, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 2}, []byte(balancedTranscript))
	require.NoError(t, err)
	require.NotContains(t, string(backup), "private-cipher")
	svc.GenerateSessionHash(c, backup) // forwarding/lineage code can recompute the hash
	repeated, err := prepareOpenAIBalancedForward(ctx, &Account{ID: 2}, []byte(balancedTranscript))
	require.NoError(t, err)
	require.NotContains(t, string(repeated), "private-cipher", "same-backup retry must remain sanitized")
	require.ErrorIs(t, OpenAIContinuityMigrationError(ctx), ErrOpenAIRecoveryExhausted)
	_, err = prepareOpenAIBalancedForward(ctx, &Account{ID: 3}, []byte(balancedTranscript))
	require.ErrorIs(t, err, ErrOpenAIRecoveryExhausted)
}

func TestBalancedLongCooldownSkipsWaitAndPreservesBindingUntilSuccess(t *testing.T) {
	group := int64(9)
	reset := time.Now().Add(24 * time.Hour)
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, RateLimitResetAt: &reset},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{}}
	svc := &OpenAIGatewayService{cfg: newSchedulerTestOpenAIWSV2Config(), cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts}, rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Request.Header.Set("session_id", "balanced-cooldown")
	hash := svc.GenerateSessionHash(c, []byte(balancedTranscript))
	cache.sessionBindings["openai:"+hash] = 1
	ctx := c.Request.Context()
	defer ManageOpenAIResponsesContinuity(ctx, []byte(balancedTranscript), true)()
	start := time.Now()
	selection, _, err := svc.SelectAccountWithScheduler(ctx, &group, "", hash, "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 2, selection.Account.ID)
	defer selection.ReleaseFunc()
	require.EqualValues(t, 1, cache.sessionBindings["openai:"+hash])
	body, err := prepareOpenAIBalancedForward(ctx, selection.Account, []byte(balancedTranscript))
	require.NoError(t, err)
	require.True(t, json.Valid(body))
	CompleteOpenAIContinuity(ctx, selection.Account)
	require.EqualValues(t, 2, cache.sessionBindings["openai:"+hash])
}

// Simulate the real lease owner check, and make release ordering observable.
type balancedLeaseCache struct {
	*schedulerTestGatewayCache
	mu        sync.Mutex
	owner     string
	released  chan struct{}
	once      sync.Once
	committed bool
}

func (c *balancedLeaseCache) AcquireContinuityLease(_ context.Context, _ int64, _, owner string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owner = owner
	return true, nil
}
func (c *balancedLeaseCache) RenewContinuityLease(_ context.Context, _ int64, _, owner string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner == owner, nil
}
func (c *balancedLeaseCache) CommitContinuityBinding(_ context.Context, _ int64, _, owner string, _ int64, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.committed = c.owner == owner
	return c.committed, nil
}
func (c *balancedLeaseCache) ReleaseContinuityLease(_ context.Context, _ int64, _, owner string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner == owner {
		c.owner = ""
	}
	c.once.Do(func() { close(c.released) })
	return nil
}
func (c *balancedLeaseCache) CheckContinuityBinding(_ context.Context, _ int64, _, owner string, _ int64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner == owner, nil
}

func TestBalancedLeaseSurvivesClientCancelUntilSuccessfulCommit(t *testing.T) {
	for _, success := range []bool{true, false} {
		cache := &balancedLeaseCache{schedulerTestGatewayCache: &schedulerTestGatewayCache{}, released: make(chan struct{})}
		svc := &OpenAIGatewayService{cache: cache}
		parent, cancel := context.WithCancel(context.Background())
		ctx := context.WithValue(parent, openAIContinuityKey{}, &openAIContinuityState{})
		finish := ManageOpenAIResponsesContinuity(ctx, []byte(`{"input":"hello"}`), true)
		require.NoError(t, svc.beginContinuity(ctx, nil, "session"))
		cancel() // Client received terminal bytes before Forward's deferred commit.
		select {
		case <-cache.released:
			t.Fatal("client cancellation released the lease before handler completion")
		case <-time.After(30 * time.Millisecond):
		}
		if success {
			CompleteOpenAIContinuity(ctx, &Account{ID: 1})
		}
		finish()
		select {
		case <-cache.released:
		case <-time.After(time.Second):
			t.Fatal("handler completion leaked lease")
		}
		cache.mu.Lock()
		require.Equal(t, success, cache.committed)
		cache.mu.Unlock()
	}
}
