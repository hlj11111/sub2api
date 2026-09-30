package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type modelHistoryCache struct {
	*schedulerTestGatewayCache
	models  map[string]int64
	lookups []string
}

func (c *modelHistoryCache) RecoverOpenAIModelContinuityAccount(_ context.Context, _, _ int64, _, model string) (int64, error) {
	c.lookups = append(c.lookups, model)
	if id := c.models[model]; id > 0 {
		return id, nil
	}
	return 0, ErrStickySessionNotFound
}

func TestBalancedModelsKeepIndependentOwnersAcrossDelegatedTurns(t *testing.T) {
	group := int64(9)
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-6-astra": "gpt-6-astra"}}},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-6-luna": "gpt-6-luna"}}},
	}
	cache := &modelHistoryCache{schedulerTestGatewayCache: &schedulerTestGatewayCache{sessionBindings: map[string]int64{}}, models: map[string]int64{"gpt-6-astra": 1, "gpt-6-luna": 2}}
	svc := &OpenAIGatewayService{cfg: newSchedulerTestOpenAIWSV2Config(), cache: cache, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts}, rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"), concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	routes := map[string]string{}
	for _, model := range []string{"gpt-6-astra", "gpt-6-luna", "gpt-6-astra", "gpt-6-luna"} {
		c := cacheRotationContext(7)
		c.Request.Header.Set("session-id", "shared-parent-thread")
		// Opaque history cannot migrate. Recovering the correct model's original
		// owner must work even when the last legacy owner was the other model.
		body := []byte(`{"model":"` + model + `","input":[{"type":"reasoning","encrypted_content":"opaque"}]}`)
		hash := svc.GenerateSessionHash(c, body)
		ctx := c.Request.Context()
		finish := ManageOpenAIResponsesContinuity(ctx, body, true)
		selection, decision, err := svc.SelectAccountWithScheduler(ctx, &group, "", hash, model, nil, OpenAIUpstreamTransportAny, false)
		require.NoError(t, err)
		require.True(t, decision.StickySessionHit)
		require.Equal(t, cache.models[model], selection.Account.ID)
		route := scopedOpenAISessionHash(ctx, hash)
		if previous := routes[model]; previous != "" {
			require.Equal(t, previous, route)
		}
		routes[model] = route
		CompleteOpenAIContinuity(ctx, selection.Account)
		selection.ReleaseFunc()
		finish()
	}
	require.NotEqual(t, routes["gpt-6-astra"], routes["gpt-6-luna"])
	require.EqualValues(t, 1, cache.sessionBindings["openai:"+routes["gpt-6-astra"]])
	require.EqualValues(t, 2, cache.sessionBindings["openai:"+routes["gpt-6-luna"]])
	require.Equal(t, []string{"gpt-6-astra", "gpt-6-luna"}, cache.lookups, "durable model owner wins over legacy usage")
}

func TestBalancedReplayPreservesControlItemsAtTheirOriginalPositions(t *testing.T) {
	const tools = `{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}]}`
	const effort = `{"type":"configuration_update","reasoning":{"effort":"high"}}`
	body, err := sjson.SetRaw(balancedTranscript, "input", `[`+tools+`,`+`{"role":"user","content":"question"},{"type":"reasoning","encrypted_content":"opaque"},`+effort+`,{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]`)
	require.NoError(t, err)
	replay, ok := openAIBalancedReplayBody([]byte(body))
	require.True(t, ok)
	require.NotContains(t, string(replay), "opaque")
	require.JSONEq(t, tools, gjson.GetBytes(replay, "input.0").Raw)
	require.JSONEq(t, effort, gjson.GetBytes(replay, "input.2").Raw)
	for _, replacement := range []string{
		`{"type":"additional_tools","role":"user","tools":[]}`,
		`{"type":"additional_tools","role":"developer","tools":["missing schema"]}`,
		`{"type":"additional_tools","role":"developer","tools":[{"type":"file_search","vector_store_ids":["private"]}]}`,
		`{"type":"configuration_update","reasoning":{"effort":null}}`,
		`{"type":"unknown_state"}`,
	} {
		bad, setErr := sjson.SetRaw(body, "input.0", replacement)
		require.NoError(t, setErr)
		_, valid := openAIBalancedReplayBody([]byte(bad))
		require.False(t, valid, replacement)
	}
}

func TestCompactionRecoveryRetainsToolDefinitionsAndMidHistoryUpdates(t *testing.T) {
	const tools = `{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"read_file"}]}`
	const update = `{"type":"configuration_update","reasoning":{"effort":"high"}}`
	prefix := []json.RawMessage{json.RawMessage(tools), json.RawMessage(`{"role":"developer","content":"old instructions"}`), json.RawMessage(`{"role":"user","content":"question"}`), json.RawMessage(update), json.RawMessage(`{"role":"assistant","content":"answer"}`)}
	digest := compactionDigest(gjson.Parse(`{"encrypted_content":"exact-compaction"}`))
	cp := &continuityCheckpoint{Version: 1, Prefixes: map[string][]json.RawMessage{digest: prefix}}
	for _, leading := range []string{`{"role":"developer","content":"current instructions"}`, tools + `,{"role":"developer","content":"current instructions"}`} {
		body := []byte(`{"input":[` + leading + `,{"type":"compaction","encrypted_content":"exact-compaction"},{"role":"user","content":"next"}]}`)
		restored, ok := restoreContinuityCompaction(body, cp)
		require.True(t, ok)
		replay, ok := portableContinuityReplay(restored)
		require.True(t, ok)
		items := gjson.GetBytes(replay, "input").Array()
		toolCount, updateIndex, userIndex := 0, -1, -1
		for i, item := range items {
			if item.Get("type").String() == "additional_tools" {
				toolCount++
			}
			if item.Get("type").String() == "configuration_update" {
				updateIndex = i
			}
			if userIndex < 0 && item.Get("role").String() == "user" {
				userIndex = i
			}
		}
		require.Equal(t, 1, toolCount)
		require.Greater(t, updateIndex, userIndex, "reasoning updates retain their historical position")
		require.NotContains(t, string(replay), "old instructions")
	}
}
