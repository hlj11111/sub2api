package service

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

const openAIBalancedRecoveryWindow = 8 * time.Second

// ManageOpenAIResponsesContinuity ties lease cleanup to handler completion, not
// client cancellation. A client can close as soon as it sees the terminal event,
// while Forward is still committing the successful account. No request payload
// is retained beyond this request or written to the binding store.
func ManageOpenAIResponsesContinuity(ctx context.Context, body []byte, balanced bool) func() {
	st := continuityState(ctx)
	if st == nil {
		return func() {}
	}
	st.mu.Lock()
	st.handlerDone = make(chan struct{})
	st.balanced = balanced
	if len(body) <= continuityCheckpointMaxBytes {
		st.ingressBody = append([]byte(nil), body...)
	}
	if balanced {
		_, st.canDropReasoning = openAIBalancedReplayBody(body)
		if !openAIBalancedToolHistoryComplete(body) {
			st.migrationUnsafe = true
			st.nonPortableReason = "unmatched_tool_output"
			st.canDropReasoning = false
		}
	}
	done := st.handlerDone
	st.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

func openAIBalancedToolHistoryComplete(body []byte) bool {
	pending := map[string]bool{}
	for _, item := range gjson.GetBytes(body, "input").Array() {
		kind, id := item.Get("type").String(), strings.TrimSpace(item.Get("call_id").String())
		if isCodexToolCallContextItemType(kind) {
			if _, duplicate := pending[id]; id == "" || duplicate {
				return false
			}
			pending[id] = true
		} else if isCodexToolCallOutputItemType(kind) {
			if !pending[id] {
				return false
			}
			pending[id] = false
		}
	}
	for _, unfinished := range pending {
		if unfinished {
			return false
		}
	}
	return true
}

// This is deliberately a narrow, structural replay check. It cannot reconstruct
// omitted messages. Require a transcript beginning with a user message and
// preceding assistant output, and completed tool pairs in order. Compaction,
// hosted tools, references and unknown item types remain non-portable.
func openAIBalancedReplayBody(body []byte) ([]byte, bool) {
	if !gjson.ValidBytes(body) {
		return nil, false
	}
	root := gjson.ParseBytes(body)
	for _, key := range []string{"previous_response_id", "conversation"} {
		v := root.Get(key)
		if v.Exists() && v.Type != gjson.Null && v.String() != "" {
			return nil, false
		}
	}
	input := root.Get("input")
	if !input.IsArray() {
		return nil, false
	}
	var kept []json.RawMessage
	pending := map[string]bool{}
	seenUser, seenAssistant, dropped := false, false, false
	for _, item := range input.Array() {
		if !item.IsObject() {
			return nil, false
		}
		kind := item.Get("type").String()
		switch {
		case kind == "compaction_trigger":
			// This is a current-turn instruction, not opaque historical state.
		case kind == "reasoning":
			if item.Get("encrypted_content").String() != "" {
				if !seenUser {
					return nil, false
				}
				dropped = true
				continue // auxiliary reasoning only; never a compaction item
			}
			if item.Get("id").String() != "" {
				return nil, false // opaque reasoning reference without its content
			}
		case kind == "message" || kind == "":
			role := item.Get("role").String()
			switch role {
			case "system", "developer":
			case "user":
				if !item.Get("content").Exists() {
					return nil, false
				}
				seenUser = true
			case "assistant":
				if !seenUser || !item.Get("content").Exists() {
					return nil, false
				}
				seenAssistant = true
			default:
				return nil, false
			}
		case isCodexToolCallContextItemType(kind):
			id := strings.TrimSpace(item.Get("call_id").String())
			if !seenUser || id == "" {
				return nil, false
			}
			if _, duplicate := pending[id]; duplicate {
				return nil, false
			}
			pending[id] = true
			seenAssistant = true
		case isCodexToolCallOutputItemType(kind):
			id := strings.TrimSpace(item.Get("call_id").String())
			if !pending[id] {
				return nil, false
			}
			pending[id] = false
		default:
			return nil, false
		}
		kept = append(kept, json.RawMessage(item.Raw))
	}
	for _, unfinished := range pending {
		if unfinished {
			return nil, false
		}
	}
	if !dropped || !seenUser || !seenAssistant {
		return nil, false
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, false
	}
	replay, err := sjson.SetRawBytes(body, "input", encoded)
	if err != nil || openAIRequestNonPortableReason(replay) != "" {
		return nil, false
	}
	return replay, true
}

// Caller holds st.mu. Different backup accounts share a bounded request budget.
func (st *openAIContinuityState) allowBalancedMigration() bool {
	if !st.balanced || st.previousResponseID != "" || st.backupBudgetExhausted() {
		return false
	}
	if st.migrationUnsafe && !st.canDropReasoning {
		return false
	}
	st.migrationPending = true
	return true
}

func allowOpenAIBalancedMigration(ctx context.Context) bool {
	st := continuityState(ctx)
	if st == nil || ctx.Err() != nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.allowBalancedMigration()
}

func (s *OpenAIGatewayService) continuityRecoveryWindow(ctx context.Context) time.Duration {
	d := s.schedulingConfig().StickySessionWaitTimeout
	if st := continuityState(ctx); st != nil && st.balanced {
		return min(d, openAIBalancedRecoveryWindow)
	}
	return d
}

func prepareOpenAIBalancedForward(ctx context.Context, account *Account, body []byte) ([]byte, error) {
	st := continuityState(ctx)
	if st == nil || !st.balanced || account == nil {
		return body, nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.backupAccountID != account.ID && st.backupBudgetExhausted() {
		return nil, ErrOpenAIRecoveryExhausted
	}
	if !st.migrationPending {
		return body, nil
	}
	if st.migrationUnsafe {
		replay, ok := openAIBalancedReplayBody(body)
		if len(st.checkpointReplay) > 0 {
			// Keep the current turn's root fields and replace only its input.
			restored, err := sjson.SetRawBytes(body, "input", gjson.GetBytes(st.checkpointReplay, "input").Raw)
			if err == nil {
				replay, ok = portableContinuityReplay(restored)
			}
		}
		if !ok {
			return nil, ErrOpenAIReplayIncomplete
		}
		body = replay
	}
	st.backupAccountID = account.ID
	if st.backupAccounts == nil {
		st.backupAccounts = map[int64]struct{}{}
	}
	st.backupAccounts[account.ID] = struct{}{}
	st.checkpointOutput = nil
	logger.FromContext(ctx).Info("openai.session_balanced_replay",
		zap.Int64("account_id", account.ID), zap.Int64("group_id", st.groupID),
		zap.Bool("reasoning_dropped", st.migrationUnsafe))
	return body, nil
}

// Caller holds the state lock. Same-account retries do not consume new slots.
func (st *openAIContinuityState) backupBudgetExhausted() bool {
	limit := st.backupLimit
	if limit <= 0 {
		limit = 3
	}
	return len(st.backupAccounts) >= limit
}
func (s *OpenAIGatewayService) continuityBackupLimit() int {
	if s != nil && s.cfg != nil && s.cfg.Gateway.SessionRecoveryMaxBackupAccounts > 0 {
		return s.cfg.Gateway.SessionRecoveryMaxBackupAccounts
	}
	return 3
}
func (s *OpenAIGatewayService) continuityLeaseWait(ctx context.Context) time.Duration {
	if st := continuityState(ctx); st != nil && st.balanced {
		if s.cfg != nil && s.cfg.Gateway.SessionRecoveryLeaseWaitSeconds > 0 {
			return time.Duration(s.cfg.Gateway.SessionRecoveryLeaseWaitSeconds) * time.Second
		}
		return 30 * time.Second
	}
	return s.continuityRecoveryWindow(ctx)
}
