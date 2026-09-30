package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// The repository stores only ciphertext. A successful binding and its checkpoint
// are committed in one fenced transaction, before another turn can take the lease.
type OpenAIContinuityCheckpointStore interface {
	GetContinuityCheckpoint(context.Context, int64, string) ([]byte, error)
	CommitContinuityCheckpoint(context.Context, int64, string, string, int64, time.Duration, []byte, time.Duration) (bool, error)
}

const continuityCheckpointMaxBytes = 4 << 20
const continuityCheckpointRetention = 24 * time.Hour

type continuityCheckpoint struct {
	Version int `json:"version"`
	// SHA-256 of an exact compaction ciphertext -> visible history preceding it.
	// No guessed timestamp/session match and no opaque compaction is discarded.
	Prefixes map[string][]json.RawMessage `json:"prefixes"`
	Order    []string                     `json:"order"`
	// Standalone /compact returns a complete window including retained messages.
	// Match that whole window before replacing it; compaction itself is represented
	// by a placeholder so its opaque ciphertext is not persisted here.
	Windows map[string][]json.RawMessage `json:"windows,omitempty"`
}

func (s *OpenAIGatewayService) continuityCheckpointCipher() (cipher.AEAD, error) {
	if s == nil || s.cfg == nil || s.cfg.Gateway.DisableSessionRecoveryCheckpoints {
		return nil, nil
	}
	// Auto-generated process-local keys cannot recover checkpoints after restart.
	if !s.cfg.Totp.EncryptionKeyConfigured {
		return nil, nil
	}
	key, err := hex.DecodeString(s.cfg.Totp.EncryptionKey)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid session checkpoint encryption key")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("sub2api/openai-session-checkpoint/v1"))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func sealContinuityCheckpoint(aead cipher.AEAD, scope string, checkpoint *continuityCheckpoint) ([]byte, error) {
	plain, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	if len(plain) > continuityCheckpointMaxBytes {
		return nil, fmt.Errorf("checkpoint size limit")
	}
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err = z.Write(plain); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, compressed.Bytes(), []byte(scope)), nil
}

func openContinuityCheckpoint(aead cipher.AEAD, scope string, sealed []byte) (*continuityCheckpoint, error) {
	if len(sealed) < aead.NonceSize()+aead.Overhead() || len(sealed) > continuityCheckpointMaxBytes+1024 {
		return nil, fmt.Errorf("invalid checkpoint size")
	}
	compressed, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], []byte(scope))
	if err != nil {
		return nil, fmt.Errorf("checkpoint authentication failed")
	}
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("invalid checkpoint compression")
	}
	defer func() { _ = z.Close() }()
	plain, err := io.ReadAll(io.LimitReader(z, continuityCheckpointMaxBytes+1))
	if err != nil || len(plain) > continuityCheckpointMaxBytes {
		return nil, fmt.Errorf("invalid checkpoint payload size")
	}
	var result continuityCheckpoint
	if err := json.Unmarshal(plain, &result); err != nil || result.Version != 1 || len(result.Prefixes) > 4 || len(result.Windows) > 4 {
		return nil, fmt.Errorf("invalid checkpoint format")
	}
	return &result, nil
}

func checkpointScope(group int64, hash string) string { return fmt.Sprintf("v1:%d:%s", group, hash) }
func compactionDigest(item gjson.Result) string {
	value := item.Get("encrypted_content").String()
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// Retain user-visible messages and tool operations. Reject references/unknown
// items; they cannot be made portable merely by stripping their encryption.
func visibleContinuityItems(input gjson.Result) ([]json.RawMessage, bool) {
	if input.Type == gjson.String {
		raw, err := json.Marshal(map[string]string{"role": "user", "content": input.String()})
		return []json.RawMessage{raw}, err == nil && strings.TrimSpace(input.String()) != ""
	}
	if !input.IsArray() {
		return nil, false
	}
	var out []json.RawMessage
	for _, item := range input.Array() {
		switch item.Get("type").String() {
		case "reasoning", "compaction_trigger":
			continue
		case "", "message":
			role := item.Get("role").String()
			if role != "user" && role != "assistant" && role != "system" && role != "developer" {
				return nil, false
			}
			if !item.Get("content").Exists() {
				return nil, false
			}
		default:
			if !isCodexToolCallContextItemType(item.Get("type").String()) && !isCodexToolCallOutputItemType(item.Get("type").String()) {
				return nil, false
			}
		}
		raw := json.RawMessage(item.Raw)
		envelope, _ := json.Marshal(map[string][]json.RawMessage{"input": {raw}})
		// Tool pairing is validated on the assembled transcript, not one item.
		reason := openAIRequestNonPortableReason(envelope)
		if reason != "" && reason != "unmatched_tool_output" {
			return nil, false
		}
		out = append(out, raw)
	}
	return out, true
}

func restoreContinuityCompaction(body []byte, checkpoint *continuityCheckpoint) ([]byte, bool) {
	if checkpoint == nil || !gjson.GetBytes(body, "input").IsArray() {
		return nil, false
	}
	var input []json.RawMessage
	restored := false
	items := gjson.GetBytes(body, "input").Array()
	for i := 0; i < len(items); i++ {
		item := items[i]
		if item.Get("type").String() == "compaction" {
			// Multiple independent compactions have ambiguous coverage. Fail closed.
			if restored {
				return nil, false
			}
			digest := compactionDigest(item)
			prefix, exists := checkpoint.Prefixes[digest]
			if !exists || len(prefix) == 0 {
				return nil, false
			}
			leadingCount := len(input)
			if window, standalone := checkpoint.Windows[digest]; standalone {
				start, end, matched := matchContinuityWindow(items, i, window)
				if !matched || start > len(input) {
					return nil, false
				}
				leadingCount = start
				i = end - 1
			} else if continuityVisiblePrefixMatches(input, prefix) {
				// Input-array chaining may retain the exact pre-compaction history.
				// Replace that prefix together with its compaction, without duplicates.
				leadingCount = 0
			}
			// Current leading system/developer messages remain authoritative.
			input = input[:leadingCount]
			for _, leading := range input {
				role := gjson.GetBytes(leading, "role").String()
				if role != "system" && role != "developer" {
					return nil, false
				}
			}
			if len(input) > 0 {
				for len(prefix) > 0 {
					role := gjson.GetBytes(prefix[0], "role").String()
					if role != "system" && role != "developer" {
						break
					}
					prefix = prefix[1:]
				}
			}
			input = append(input, prefix...)
			restored = true
		} else {
			input = append(input, json.RawMessage(item.Raw))
		}
	}
	if !restored {
		return nil, false
	}
	encoded, err := json.Marshal(input)
	if err != nil || len(encoded) > continuityCheckpointMaxBytes {
		return nil, false
	}
	replay, err := sjson.SetRawBytes(body, "input", encoded)
	if err != nil {
		return nil, false
	}
	return replay, true
}

func continuityVisiblePrefixMatches(input, prefix []json.RawMessage) bool {
	encoded, err := json.Marshal(input)
	if err != nil {
		return false
	}
	visible, ok := visibleContinuityItems(gjson.ParseBytes(encoded))
	if !ok || len(visible) != len(prefix) {
		return false
	}
	for i := range visible {
		if !continuityItemsEqual(visible[i], prefix[i]) {
			return false
		}
	}
	return true
}

func matchContinuityWindow(items []gjson.Result, compactionIndex int, window []json.RawMessage) (int, int, bool) {
	compactionOffset := -1
	for i, raw := range window {
		if gjson.GetBytes(raw, "type").String() == "compaction" {
			if compactionOffset >= 0 {
				return 0, 0, false
			}
			compactionOffset = i
		}
	}
	start := compactionIndex - compactionOffset
	if compactionOffset < 0 || start < 0 || start+len(window) > len(items) {
		return 0, 0, false
	}
	for i, raw := range window {
		if i == compactionOffset {
			continue // exact ciphertext digest was already used to find the window
		}
		if !continuityItemsEqual(raw, []byte(items[start+i].Raw)) {
			return 0, 0, false
		}
	}
	return start, start + len(window), true
}

func continuityItemsEqual(a, b []byte) bool {
	canonical := func(raw []byte) []byte {
		var item any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&item) != nil {
			return nil
		}
		encoded, _ := json.Marshal(item)
		return encoded
	}
	left, right := canonical(a), canonical(b)
	return left != nil && right != nil && bytes.Equal(left, right)
}

// Called once after acquiring the request lease. Original ingress stays untouched
// for its original account; reconstructed history is used only for migration.
func (s *OpenAIGatewayService) loadContinuityCheckpoint(ctx context.Context) {
	st := continuityState(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	if st.checkpointLoaded {
		st.mu.Unlock()
		return
	}
	st.checkpointLoaded = true
	body := append([]byte(nil), st.ingressBody...)
	hash, group := st.hash, st.groupID
	st.mu.Unlock()
	aead, err := s.continuityCheckpointCipher()
	store, ok := s.cache.(OpenAIContinuityCheckpointStore)
	if err != nil || aead == nil || !ok || hash == "" {
		return
	}
	sealed, err := store.GetContinuityCheckpoint(ctx, group, s.openAISessionCacheKey(hash))
	if err != nil {
		logger.FromContext(ctx).Warn("openai.session_checkpoint_read_failed", zap.Int64("group_id", group))
		return
	}
	checkpoint := &continuityCheckpoint{Version: 1, Prefixes: map[string][]json.RawMessage{}}
	if len(sealed) > 0 {
		checkpoint, err = openContinuityCheckpoint(aead, checkpointScope(group, hash), sealed)
		if err != nil {
			logger.FromContext(ctx).Warn("openai.session_checkpoint_invalid", zap.Int64("group_id", group))
			return
		}
	}
	replay, restored := restoreContinuityCompaction(body, checkpoint)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.checkpoint = checkpoint
	if restored {
		// Auxiliary reasoning may also appear after the compaction; strip only that
		// reasoning and validate every visible message/tool pair on the whole replay.
		if portable, valid := portableContinuityReplay(replay); valid {
			st.checkpointReplay = portable
			st.canDropReasoning = true
		}
	}
}

func portableContinuityReplay(body []byte) ([]byte, bool) {
	items, ok := visibleContinuityItems(gjson.GetBytes(body, "input"))
	if !ok || len(items) == 0 {
		return nil, false
	}
	// Keep a current-turn native compaction instruction when replaying history.
	if HasCompactionTriggerInInput(body) {
		items = append(items, json.RawMessage(`{"type":"compaction_trigger"}`))
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return nil, false
	}
	replay, err := sjson.SetRawBytes(body, "input", encoded)
	if err != nil || openAIRequestNonPortableReason(replay) != "" || !openAIBalancedToolHistoryComplete(replay) {
		return nil, false
	}
	seenUser := false
	for _, item := range items {
		role := gjson.GetBytes(item, "role").String()
		if role == "user" {
			seenUser = true
		}
		if !seenUser && (role == "assistant" || isCodexToolCallContextItemType(gjson.GetBytes(item, "type").String())) {
			return nil, false
		}
	}
	return replay, seenUser
}

// Only client-visible completed Responses output is eligible. The forwarder's
// success check remains authoritative; failed/incomplete streams never save.
func captureContinuityCheckpointOutput(ctx context.Context, response []byte) {
	st := continuityState(ctx)
	if st == nil {
		return
	}
	status := gjson.GetBytes(response, "status").String()
	if status == "failed" || status == "incomplete" || status == "cancelled" {
		return
	}
	output := gjson.GetBytes(response, "output")
	if !output.IsArray() || len(output.Raw) > continuityCheckpointMaxBytes {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.ingressBody) == 0 {
		return
	}
	st.checkpointOutput = append([]byte(nil), output.Raw...)
}

// Caller holds st.mu. Match compaction coverage to the exact visible prefix.
func (st *openAIContinuityState) sealedCheckpoint() ([]byte, error) {
	if st.gateway == nil || len(st.checkpointOutput) == 0 || len(st.ingressBody) == 0 {
		return nil, nil
	}
	aead, err := st.gateway.continuityCheckpointCipher()
	if err != nil || aead == nil {
		return nil, err
	}
	body := st.ingressBody
	if recovered, ok := restoreContinuityCompaction(body, st.checkpoint); ok {
		body = recovered
	}
	if gjson.GetBytes(body, "previous_response_id").String() != "" || gjson.GetBytes(body, "conversation").String() != "" {
		return nil, nil
	}
	prefix, ok := visibleContinuityItems(gjson.GetBytes(body, "input"))
	if !ok || len(prefix) == 0 {
		return nil, nil
	}
	// Require an actual beginning; never treat the suffix after an unknown
	// compaction as a complete checkpoint.
	seenUser := false
	for _, raw := range prefix {
		if gjson.GetBytes(raw, "role").String() == "user" {
			seenUser = true
			break
		}
	}
	if !seenUser {
		return nil, nil
	}
	cp := st.checkpoint
	if cp == nil {
		cp = &continuityCheckpoint{Version: 1, Prefixes: map[string][]json.RawMessage{}}
	}
	if cp.Prefixes == nil {
		cp.Prefixes = map[string][]json.RawMessage{}
	}
	if st.checkpointStandalone {
		var window []json.RawMessage
		digest := ""
		for _, item := range gjson.ParseBytes(st.checkpointOutput).Array() {
			if item.Get("type").String() == "compaction" {
				if digest != "" || compactionDigest(item) == "" {
					return nil, nil
				}
				digest = compactionDigest(item)
				window = append(window, json.RawMessage(`{"type":"compaction"}`))
			} else {
				visible, valid := visibleContinuityItems(gjson.Parse("[" + item.Raw + "]"))
				if !valid || len(visible) != 1 {
					return nil, nil
				}
				window = append(window, visible[0])
			}
		}
		if digest == "" {
			return nil, nil
		}
		if cp.Windows == nil {
			cp.Windows = map[string][]json.RawMessage{}
		}
		if _, exists := cp.Prefixes[digest]; !exists {
			cp.Order = append(cp.Order, digest)
		}
		cp.Prefixes[digest] = append([]json.RawMessage(nil), prefix...)
		cp.Windows[digest] = window
	} else {
		for _, item := range gjson.ParseBytes(st.checkpointOutput).Array() {
			if item.Get("type").String() == "compaction" {
				digest := compactionDigest(item)
				if digest == "" {
					return nil, nil
				}
				if _, exists := cp.Prefixes[digest]; !exists {
					cp.Order = append(cp.Order, digest)
				}
				cp.Prefixes[digest] = append([]json.RawMessage(nil), prefix...)
				delete(cp.Windows, digest)
			} else {
				raw, valid := visibleContinuityItems(gjson.Parse("[" + item.Raw + "]"))
				if !valid {
					return nil, nil
				}
				prefix = append(prefix, raw...)
			}
		}
	}
	for len(cp.Order) > 4 {
		delete(cp.Prefixes, cp.Order[0])
		delete(cp.Windows, cp.Order[0])
		cp.Order = cp.Order[1:]
	}
	if len(cp.Prefixes) == 0 {
		return nil, nil
	}
	return sealContinuityCheckpoint(aead, checkpointScope(st.groupID, st.hash), cp)
}
