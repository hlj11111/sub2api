package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

var ErrOpenAIContinuityUnavailable = infraerrors.ServiceUnavailable("SESSION_ACCOUNT_UNAVAILABLE", "原会话渠道当前不可用，请稍后在当前会话重试")
var ErrOpenAIContextIncomplete = infraerrors.Conflict("SESSION_CONTEXT_INCOMPLETE", "本地上下文校验未通过：原渠道不可用，当前历史无法安全跨渠道接续；请稍后重试或补发完整历史")

// The lease token also acts as a fencing version: a late stream completion can
// never overwrite a binding established by a newer request.
type OpenAIContinuityCache interface {
	AcquireContinuityLease(context.Context, int64, string, string, time.Duration) (bool, error)
	RenewContinuityLease(context.Context, int64, string, string, time.Duration) (bool, error)
	CommitContinuityBinding(context.Context, int64, string, string, int64, time.Duration) (bool, error)
	ReleaseContinuityLease(context.Context, int64, string, string) error
	CheckContinuityBinding(context.Context, int64, string, string, int64) (bool, error)
}

// OpenAIContinuityHistory supplies a legacy recovery candidate. It does not
// authorize the account or commit a binding; only a successful turn can do that.
type OpenAIContinuityHistory interface {
	RecoverOpenAIContinuityAccount(context.Context, int64, int64, string) (int64, error)
}

type openAIContinuityKey struct{}
type openAIContinuityState struct {
	mu                   sync.Mutex
	routingHashes        sync.Map // original cache hash -> stable account routing hash
	wsRoutingHash        string   // connection identity for incremental WS turns
	enabled              bool
	explicitSession      bool
	clientSessionID      string // history lookup only; never logged
	retryAccountID       int64  // request-local retry target; never commits a session binding
	migrationUnsafe      bool
	nonPortableReason    string // fixed diagnostic category; never request content
	previousResponseID   string
	waitMigrationClaimed bool
	initialized          bool
	groupID              int64
	hash                 string
	owner                string
	cache                OpenAIContinuityCache
	gateway              *OpenAIGatewayService
	stop                 chan struct{}
	stopped              bool
	handlerDone          chan struct{}
	balanced             bool
	canDropReasoning     bool
	migrationPending     bool
	backupAccountID      int64
	recoveryDeadline     time.Time
}

func continuityState(ctx context.Context) *openAIContinuityState {
	state, _ := ctx.Value(openAIContinuityKey{}).(*openAIContinuityState)
	return state
}
func attachOpenAIContinuity(c *gin.Context) {
	if c == nil || c.Request == nil || continuityState(c.Request.Context()) != nil {
		return
	}
	sessionID := extractClientSessionID(c.Request.Header)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), openAIContinuityKey{}, &openAIContinuityState{
		explicitSession: sessionID != "", clientSessionID: sessionID,
	}))
}
func continuityEnabled(ctx context.Context) bool {
	st := continuityState(ctx)
	return st != nil && st.enabled
}

func (s *OpenAIGatewayService) beginContinuity(ctx context.Context, groupID *int64, hash string) error {
	st := continuityState(ctx)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.enabled = true
	if st.stopped {
		st.initialized = false
		st.stopped = false
	}
	if st.initialized {
		return nil
	}
	hash = scopedOpenAISessionHash(ctx, hash)
	st.groupID, st.hash, st.gateway = derefGroupID(groupID), hash, s
	cache, ok := s.cache.(OpenAIContinuityCache)
	if !ok || hash == "" {
		st.initialized = true
		return nil
	}
	st.cache, st.owner, st.stop = cache, uuid.NewString(), make(chan struct{})
	waitCtx, cancel := context.WithTimeout(ctx, s.continuityRecoveryWindow(ctx))
	defer cancel()
	for {
		acquired, err := cache.AcquireContinuityLease(waitCtx, st.groupID, s.openAISessionCacheKey(hash), st.owner, 30*time.Second)
		if err != nil {
			return continuityLocalStateError("LOCAL_SESSION_STORE_UNAVAILABLE", "本地会话存储暂时不可用，请稍后重试", err)
		}
		if acquired {
			st.initialized = true
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return continuityLocalStateError("LOCAL_SESSION_LEASE_BUSY", "本地会话仍有请求处理中，请稍后重试", waitCtx.Err())
		case <-timer.C:
		}
	}
	owner, group, done := st.owner, st.groupID, st.stop
	lifetimeDone := ctx.Done()
	renewalBase := ctx
	if st.handlerDone != nil {
		lifetimeDone = st.handlerDone
		renewalBase = context.WithoutCancel(ctx)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = cache.ReleaseContinuityLease(cleanup, group, s.openAISessionCacheKey(hash), owner)
		}()
		for {
			select {
			case <-lifetimeDone:
				return
			case <-done:
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(renewalBase, 3*time.Second)
				renewed, err := cache.RenewContinuityLease(renewCtx, group, s.openAISessionCacheKey(hash), owner, 30*time.Second)
				renewCancel()
				if err != nil || !renewed {
					return
				}
			}
		}
	}()
	return nil
}

// CompleteOpenAIContinuity is called only after a successful upstream turn.
// Fenced commits are intentionally not allowed to convert a success into a
// second upstream attempt (the response may already have reached the client).
func CompleteOpenAIContinuity(ctx context.Context, account *Account) {
	st := continuityState(ctx)
	if st == nil || !st.enabled || account == nil || st.hash == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := CheckAccountAccess(ctx, account.ID, nil); err != nil {
		return
	}
	if st.cache != nil {
		ok, err := st.cache.CommitContinuityBinding(ctx, st.groupID, st.gateway.openAISessionCacheKey(st.hash), st.owner, account.ID, st.gateway.openAIWSSessionStickyTTL())
		if err != nil || !ok {
			reason := "lease_mismatch_or_expired"
			if err != nil {
				reason = "binding_store_error"
			}
			logger.FromContext(ctx).Warn("session_binding_commit_rejected",
				zap.Int64("account_id", account.ID), zap.Int64("group_id", st.groupID),
				zap.String("session_hash", st.hash), zap.String("reason", reason), zap.Error(err))
			return
		}
		if !st.stopped {
			close(st.stop)
			st.stopped = true
		}
	} else if st.gateway.cache != nil {
		_ = st.gateway.cache.SetSessionAccountID(ctx, st.groupID, st.gateway.openAISessionCacheKey(st.hash), account.ID, st.gateway.openAIWSSessionStickyTTL())
	}
	slog.Debug("session_binding_committed", "account_id", account.ID, "group_id", st.groupID)
}

func (s *OpenAIGatewayService) selectContinuityAccount(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, bool, error) {
	decision := OpenAIAccountScheduleDecision{}
	if NormalizeOpenAICompatiblePlatform(req.Platform) != PlatformOpenAI || req.RequiredImageCapability != "" || req.RequiredCapability == OpenAIEndpointCapabilityEmbeddings || req.RequiredCapability == OpenAIEndpointCapabilityAlphaSearch || req.RequiredCapability == OpenAIEndpointCapabilityLive {
		return nil, decision, false, nil
	}
	if st := continuityState(ctx); st != nil {
		st.mu.Lock()
		st.previousResponseID = strings.TrimSpace(req.PreviousResponseID)
		st.mu.Unlock()
	}
	if err := s.beginContinuity(ctx, req.GroupID, req.SessionHash); err != nil {
		return nil, decision, true, err
	}
	helper := &defaultOpenAIAccountScheduler{service: s, stats: newOpenAIAccountRuntimeStats()}
	if strings.TrimSpace(req.PreviousResponseID) != "" {
		ownerID := int64(0)
		if store := s.getOpenAIWSStateStore(); store != nil {
			ownerID, _ = store.GetResponseAccount(ctx, derefGroupID(req.GroupID), strings.TrimSpace(req.PreviousResponseID))
		}
		if ownerID > 0 {
			if err := CheckAccountAccess(ctx, ownerID, req.GroupID); err != nil {
				return nil, decision, true, err
			}
			if err := s.waitForContinuityRecovery(ctx, ownerID, req); err != nil {
				return nil, decision, true, err
			}
		}
		selection, err := s.selectAccountByPreviousResponseIDForCapability(ctx, req.GroupID, req.PreviousResponseID, req.RequestedModel, req.ExcludedIDs, req.RequiredCapability, req.RequireCompact)
		if err != nil {
			return nil, decision, true, err
		}
		if selection != nil && selection.Account != nil {
			a := selection.Account
			if CheckAccountAccess(ctx, a.ID, req.GroupID) == nil && s.openAIAccountMatchesSchedulingGroup(a, req.GroupID) && helper.isAccountRequestCompatible(ctx, a, req) && helper.isAccountTransportCompatible(a, req.RequiredTransport) {
				decision.Layer, decision.StickyPreviousHit, decision.SelectedAccountID, decision.SelectedAccountType = openAIAccountScheduleLayerPreviousResponse, true, a.ID, a.Type
				return selection, decision, true, nil
			}
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		}
		// A boolean tool-coverage hint cannot establish completeness of ordinary history.
		if ownerID > 0 {
			return nil, decision, true, ErrOpenAIContinuityUnavailable
		}
		return nil, decision, true, openAIContextIncomplete(ctx, "previous_response_owner_missing", 0)
	}
	if req.SessionHash == "" {
		if st := continuityState(ctx); st != nil && st.migrationUnsafe {
			return nil, decision, true, openAIContextIncomplete(ctx, "session_identity_missing", 0)
		}
		return nil, decision, false, nil
	}
	var accountID int64
	if st := continuityState(ctx); st != nil {
		st.mu.Lock()
		accountID, st.retryAccountID = st.retryAccountID, 0
		st.mu.Unlock()
	}
	var err error
	if accountID == 0 {
		accountID, err = s.getStickySessionAccountID(ctx, req.GroupID, req.SessionHash)
	}
	if err != nil && !errors.Is(err, ErrStickySessionNotFound) {
		return nil, decision, true, continuityLocalStateError("LOCAL_SESSION_STORE_UNAVAILABLE", "本地会话归属查询失败，请稍后重试", err)
	}
	if accountID <= 0 {
		if st := continuityState(ctx); st != nil && st.migrationUnsafe && !allowOpenAIBalancedMigration(ctx) {
			return nil, decision, true, openAIContextIncomplete(ctx, "session_binding_missing", 0)
		}
		return nil, decision, false, nil
	}
	if err := CheckAccountAccess(ctx, accountID, req.GroupID); err != nil {
		if !errors.Is(err, ErrAccountAccessDenied) || (continuityState(ctx).migrationUnsafe && !allowOpenAIBalancedMigration(ctx)) {
			return nil, decision, true, err
		}
		if st := continuityState(ctx); st != nil && st.balanced && !allowOpenAIBalancedMigration(ctx) {
			return nil, decision, true, ErrOpenAIRecoveryExhausted
		}
		// A portable request may migrate after revocation, but only through the
		// normal picker, which applies the latest policy to every candidate.
		return nil, decision, false, nil
	}
	if err := s.waitForContinuityRecovery(ctx, accountID, req); err != nil {
		return nil, decision, true, err
	}
	req.StickyAccountID, req.PreserveStickyBinding, req.DisableStickyEscape = accountID, true, true
	selection, _, err := helper.selectBySessionHash(ctx, req)
	if err != nil {
		return nil, decision, true, err
	}
	if selection != nil && selection.Account != nil {
		if selection.WaitPlan != nil && continuityState(ctx).balanced {
			selection.WaitPlan.Timeout = min(selection.WaitPlan.Timeout, s.continuityRecoveryWindow(ctx))
		}
		decision.Layer, decision.StickySessionHit, decision.SelectedAccountID, decision.SelectedAccountType = openAIAccountScheduleLayerSessionSticky, true, selection.Account.ID, selection.Account.Type
		slog.Debug("session_binding_hit", "account_id", accountID, "group_id", derefGroupID(req.GroupID), "waiting", selection.WaitPlan != nil)
		return selection, decision, true, nil
	}
	if st := continuityState(ctx); st != nil && st.migrationUnsafe && !allowOpenAIBalancedMigration(ctx) {
		return nil, decision, true, openAIContextIncomplete(ctx, "bound_account_not_selectable", accountID)
	}
	if st := continuityState(ctx); st != nil && st.balanced && !allowOpenAIBalancedMigration(ctx) {
		return nil, decision, true, ErrOpenAIRecoveryExhausted
	}
	// No upstream history reference: the protocol's request carries its context.
	// The old binding is kept until a replacement turn succeeds.
	slog.Debug("session_safe_migration_candidate", "group_id", derefGroupID(req.GroupID), "account_id", accountID)
	return nil, decision, false, nil
}

// Short cooldowns should not break an otherwise usable session. Use the same
// bounded wait budget as capacity waiting; disabled/revoked/incompatible accounts
// never enter this loop. Recheck the policy on every wakeup.
func (s *OpenAIGatewayService) waitForContinuityRecovery(ctx context.Context, accountID int64, req OpenAIAccountScheduleRequest) error {
	if _, excluded := req.ExcludedIDs[accountID]; excluded {
		return nil
	}
	deadline := time.Now().Add(s.continuityRecoveryWindow(ctx))
	logged := false
	for {
		if err := CheckAccountAccess(ctx, accountID, req.GroupID); err != nil {
			return err
		}
		a, err := s.getSchedulableAccount(ctx, accountID)
		if err != nil || a == nil {
			return nil
		}
		if a.IsSchedulable() {
			return nil
		}
		candidate := *a
		candidate.RateLimitResetAt, candidate.OverloadUntil, candidate.TempUnschedulableUntil = nil, nil, nil
		if !candidate.IsSchedulable() || !candidate.IsModelSupported(req.RequestedModel) || !s.openAIAccountMatchesSchedulingGroup(a, req.GroupID) {
			return nil
		}
		if st := continuityState(ctx); st != nil && st.balanced {
			// A reset after this request's recovery budget cannot be helped by waiting.
			for _, until := range []*time.Time{a.RateLimitResetAt, a.OverloadUntil, a.TempUnschedulableUntil} {
				if until != nil && until.After(deadline) {
					return nil
				}
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		if !logged {
			slog.Debug("session_binding_wait", "account_id", accountID, "group_id", derefGroupID(req.GroupID), "reason", "temporary_cooldown")
			logged = true
		}
		pause := min(remaining, 200*time.Millisecond)
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ErrOpenAIContinuityUnavailable.WithCause(ctx.Err())
		case <-timer.C:
		}
	}
}

func scopedOpenAISessionHash(ctx context.Context, hash string) string {
	if st := continuityState(ctx); st != nil {
		if routing, ok := st.routingHashes.Load(hash); ok {
			if routingHash, valid := routing.(string); valid {
				hash = routingHash
			}
		}
	}
	userID, _ := ctx.Value(ctxkey.UserID).(int64)
	if userID <= 0 || hash == "" {
		return hash
	}
	return fmt.Sprintf("u%d:%s", userID, hash)
}

func checkOpenAIContinuityBeforeForward(ctx context.Context, account *Account) error {
	st := continuityState(ctx)
	if st == nil || !st.enabled || st.cache == nil || account == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ok, err := st.cache.CheckContinuityBinding(ctx, st.groupID, st.gateway.openAISessionCacheKey(st.hash), st.owner, account.ID)
	if err != nil || !ok {
		return continuityLocalStateError("LOCAL_SESSION_LEASE_LOST", "本地会话占用已失效，请重试当前会话", err)
	}
	return nil
}

// openAIContextIncomplete records the rejecting branch at WARN so it is visible
// with production INFO logging. Only hashes, booleans and fixed categories are
// logged, never opaque IDs or request content. The logged code matches the
// detailed client-facing classification while retaining the base error cause.
func openAIContextIncomplete(ctx context.Context, reason string, accountID int64) error {
	stateReason := ""
	fields := []zap.Field{zap.String("reason", reason)}
	if accountID > 0 {
		fields = append(fields, zap.Int64("account_id", accountID))
	}
	if st := continuityState(ctx); st != nil {
		st.mu.Lock()
		stateReason = st.nonPortableReason
		fields = append(fields,
			zap.Int64("group_id", st.groupID),
			zap.String("session_hash", st.hash),
			zap.Bool("previous_response_present", st.previousResponseID != ""),
			zap.Bool("migration_unsafe", st.migrationUnsafe),
			zap.String("non_portable_reason", st.nonPortableReason),
		)
		st.mu.Unlock()
	}
	err := continuityContextError(reason, stateReason)
	fields = append(fields, zap.String("error_code", infraerrors.FromError(err).Reason))
	logger.FromContext(ctx).Warn("openai.session_context_incomplete", fields...)
	return err
}

func openAIRequestHasNonPortableState(body []byte) bool {
	return openAIRequestNonPortableReason(body) != ""
}

// External item references and encrypted state are not portable between accounts.
// Return the first detected category without retaining any request content.
// The mere presence of tool call pairs cannot prove ordinary history completeness.
func openAIRequestNonPortableReason(body []byte) string {
	body = []byte(openAIRequestPayloadView(body).Raw)
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("type").String() == "compaction" {
			return "compaction"
		}
	}
	coverage := AnalyzeToolCallOutputContextCoverageBytes(body)
	if coverage.HasFunctionCallOutput && !coverage.ContextCoversAllCallIDs {
		return "unmatched_tool_output"
	}
	if v := gjson.GetBytes(body, "conversation"); v.Exists() && v.Type != gjson.Null && v.String() != "" {
		return "conversation_reference"
	}
	reason := ""
	var checkReferences func(gjson.Result)
	checkReferences = func(value gjson.Result) {
		if reason != "" || (!value.IsObject() && !value.IsArray()) {
			return
		}
		value.ForEach(func(key, v gjson.Result) bool {
			switch key.String() {
			case "file_id", "file_ids", "vector_store_ids", "encrypted_content":
				if v.Exists() && v.Type != gjson.Null && v.String() != "" && v.Raw != "[]" {
					reason = key.String()
				}
			case "container":
				if v.Type == gjson.String && v.String() != "auto" && v.String() != "" {
					reason = "container_reference"
				}
			}
			if reason == "" {
				checkReferences(v)
			}
			return reason == ""
		})
	}
	checkReferences(gjson.GetBytes(body, "input"))
	gjson.GetBytes(body, "tools").ForEach(func(_, tool gjson.Result) bool {
		kind := tool.Get("type").String()
		if kind == "file_search" || kind == "code_interpreter" {
			checkReferences(tool)
		}
		return reason == ""
	})
	gjson.GetBytes(body, "input").ForEach(func(_, item gjson.Result) bool {
		if reason != "" {
			return false
		}
		if item.Get("type").String() == "item_reference" {
			reason = "item_reference"
			return false
		}
		if item.Get("encrypted_content").String() != "" {
			reason = "encrypted_content"
			return false
		}
		return true
	})
	return reason
}

// ClaimOpenAIContinuityWaitMigration permits one bounded reselection after a
// capacity wait times out, before forwarding any part of the turn. Historical
// IDs and account-scoped references require proven replay and cannot use this path.
func ClaimOpenAIContinuityWaitMigration(ctx context.Context) bool {
	st := continuityState(ctx)
	if st == nil || ctx.Err() != nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.enabled || st.previousResponseID != "" || st.waitMigrationClaimed || (st.migrationUnsafe && !st.allowBalancedMigration()) || (st.balanced && st.backupAccountID != 0) {
		return false
	}
	st.waitMigrationClaimed = true
	if st.balanced {
		st.migrationPending = true
	}
	return true
}

// AcquireOpenAIWebSocketAccountSlot uses the same queue and timeout as HTTP
// sticky admission. Unlike the HTTP helper it never writes SSE bytes to a WS.
func (s *OpenAIGatewayService) AcquireOpenAIWebSocketAccountSlot(ctx context.Context, accountID int64, maxConcurrency int, plan *AccountWaitPlan) (func(), bool, error) {
	if err := CheckAccountAccess(ctx, accountID, nil); err != nil {
		return nil, false, err
	}
	result, err := s.tryAcquireAccountSlot(ctx, accountID, maxConcurrency)
	if err != nil {
		return nil, false, err
	}
	if result != nil && result.Acquired {
		return result.ReleaseFunc, true, nil
	}
	if !continuityEnabled(ctx) || s.concurrencyService == nil {
		return nil, false, nil
	}
	cfg := s.schedulingConfig()
	timeout, maxWaiting := cfg.StickySessionWaitTimeout, cfg.StickySessionMaxWaiting
	if plan != nil {
		timeout, maxWaiting = plan.Timeout, plan.MaxWaiting
	}
	canWait, err := s.concurrencyService.IncrementAccountWaitCount(ctx, accountID, maxWaiting)
	if err != nil {
		return nil, false, err
	}
	if !canWait {
		return nil, false, ErrOpenAIContinuityUnavailable
	}
	defer s.concurrencyService.DecrementAccountWaitCount(ctx, accountID)
	slog.Debug("session_binding_wait", "account_id", accountID, "reason", "concurrency_full")
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return nil, false, ErrOpenAIContinuityUnavailable.WithCause(waitCtx.Err())
		case <-ticker.C:
			if err := CheckAccountAccess(waitCtx, accountID, nil); err != nil {
				return nil, false, err
			}
			result, err := s.tryAcquireAccountSlot(waitCtx, accountID, maxConcurrency)
			if err != nil {
				return nil, false, err
			}
			if result != nil && result.Acquired {
				return result.ReleaseFunc, true, nil
			}
		}
	}
}

// PrepareOpenAIContinuityRecovery adds a small retry budget for transient upstream
// failures in an established request context. Explicit pool settings and typed
// failure policies retain precedence; authentication/payload errors never opt in.
func PrepareOpenAIContinuityRecovery(ctx context.Context, account *Account, failure *UpstreamFailoverError) {
	st := continuityState(ctx)
	if st != nil && st.balanced && failure != nil && account != nil {
		st.mu.Lock()
		if st.recoveryDeadline.IsZero() {
			st.recoveryDeadline = time.Now().Add(openAIBalancedRecoveryWindow)
		}
		if failure.SameAccountRetryDeadline.IsZero() || failure.SameAccountRetryDeadline.After(st.recoveryDeadline) {
			failure.SameAccountRetryDeadline = st.recoveryDeadline
		}
		// A deadline alone opts the shared retry helper out of its count limit.
		// Keep a hard cap as well, including explicit zero-account retry policy
		// (the handler's effective limit remains zero in that case).
		if failure.SameAccountRetryMax <= 0 {
			failure.SameAccountRetryMax = max(1, account.GetPoolModeRetryCount())
		}
		st.mu.Unlock()
	}
	if st == nil || !st.enabled || !st.explicitSession || st.hash == "" || account == nil || account.Platform != PlatformOpenAI || account.IsPoolMode() || failure == nil ||
		failure.RetryableOnSameAccount || !failure.ShouldRetryNextAccount() || failure.IsCredentialFailure() || failure.Reason != "" {
		return
	}
	switch failure.StatusCode {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		failure.RetryableOnSameAccount = true
		failure.RequestScopedTransient = true
		failure.SameAccountRetryMax = 2
	}
}

// RetryOpenAIContinuityAccount keeps a retry on the attempted account even before
// its first successful turn has committed a binding. Selection still rechecks
// current permissions, health, model compatibility and concurrency limits.
func RetryOpenAIContinuityAccount(ctx context.Context, accountID int64) {
	if st := continuityState(ctx); st != nil && st.enabled {
		st.mu.Lock()
		st.retryAccountID = accountID
		st.mu.Unlock()
	}
}

// OpenAIContinuityMigrationError is checked after original-account retries and
// before excluding that account. Never discard opaque history to force a replay.
func OpenAIContinuityMigrationError(ctx context.Context) error {
	if st := continuityState(ctx); st != nil && st.enabled {
		st.mu.Lock()
		previousResponseID, migrationUnsafe := st.previousResponseID, st.migrationUnsafe
		balanced, backup := st.balanced, st.backupAccountID
		allowed := st.allowBalancedMigration()
		st.mu.Unlock()
		if balanced && backup != 0 {
			return ErrOpenAIRecoveryExhausted
		}
		if allowed {
			return nil
		}
		if previousResponseID != "" {
			return ErrOpenAIContinuityUnavailable
		}
		if migrationUnsafe {
			return openAIContextIncomplete(ctx, "failover_requires_non_portable_history", 0)
		}
	}
	return nil
}
