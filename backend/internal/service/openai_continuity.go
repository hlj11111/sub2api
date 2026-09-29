package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

var ErrOpenAIContinuityUnavailable = infraerrors.ServiceUnavailable("SESSION_ACCOUNT_UNAVAILABLE", "The session account is temporarily unavailable; retry this conversation later")
var ErrOpenAIContextIncomplete = infraerrors.Conflict("SESSION_CONTEXT_INCOMPLETE", "Cannot safely continue this conversation: its previous response is unavailable and complete replay history is not available")

// The lease token also acts as a fencing version: a late stream completion can
// never overwrite a binding established by a newer request.
type OpenAIContinuityCache interface {
	AcquireContinuityLease(context.Context, int64, string, string, time.Duration) (bool, error)
	RenewContinuityLease(context.Context, int64, string, string, time.Duration) (bool, error)
	CommitContinuityBinding(context.Context, int64, string, string, int64, time.Duration) (bool, error)
	ReleaseContinuityLease(context.Context, int64, string, string) error
	CheckContinuityBinding(context.Context, int64, string, string, int64) (bool, error)
}
type openAIContinuityKey struct{}
type openAIContinuityState struct {
	mu                   sync.Mutex
	routingHashes        sync.Map // original cache hash -> stable account routing hash
	wsRoutingHash        string   // connection identity for incremental WS turns
	enabled              bool
	migrationUnsafe      bool
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
}

func continuityState(ctx context.Context) *openAIContinuityState {
	state, _ := ctx.Value(openAIContinuityKey{}).(*openAIContinuityState)
	return state
}
func attachOpenAIContinuity(c *gin.Context) {
	if c == nil || c.Request == nil || continuityState(c.Request.Context()) != nil {
		return
	}
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), openAIContinuityKey{}, &openAIContinuityState{}))
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
	waitCtx, cancel := context.WithTimeout(ctx, s.schedulingConfig().StickySessionWaitTimeout)
	defer cancel()
	for {
		acquired, err := cache.AcquireContinuityLease(waitCtx, st.groupID, s.openAISessionCacheKey(hash), st.owner, 30*time.Second)
		if err != nil {
			return ErrOpenAIContinuityUnavailable.WithCause(err)
		}
		if acquired {
			st.initialized = true
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return ErrOpenAIContinuityUnavailable
		case <-timer.C:
		}
	}
	owner, group, done := st.owner, st.groupID, st.stop
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
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				renewed, err := cache.RenewContinuityLease(ctx, group, s.openAISessionCacheKey(hash), owner, 30*time.Second)
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
			slog.Warn("session_binding_commit_rejected", "account_id", account.ID, "group_id", st.groupID, "error", err)
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
		return nil, decision, true, ErrOpenAIContextIncomplete
	}
	if req.SessionHash == "" {
		if st := continuityState(ctx); st != nil && st.migrationUnsafe {
			return nil, decision, true, ErrOpenAIContextIncomplete
		}
		return nil, decision, false, nil
	}
	accountID, err := s.getStickySessionAccountID(ctx, req.GroupID, req.SessionHash)
	if err != nil && !errors.Is(err, ErrStickySessionNotFound) {
		return nil, decision, true, ErrOpenAIContinuityUnavailable.WithCause(err)
	}
	if accountID <= 0 {
		if st := continuityState(ctx); st != nil && st.migrationUnsafe {
			return nil, decision, true, ErrOpenAIContextIncomplete
		}
		return nil, decision, false, nil
	}
	if err := CheckAccountAccess(ctx, accountID, req.GroupID); err != nil {
		if !errors.Is(err, ErrAccountAccessDenied) || continuityState(ctx).migrationUnsafe {
			return nil, decision, true, err
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
		decision.Layer, decision.StickySessionHit, decision.SelectedAccountID, decision.SelectedAccountType = openAIAccountScheduleLayerSessionSticky, true, selection.Account.ID, selection.Account.Type
		slog.Debug("session_binding_hit", "account_id", accountID, "group_id", derefGroupID(req.GroupID), "waiting", selection.WaitPlan != nil)
		return selection, decision, true, nil
	}
	if st := continuityState(ctx); st != nil && st.migrationUnsafe {
		return nil, decision, true, ErrOpenAIContextIncomplete
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
	deadline := time.Now().Add(s.schedulingConfig().StickySessionWaitTimeout)
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
			hash = routing.(string)
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
		return ErrOpenAIContinuityUnavailable
	}
	return nil
}

// External item references and encrypted state are not portable between accounts.
// The mere presence of tool call pairs cannot prove ordinary history completeness.
func openAIRequestHasNonPortableState(body []byte) bool {
	body = []byte(openAIRequestPayloadView(body).Raw)
	coverage := AnalyzeToolCallOutputContextCoverageBytes(body)
	if coverage.HasFunctionCallOutput && !coverage.ContextCoversAllCallIDs {
		return true
	}
	if v := gjson.GetBytes(body, "conversation"); v.Exists() && v.Type != gjson.Null && v.String() != "" {
		return true
	}
	unsafe := false
	var checkReferences func(gjson.Result)
	checkReferences = func(value gjson.Result) {
		if !value.IsObject() && !value.IsArray() {
			return
		}
		value.ForEach(func(key, v gjson.Result) bool {
			switch key.String() {
			case "file_id", "file_ids", "vector_store_ids", "encrypted_content":
				if v.Exists() && v.Type != gjson.Null && v.String() != "" && v.Raw != "[]" {
					unsafe = true
				}
			case "container":
				if v.Type == gjson.String && v.String() != "auto" && v.String() != "" {
					unsafe = true
				}
			}
			if !unsafe {
				checkReferences(v)
			}
			return !unsafe
		})
	}
	checkReferences(gjson.GetBytes(body, "input"))
	gjson.GetBytes(body, "tools").ForEach(func(_, tool gjson.Result) bool {
		kind := tool.Get("type").String()
		if kind == "file_search" || kind == "code_interpreter" {
			checkReferences(tool)
		}
		return !unsafe
	})
	gjson.GetBytes(body, "input").ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() == "item_reference" || item.Get("encrypted_content").String() != "" {
			unsafe = true
			return false
		}
		return true
	})
	return unsafe
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
	if !st.enabled || st.migrationUnsafe || st.previousResponseID != "" || st.waitMigrationClaimed {
		return false
	}
	st.waitMigrationClaimed = true
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
