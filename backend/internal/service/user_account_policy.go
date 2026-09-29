package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var ErrAccountAccessDenied = infraerrors.Forbidden("ACCOUNT_ACCESS_DENIED", "The account is not permitted for this user and group")
var ErrAccountPolicyUnavailable = infraerrors.ServiceUnavailable("ACCOUNT_POLICY_UNAVAILABLE", "Unable to verify account access")
var ErrAllowedAccountsUnavailable = infraerrors.ServiceUnavailable("ALLOWED_ACCOUNTS_UNAVAILABLE", "No permitted accounts are currently available for this user and group")

func IsAccountPolicyError(err error) bool {
	return errors.Is(err, ErrAccountAccessDenied) || errors.Is(err, ErrAccountPolicyUnavailable) || errors.Is(err, ErrAllowedAccountsUnavailable)
}

type UserAccountPolicy struct {
	Mode       string  `json:"mode"`
	AccountIDs []int64 `json:"account_ids"`
}

type UserAccountPolicyRepository interface {
	Get(context.Context, int64, int64) (*UserAccountPolicy, error)
	Set(context.Context, int64, int64, UserAccountPolicy) error
	// Allowed checks current policy and current group membership in one database snapshot.
	Allowed(context.Context, int64, []int64, int64) (bool, error)
	Filter(context.Context, int64, []int64, []int64) ([]int64, error)
}

type UserAccountPolicyService struct {
	repo        UserAccountPolicyRepository
	invalidator APIKeyAuthCacheInvalidator
}

func NewUserAccountPolicyService(repo UserAccountPolicyRepository, invalidator APIKeyAuthCacheInvalidator) *UserAccountPolicyService {
	return &UserAccountPolicyService{repo: repo, invalidator: invalidator}
}
func (s *UserAccountPolicyService) Get(ctx context.Context, userID, groupID int64) (*UserAccountPolicy, error) {
	return s.repo.Get(ctx, userID, groupID)
}
func (s *UserAccountPolicyService) Set(ctx context.Context, userID, groupID int64, p UserAccountPolicy) error {
	if p.Mode != "all" && p.Mode != "allowlist" {
		return infraerrors.BadRequest("INVALID_ACCOUNT_POLICY", "mode must be all or allowlist")
	}
	if p.Mode == "all" && len(p.AccountIDs) > 0 {
		return infraerrors.BadRequest("INVALID_ACCOUNT_POLICY", "Unrestricted policies must have an empty account list")
	}
	seen := make(map[int64]bool, len(p.AccountIDs))
	ids := make([]int64, 0, len(p.AccountIDs))
	for _, id := range p.AccountIDs {
		if id <= 0 {
			return infraerrors.BadRequest("INVALID_ACCOUNT_POLICY", "Account IDs must be positive")
		}
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	p.AccountIDs = ids
	if err := s.repo.Set(ctx, userID, groupID, p); err != nil {
		return err
	}
	if s.invalidator != nil {
		s.invalidator.InvalidateAuthCacheByUserID(ctx, userID)
	}
	return nil
}

type accountPolicyContextKey struct{}
type accountPolicyScope struct {
	service *UserAccountPolicyService
	userID  int64
	groupID int64
	mu      sync.Mutex
	targets map[int64]int64
}

// WithUserAccountPolicy is installed only by authentication, never from request metadata.
func WithUserAccountPolicy(ctx context.Context, s *UserAccountPolicyService, userID int64, groupID *int64) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, accountPolicyContextKey{}, &accountPolicyScope{service: s, userID: userID, groupID: derefGroupID(groupID), targets: make(map[int64]int64)})
}

// CheckAccountAccess always reads current policy. In particular, a long-lived
// websocket or a request which waited for capacity cannot retain revoked access.
func CheckAccountAccess(ctx context.Context, accountID int64, targetGroupID *int64) error {
	scope, ok := ctx.Value(accountPolicyContextKey{}).(*accountPolicyScope)
	if !ok {
		return nil
	} // Internal administrative probes have no end-user scope.
	groups := []int64{scope.groupID}
	target := int64(0)
	if targetGroupID != nil {
		target = *targetGroupID
	} else if group, ok := ctx.Value(ctxkey.Group).(*Group); ok && group != nil {
		target = group.ID
	}
	scope.mu.Lock()
	if targetGroupID != nil {
		scope.targets[accountID] = target
	} else if remembered := scope.targets[accountID]; remembered > 0 {
		target = remembered
	}
	scope.mu.Unlock()
	if target > 0 && target != scope.groupID {
		groups = append(groups, target)
	}
	allowed, err := scope.service.repo.Allowed(ctx, scope.userID, groups, accountID)
	if err != nil {
		return ErrAccountPolicyUnavailable.WithCause(err)
	}
	if !allowed {
		slog.Info("account_policy_denied", "user_id", scope.userID, "group_id", scope.groupID, "target_group_id", target, "account_id", accountID)
		return ErrAccountAccessDenied
	}
	return nil
}

func filterAccountsByUserPolicy(ctx context.Context, groupID *int64, accounts []Account) ([]Account, error) {
	scope, ok := ctx.Value(accountPolicyContextKey{}).(*accountPolicyScope)
	if !ok {
		return accounts, nil
	}
	groups := []int64{scope.groupID}
	if groupID != nil && *groupID != scope.groupID {
		groups = append(groups, *groupID)
	}
	if len(accounts) == 0 {
		for _, group := range groups {
			if group <= 0 {
				continue
			}
			policy, err := scope.service.repo.Get(ctx, scope.userID, group)
			if err != nil {
				return nil, ErrAccountPolicyUnavailable.WithCause(err)
			}
			if policy != nil && policy.Mode == "allowlist" {
				if len(policy.AccountIDs) == 0 {
					return nil, ErrAccountAccessDenied
				}
				return nil, ErrAllowedAccountsUnavailable
			}
		}
		return accounts, nil
	}
	ids := make([]int64, 0, len(accounts))
	for _, a := range accounts {
		ids = append(ids, a.ID)
	}
	allowed, err := scope.service.repo.Filter(ctx, scope.userID, groups, ids)
	if err != nil {
		return nil, ErrAccountPolicyUnavailable.WithCause(err)
	}
	if len(allowed) == 0 {
		slog.Info("account_policy_pool_denied", "user_id", scope.userID, "group_id", scope.groupID, "target_group_id", derefGroupID(groupID))
		return nil, ErrAccountAccessDenied
	}
	set := make(map[int64]bool, len(allowed))
	for _, id := range allowed {
		set[id] = true
	}
	filtered := make([]Account, 0, len(allowed))
	scope.mu.Lock()
	for _, a := range accounts {
		if set[a.ID] {
			filtered = append(filtered, a)
			if groupID != nil {
				scope.targets[a.ID] = *groupID
			}
		}
	}
	scope.mu.Unlock()
	return filtered, nil
}
