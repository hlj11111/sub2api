package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type policyTestRepo struct {
	policies map[int64]map[int64]bool
	err      error
	saved    UserAccountPolicy
}

func (r *policyTestRepo) Get(context.Context, int64, int64) (*UserAccountPolicy, error) {
	return &r.saved, r.err
}
func (r *policyTestRepo) Set(_ context.Context, _, _ int64, p UserAccountPolicy) error {
	r.saved = p
	return r.err
}
func (r *policyTestRepo) Allowed(_ context.Context, _ int64, groups []int64, id int64) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	for _, group := range groups {
		if ids, restricted := r.policies[group]; restricted && !ids[id] {
			return false, nil
		}
	}
	return true, nil
}
func (r *policyTestRepo) Filter(ctx context.Context, u int64, groups, ids []int64) ([]int64, error) {
	var allowed []int64
	for _, id := range ids {
		ok, err := r.Allowed(ctx, u, groups, id)
		if err != nil {
			return nil, err
		}
		if ok {
			allowed = append(allowed, id)
		}
	}
	return allowed, nil
}
func TestUserAccountPolicyIntersectionAndLiveRevocation(t *testing.T) {
	repo := &policyTestRepo{policies: map[int64]map[int64]bool{1: {10: true, 20: true}, 2: {20: true, 30: true}}}
	svc := NewUserAccountPolicyService(repo, nil)
	origin, target := int64(1), int64(2)
	ctx := WithUserAccountPolicy(context.Background(), svc, 7, &origin)
	got, err := filterAccountsByUserPolicy(ctx, &target, []Account{{ID: 10}, {ID: 20}, {ID: 30}})
	require.NoError(t, err)
	require.Equal(t, []Account{{ID: 20}}, got)
	require.NoError(t, CheckAccountAccess(ctx, 20, nil))
	// Removing the final allowed account does not restore unrestricted access.
	delete(repo.policies[2], 20)
	require.ErrorIs(t, CheckAccountAccess(ctx, 20, nil), ErrAccountAccessDenied)
	require.NoError(t, CheckAccountAccess(context.Background(), 20, nil))
}
func TestUserAccountPolicyFailureClosedAndEmptyList(t *testing.T) {
	repo := &policyTestRepo{policies: map[int64]map[int64]bool{1: {}}}
	group := int64(1)
	ctx := WithUserAccountPolicy(context.Background(), NewUserAccountPolicyService(repo, nil), 7, &group)
	require.ErrorIs(t, CheckAccountAccess(ctx, 1, nil), ErrAccountAccessDenied)
	repo.err = errors.New("database unavailable")
	require.Error(t, CheckAccountAccess(ctx, 1, nil))
	_, err := filterAccountsByUserPolicy(ctx, &group, []Account{{ID: 1}})
	require.Error(t, err)
}
func TestUserAccountPolicyValidation(t *testing.T) {
	repo := &policyTestRepo{}
	svc := NewUserAccountPolicyService(repo, nil)
	for _, p := range []UserAccountPolicy{{Mode: ""}, {Mode: "all", AccountIDs: []int64{1}}, {Mode: "allowlist", AccountIDs: []int64{-1}}} {
		require.Error(t, svc.Set(context.Background(), 1, 2, p))
	}
	require.NoError(t, svc.Set(context.Background(), 1, 2, UserAccountPolicy{Mode: "allowlist", AccountIDs: []int64{2, 2, 3}}))
	require.Equal(t, []int64{2, 3}, repo.saved.AccountIDs)
	require.NoError(t, svc.Set(context.Background(), 1, 2, UserAccountPolicy{Mode: "allowlist"}))
	require.Equal(t, "allowlist", repo.saved.Mode)
}

type policyInvalidator struct {
	APIKeyAuthCacheInvalidator
	users []int64
}

func (i *policyInvalidator) InvalidateAuthCacheByUserID(_ context.Context, id int64) {
	i.users = append(i.users, id)
}
func TestUserAccountPolicyInvalidatesAllKeysAfterSave(t *testing.T) {
	repo := &policyTestRepo{}
	invalidator := &policyInvalidator{}
	svc := NewUserAccountPolicyService(repo, invalidator)
	require.NoError(t, svc.Set(context.Background(), 7, 9, UserAccountPolicy{Mode: "allowlist"}))
	require.Equal(t, []int64{7}, invalidator.users)
	repo.err = errors.New("save failed")
	require.Error(t, svc.Set(context.Background(), 7, 9, UserAccountPolicy{Mode: "all"}))
	require.Equal(t, []int64{7}, invalidator.users)
}

func TestUserAccountPolicyBatchSelectionAndTaskAccess(t *testing.T) {
	group := int64(1)
	policies := &policyTestRepo{policies: map[int64]map[int64]bool{group: {20: true}}}
	ctx := WithUserAccountPolicy(context.Background(), NewUserAccountPolicyService(policies, nil), 7, &group)
	svc := &BatchImagePublicService{AccountRepo: &schedulerTestOpenAIAccountRepo{accounts: []Account{{ID: 10, Platform: PlatformGemini}, {ID: 20, Platform: PlatformGemini}}}}
	accounts, err := svc.listCandidateAccounts(ctx, &group, PlatformGemini)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	require.EqualValues(t, 20, accounts[0].ID)
	denied := int64(10)
	require.ErrorIs(t, checkBatchImageJobAccountAccess(ctx, &BatchImageJob{AccountID: &denied}), ErrAccountAccessDenied)
	granted := int64(20)
	require.NoError(t, checkBatchImageJobAccountAccess(ctx, &BatchImageJob{AccountID: &granted}))
	delete(policies.policies[group], 20)
	require.ErrorIs(t, checkBatchImageJobAccountAccess(ctx, &BatchImageJob{AccountID: &granted}), ErrAccountAccessDenied)
	require.ErrorIs(t, batchImageProviderSubmitPublicError(ErrAccountAccessDenied), ErrAccountAccessDenied)
}
