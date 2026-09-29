package admin

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type accountPolicyHandlerRepo struct {
	service.UserAccountPolicyRepository
	policy      service.UserAccountPolicy
	user, group int64
}

func (r *accountPolicyHandlerRepo) Get(_ context.Context, u, g int64) (*service.UserAccountPolicy, error) {
	r.user, r.group = u, g
	return &r.policy, nil
}
func (r *accountPolicyHandlerRepo) Set(_ context.Context, u, g int64, p service.UserAccountPolicy) error {
	r.user, r.group, r.policy = u, g, p
	return nil
}
func TestUserAccountPolicyAdminEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &accountPolicyHandlerRepo{policy: service.UserAccountPolicy{Mode: "all", AccountIDs: []int64{}}}
	h := &UserHandler{accountPolicies: service.NewUserAccountPolicyService(repo, nil)}
	router := gin.New()
	router.GET("/users/:id/groups/:group_id/account-policy", h.GetAccountPolicy)
	router.PUT("/users/:id/groups/:group_id/account-policy", h.SetAccountPolicy)
	for _, tc := range []struct {
		method, path, body string
		status             int
		contains           string
	}{
		{"GET", "/users/7/groups/9/account-policy", "", 200, "\"mode\":\"all\""},
		{"PUT", "/users/7/groups/9/account-policy", "{\"mode\":\"allowlist\",\"account_ids\":[]}", 200, "\"mode\":\"allowlist\""},
		{"GET", "/users/7/groups/9/account-policy", "", 200, "\"account_ids\":[]"},
		{"PUT", "/users/7/groups/9/account-policy", "{\"mode\":\"invalid\"}", 400, "mode must"},
		{"PUT", "/users/7/groups/9/account-policy", "{\"mode\":\"all\",\"account_ids\":[1]}", 400, "empty account list"},
		{"GET", "/users/no/groups/9/account-policy", "", 400, "Invalid user or group"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, tc.status, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), tc.contains)
	}
	require.EqualValues(t, 7, repo.user)
	require.EqualValues(t, 9, repo.group)
}
