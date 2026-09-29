package repository

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
)

type denyUpstreamAccountPolicyRepo struct {
	service.UserAccountPolicyRepository
}

func (denyUpstreamAccountPolicyRepo) Allowed(context.Context, int64, []int64, int64) (bool, error) {
	return false, nil
}
func TestUserAccountPolicyStopsAtHTTPTransport(t *testing.T) {
	group := int64(1)
	ctx := service.WithUserAccountPolicy(context.Background(), service.NewUserAccountPolicyService(denyUpstreamAccountPolicyRepo{}, nil), 7, &group)
	req := httptest.NewRequest("POST", "https://example.test/v1/responses", nil).WithContext(ctx)
	upstream := &httpUpstreamService{}
	_, err := upstream.Do(req, "", 3, 1)
	require.ErrorIs(t, err, service.ErrAccountAccessDenied)
	_, err = upstream.DoWithTLS(req, "", 3, 1, nil)
	require.ErrorIs(t, err, service.ErrAccountAccessDenied)
}
