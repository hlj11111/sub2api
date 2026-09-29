package admin

import (
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func accountPolicyParams(c *gin.Context) (int64, int64, bool) {
	userID, e1 := strconv.ParseInt(c.Param("id"), 10, 64)
	groupID, e2 := strconv.ParseInt(c.Param("group_id"), 10, 64)
	if e1 != nil || e2 != nil || userID <= 0 || groupID <= 0 {
		response.BadRequest(c, "Invalid user or group ID")
		return 0, 0, false
	}
	return userID, groupID, true
}
func (h *UserHandler) GetAccountPolicy(c *gin.Context) {
	userID, groupID, ok := accountPolicyParams(c)
	if !ok {
		return
	}
	p, err := h.accountPolicies.Get(c.Request.Context(), userID, groupID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, p)
}
func (h *UserHandler) SetAccountPolicy(c *gin.Context) {
	userID, groupID, ok := accountPolicyParams(c)
	if !ok {
		return
	}
	var p service.UserAccountPolicy
	if err := c.ShouldBindJSON(&p); err != nil {
		response.BadRequest(c, "Invalid account policy")
		return
	}
	if err := h.accountPolicies.Set(c.Request.Context(), userID, groupID, p); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	h.GetAccountPolicy(c)
}
