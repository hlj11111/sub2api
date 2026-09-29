package service

import (
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var ErrOpenAIRecoveryExhausted = infraerrors.ServiceUnavailable("LOCAL_SESSION_RECOVERY_EXHAUSTED", "本地恢复次数已用完：本轮已尝试备用渠道，请稍后在当前会话重试").WithCause(ErrOpenAIContinuityUnavailable)
var ErrOpenAIReplayIncomplete = infraerrors.Conflict("LOCAL_SESSION_REPLAY_INCOMPLETE", "本地上下文校验未通过：缺少可独立重放的完整消息或工具结果，无法安全切换渠道；请补发完整历史或稍后重试原渠道").WithCause(ErrOpenAIContextIncomplete)

func continuityLocalStateError(code, message string, cause error) error {
	return infraerrors.ServiceUnavailable(code, message).WithCause(ErrOpenAIContinuityUnavailable.WithCause(cause))
}

func continuityContextError(reason, state string) error {
	code, message := "LOCAL_SESSION_REPLAY_INCOMPLETE", ErrOpenAIReplayIncomplete.Message
	switch reason {
	case "session_identity_missing":
		code, message = "LOCAL_SESSION_ID_MISSING", "本地会话校验未通过：请求含历史状态，但缺少可识别的会话标识；请保持原会话标识并重试"
	case "session_binding_missing":
		code, message = "LOCAL_SESSION_BINDING_MISSING", "本地会话归属缺失：未找到原渠道，且当前历史无法安全跨渠道重放；请补发完整历史或稍后重试"
	case "previous_response_owner_missing":
		code, message = "LOCAL_RESPONSE_OWNER_MISSING", "本地响应归属缺失：无法确定 previous_response_id 所属渠道；请补发完整历史后重试"
	case "forward_protocol_cannot_resume_previous_response", "http_bridge_requires_replay":
		code, message = "LOCAL_SESSION_PROTOCOL_UNSUPPORTED", "本地协议校验未通过：当前转发协议无法使用历史响应 ID 接续；请补发完整历史"
	default:
		switch state {
		case "unmatched_tool_output":
			code, message = "LOCAL_SESSION_TOOL_CONTEXT_MISSING", "本地工具历史校验未通过：工具结果缺少对应调用，无法安全切换渠道；请补发完整工具调用和结果"
		case "encrypted_content":
			code, message = "LOCAL_SESSION_ENCRYPTED_HISTORY", "本地历史校验未通过：请求含加密历史，且无法确认可独立重放的消息和工具结果；原渠道不可用时不能安全切换"
		case "compaction":
			code, message = "LOCAL_SESSION_COMPACTED_HISTORY", "本地历史校验未通过：请求依赖加密压缩历史，当前没有可重建的完整历史；请稍后重试原渠道"
		case "file_id", "file_ids", "vector_store_ids", "container_reference", "item_reference", "conversation_reference":
			code, message = "LOCAL_SESSION_RESOURCE_BOUND", "本地资源校验未通过：请求引用原渠道的历史或资源，无法直接换渠道；请稍后重试原渠道"
		}
	}
	return infraerrors.Conflict(code, message).WithCause(ErrOpenAIContextIncomplete)
}

// Stable types distinguish local checks and state failures from upstream errors.
// Codes remain machine-readable; messages are intended for Chinese clients.
func OpenAIContinuityErrorType(code string) string {
	switch {
	case code == "ACCOUNT_ACCESS_DENIED":
		return "local_permission_error"
	case code == "ACCOUNT_POLICY_UNAVAILABLE", strings.HasPrefix(code, "LOCAL_SESSION_STORE"), strings.HasPrefix(code, "LOCAL_SESSION_LEASE"):
		return "local_state_error"
	case code == "ALLOWED_ACCOUNTS_UNAVAILABLE", code == "SESSION_ACCOUNT_UNAVAILABLE", code == "LOCAL_SESSION_RECOVERY_EXHAUSTED":
		return "local_routing_error"
	default:
		return "local_validation_error"
	}
}
