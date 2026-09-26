"""流水线服务向 API 和 CLI 暴露的稳定错误。"""


class PipelineError(RuntimeError):
    code = "pipeline_error"
    status = 400


class NotFound(PipelineError):
    code = "not_found"
    status = 404


class Conflict(PipelineError):
    code = "conflict"
    status = 409


class InvalidState(PipelineError):
    code = "invalid_state"
    status = 409


class StaleLease(PipelineError):
    """迟到结果：fencing 令牌或持有者与当前租约不符。"""

    code = "stale_lease"
    status = 409


class InputChanged(PipelineError):
    """完成记录与领取时的输入摘要或规则摘要不一致。"""

    code = "input_changed"
    status = 409


class ValidationFailed(PipelineError):
    code = "validation_failed"
    status = 422
