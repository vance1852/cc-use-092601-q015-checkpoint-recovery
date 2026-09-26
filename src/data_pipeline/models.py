"""数据加工流水线的领域值对象。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """失败重试策略：最多尝试 ``max_attempts`` 次，指数退避封顶。"""

    max_attempts: int = 3
    backoff_base_seconds: int = 5
    backoff_max_seconds: int = 300

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValueError("max_attempts 必须是不小于 1 的整数")
        if not isinstance(self.backoff_base_seconds, int) or self.backoff_base_seconds < 0:
            raise ValueError("backoff_base_seconds 必须是非负整数")
        if not isinstance(self.backoff_max_seconds, int) or self.backoff_max_seconds < 0:
            raise ValueError("backoff_max_seconds 必须是非负整数")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds 不能小于 backoff_base_seconds")

    @classmethod
    def from_raw(cls, raw: object) -> RetryPolicy:
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("retry 必须是对象")
        return cls(
            max_attempts=int(raw.get("max_attempts", 3)),
            backoff_base_seconds=int(raw.get("backoff_base_seconds", 5)),
            backoff_max_seconds=int(raw.get("backoff_max_seconds", 300)),
        )

    def delay_seconds(self, attempts_used: int) -> int:
        """第 ``attempts_used`` 次尝试失败后的退避秒数。"""

        delay = self.backoff_base_seconds * (2 ** (attempts_used - 1))
        return min(delay, self.backoff_max_seconds)
