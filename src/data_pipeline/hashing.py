"""规范化 JSON 与内容摘要工具。"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


def canonical_json(value: object) -> str:
    """生成跨平台一致、键排序的紧凑 JSON 文本。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def digest_value(value: Any) -> str:
    """计算单个值的规范化 SHA-256。"""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def content_digest(values: Iterable[object]) -> str:
    """按输入顺序计算规范化内容摘要，逐条换行分隔以防拼接歧义。"""

    digest = hashlib.sha256()
    for value in values:
        digest.update(canonical_json(value).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
