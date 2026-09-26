"""数据加工流水线的 SQLite 模式与事务辅助。

断点恢复依赖以下持久化证据：

- ``pipeline_jobs``：任务级输入清单摘要、处理规则版本与摘要、重试策略；
- ``pipeline_shards``：分片输入摘要、依赖、租约持有者/到期时间/fencing 令牌、
  输出校验值与人工处置状态；
- ``shard_dependencies``：分片依赖边，完成与解锁在同一事务内更新；
- ``shard_attempts``：每次领取与结局，用于重启后解释分片来源；
- ``pipeline_events``：取消、人工处置等审计事件。
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path


SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('active', 'cancelled', 'completed')),
    rule_id TEXT NOT NULL,
    rule_version INTEGER NOT NULL CHECK (rule_version > 0),
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    manifest_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    retry_max_attempts INTEGER NOT NULL CHECK (retry_max_attempts > 0),
    retry_backoff_base_seconds INTEGER NOT NULL CHECK (retry_backoff_base_seconds >= 0),
    retry_backoff_max_seconds INTEGER NOT NULL CHECK (retry_backoff_max_seconds >= 0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    cancelled_at TEXT,
    cancel_reason TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_shards (
    shard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES pipeline_jobs(job_id),
    shard_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    state TEXT NOT NULL CHECK (
        state IN ('blocked', 'waiting', 'leased', 'succeeded', 'manual', 'cancelled')
    ),
    input_spec_json TEXT NOT NULL,
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    dependencies_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner TEXT,
    lease_expires_at TEXT,
    lease_fence INTEGER NOT NULL DEFAULT 0 CHECK (lease_fence >= 0),
    available_at TEXT NOT NULL,
    output_sha256 TEXT,
    output_location TEXT,
    result_json TEXT,
    last_error TEXT,
    manual_reason TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (job_id, shard_key)
);

CREATE INDEX IF NOT EXISTS idx_shards_claim
ON pipeline_shards(state, available_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS shard_dependencies (
    job_id TEXT NOT NULL REFERENCES pipeline_jobs(job_id),
    shard_key TEXT NOT NULL,
    depends_on_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (job_id, shard_key, depends_on_key)
);

CREATE TABLE IF NOT EXISTS shard_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES pipeline_jobs(job_id),
    shard_id INTEGER NOT NULL REFERENCES pipeline_shards(shard_id),
    attempt_no INTEGER NOT NULL,
    fence INTEGER NOT NULL,
    worker_id TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed', 'expired')),
    error TEXT,
    output_sha256 TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempts_shard ON shard_attempts(shard_id, attempt_id);

CREATE TABLE IF NOT EXISTS pipeline_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES pipeline_jobs(job_id),
    shard_key TEXT,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "pipeline_jobs", "pipeline_shards", "shard_dependencies",
    "shard_attempts", "pipeline_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用外键与繁忙等待。"""

    # check_same_thread=False：HTTP 服务用 ThreadingHTTPServer 分发请求；
    # 所有写事务都是 BEGIN IMMEDIATE 且设置了 busy_timeout，串行化由 SQLite 保证。
    connection = sqlite3.connect(
        str(path), isolation_level=None, check_same_thread=False
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """显式事务；异常时保证回滚。"""

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def initialize(connection: sqlite3.Connection) -> None:
    """初始化模式，重复执行不改变已有数据。"""

    connection.executescript(SCHEMA_SQL)
    with transaction(connection, immediate=True):
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


def inspect_schema(connection: sqlite3.Connection) -> dict[str, object]:
    """返回适合机器检查的数据库结构摘要。"""

    table_rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    tables = tuple(row["name"] for row in table_rows)
    version_row = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    missing = sorted(REQUIRED_TABLES - set(tables))
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    return {
        "tables": tables,
        "missing_tables": missing,
        "schema_version": None if version_row is None else version_row["value"],
        "foreign_keys": bool(foreign_keys),
    }
