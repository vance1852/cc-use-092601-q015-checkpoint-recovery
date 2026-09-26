"""数据加工断点恢复服务的 SQLite 模式与事务辅助。"""

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

CREATE TABLE IF NOT EXISTS data_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('planner', 'supervisor', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_tasks (
    task_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running', 'cancelled', 'completed')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    rule_version TEXT NOT NULL,
    rule_params_json TEXT NOT NULL,
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    manifest_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    retry_delay_seconds INTEGER NOT NULL CHECK (retry_delay_seconds >= 0),
    retry_backoff_multiplier REAL NOT NULL CHECK (retry_backoff_multiplier >= 1.0),
    created_by TEXT NOT NULL REFERENCES data_users(user_id),
    created_at TEXT NOT NULL,
    cancelled_at TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS data_shards (
    task_id TEXT NOT NULL REFERENCES data_tasks(task_id),
    shard_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'ready', 'leased', 'succeeded', 'failed', 'manual')),
    task_revision INTEGER NOT NULL CHECK (task_revision > 0),
    input_refs_json TEXT NOT NULL,
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    depends_on_json TEXT NOT NULL,
    pending_deps INTEGER NOT NULL CHECK (pending_deps >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner TEXT,
    lease_seq INTEGER NOT NULL DEFAULT 0 CHECK (lease_seq >= 0),
    lease_expires_at TEXT,
    available_at TEXT NOT NULL,
    output_sha256 TEXT CHECK (output_sha256 IS NULL OR length(output_sha256) = 64),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (task_id, shard_key)
);

CREATE INDEX IF NOT EXISTS idx_data_shards_claimable
ON data_shards(state, available_at);

CREATE TABLE IF NOT EXISTS data_shard_deps (
    task_id TEXT NOT NULL,
    shard_key TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (task_id, shard_key, depends_on),
    FOREIGN KEY (task_id, shard_key) REFERENCES data_shards(task_id, shard_key)
);

CREATE INDEX IF NOT EXISTS idx_data_shard_deps_upstream
ON data_shard_deps(task_id, depends_on);

CREATE TABLE IF NOT EXISTS data_completions (
    completion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    shard_key TEXT NOT NULL,
    task_revision INTEGER NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    lease_seq INTEGER NOT NULL CHECK (lease_seq >= 0),
    worker_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    rule_version TEXT NOT NULL,
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    output_sha256 TEXT NOT NULL CHECK (length(output_sha256) = 64),
    result_json TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    UNIQUE (task_id, shard_key, task_revision, attempt)
);

CREATE INDEX IF NOT EXISTS idx_data_completions_shard
ON data_completions(task_id, shard_key, completion_id);

CREATE TABLE IF NOT EXISTS data_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_data_audit_entity
ON data_audit_events(entity_type, entity_id, event_id);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "data_users", "data_tasks", "data_shards", "data_shard_deps",
    "data_completions", "data_audit_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。

    check_same_thread=False 允许 HTTP 线程池复用单个连接；
    并发请求由 api 层的调度锁串行化，事务完整性不受影响。
    """

    connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
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
    """初始化基础资料表，重复执行不改变已有数据。"""

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
