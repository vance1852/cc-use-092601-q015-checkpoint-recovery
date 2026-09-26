"""数据加工任务断点恢复的领域用例。

任务保存输入清单摘要、处理规则版本、分片依赖和输出校验值；
工作进程通过有期限的租约领取分片，完成记录与下游分片解锁在同一事务提交；
租约过期可被接管，迟到结果因租约序号不匹配而无法覆盖新持有者；
失败按任务级可配置策略重试，耗尽后进入人工处置；
取消只阻止新领取，已完成证据永久保留。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from datetime import timedelta
from typing import Any, Mapping, Sequence

from .clock import SystemClock, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"task.create", "task.revise", "task.cancel", "task.read"},
    "supervisor": {"shard.resolve", "task.read"},
    "auditor": {"task.read", "audit.read"},
}

# 存储状态到查询分组的映射：待处理 / 运行中 / 失败 / 完成
STATE_GROUPS = {
    "pending": "pending",
    "ready": "pending",
    "leased": "running",
    "failed": "failed",
    "manual": "failed",
    "succeeded": "completed",
}

_KEY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
MAX_LEASE_SECONDS = 86400


def _normalize_key(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not _KEY_PATTERN.match(text):
        raise ValidationFailed(f"{field}只能包含字母、数字、点、下划线和连字符，且不超过 64 字符")
    return text


def _normalize_manifest(raw: object) -> list[dict[str, str]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)) or not raw:
        raise ValidationFailed("输入清单不能为空")
    entries: dict[str, dict[str, str]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValidationFailed("输入清单条目必须是对象")
        uri = str(item.get("uri") or "").strip()
        sha256 = str(item.get("sha256") or "").strip().lower()
        if not uri:
            raise ValidationFailed("输入清单条目缺少 uri")
        if not _HEX64.match(sha256):
            raise ValidationFailed(f"输入对象 {uri} 的 sha256 必须是 64 位十六进制")
        if uri in entries:
            raise ValidationFailed(f"输入清单 uri 重复: {uri}")
        entries[uri] = {"uri": uri, "sha256": sha256}
    return [entries[uri] for uri in sorted(entries)]


def _normalize_shards(raw: object, manifest_by_uri: Mapping[str, Mapping[str, str]]) -> list[dict[str, Any]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)) or not raw:
        raise ValidationFailed("分片定义不能为空")
    shards: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValidationFailed("分片定义必须是对象")
        key = _normalize_key(item.get("shard_key"), "分片键")
        if key in shards:
            raise ValidationFailed(f"分片键重复: {key}")
        inputs = item.get("inputs") or []
        depends_on = item.get("depends_on") or []
        if not isinstance(inputs, (list, tuple)) or not isinstance(depends_on, (list, tuple)):
            raise ValidationFailed(f"分片 {key} 的 inputs 和 depends_on 必须是数组")
        if not all(isinstance(value, str) for value in inputs):
            raise ValidationFailed(f"分片 {key} 的 inputs 必须是字符串数组")
        if not all(isinstance(value, str) for value in depends_on):
            raise ValidationFailed(f"分片 {key} 的 depends_on 必须是字符串数组")
        unknown_inputs = sorted({uri for uri in inputs if uri not in manifest_by_uri})
        if unknown_inputs:
            raise ValidationFailed(f"分片 {key} 引用了清单外的输入: {', '.join(unknown_inputs)}")
        input_refs = [dict(manifest_by_uri[uri]) for uri in sorted(set(inputs))]
        dependencies = sorted(set(depends_on))
        if key in dependencies:
            raise ValidationFailed(f"分片 {key} 不能依赖自身")
        if not input_refs and not dependencies:
            raise ValidationFailed(f"分片 {key} 必须至少有一个输入或一个依赖")
        shards[key] = {"shard_key": key, "input_refs": input_refs, "depends_on": dependencies}
    for key, spec in shards.items():
        unknown_deps = [dep for dep in spec["depends_on"] if dep not in shards]
        if unknown_deps:
            raise ValidationFailed(f"分片 {key} 依赖了不存在的分片: {', '.join(unknown_deps)}")
    _assert_acyclic(shards)
    return [shards[key] for key in sorted(shards)]


def _assert_acyclic(shards: Mapping[str, Mapping[str, Any]]) -> None:
    indegree = {key: 0 for key in shards}
    for key, spec in shards.items():
        for _dep in spec["depends_on"]:
            indegree[key] += 1
    queue = sorted(key for key, degree in indegree.items() if degree == 0)
    visited = 0
    while queue:
        current = queue.pop()
        visited += 1
        for key, spec in shards.items():
            if current in spec["depends_on"]:
                indegree[key] -= 1
                if indegree[key] == 0:
                    queue.append(key)
    if visited != len(shards):
        raise ValidationFailed("分片依赖存在环路")


def _normalize_retry_policy(raw: object) -> dict[str, Any]:
    if raw is not None and not isinstance(raw, Mapping):
        raise ValidationFailed("重试策略必须是 JSON 对象")
    payload: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
    try:
        max_attempts = int(payload.get("max_attempts", 3))
        delay_seconds = int(payload.get("retry_delay_seconds", 30))
        multiplier = float(payload.get("backoff_multiplier", 1.0))
    except (TypeError, ValueError) as exc:
        raise ValidationFailed("重试策略必须是整数次数、整数秒数和数值倍率") from exc
    if isinstance(payload.get("max_attempts"), bool) or isinstance(payload.get("retry_delay_seconds"), bool):
        raise ValidationFailed("重试策略的次数和秒数必须是整数")
    if max_attempts < 1:
        raise ValidationFailed("最大尝试次数必须大于零")
    if delay_seconds < 0:
        raise ValidationFailed("重试间隔不能为负数")
    if not math.isfinite(multiplier) or multiplier < 1.0:
        raise ValidationFailed("退避倍率必须是不小于 1 的有限数值")
    return {
        "max_attempts": max_attempts,
        "retry_delay_seconds": delay_seconds,
        "backoff_multiplier": multiplier,
    }


class ForgeService:
    """在单个 SQLite 连接上提供数据加工断点恢复的全部业务操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM data_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"用户不存在: {user_id}")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self, entity_type: str, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM data_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        created_at = self._now()
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": created_at,
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO data_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload),
             previous_hash, event_hash, created_at),
        )

    # ------------------------------------------------------------------
    # 用户与任务定义
    # ------------------------------------------------------------------

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO data_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    def _build_definition(
        self,
        rule_version: object,
        rule_params: object,
        raw_manifest: object,
        raw_shards: object,
    ) -> dict[str, Any]:
        version = str(rule_version or "").strip()
        if not version:
            raise ValidationFailed("处理规则版本不能为空")
        params: Mapping[str, Any] = rule_params if isinstance(rule_params, Mapping) else {}
        if rule_params is not None and not isinstance(rule_params, Mapping):
            raise ValidationFailed("处理规则参数必须是 JSON 对象")
        manifest = _normalize_manifest(raw_manifest)
        manifest_by_uri = {entry["uri"]: entry for entry in manifest}
        shards = _normalize_shards(raw_shards, manifest_by_uri)
        manifest_sha256 = content_digest(manifest)
        rule_sha256 = content_digest([{"version": version, "params": params}])
        for spec in shards:
            spec["input_sha256"] = content_digest(spec["input_refs"]) if spec["input_refs"] else content_digest(
                [{"derived_from": dep} for dep in spec["depends_on"]]
            )
        return {
            "rule_version": version,
            "rule_params": dict(params),
            "rule_sha256": rule_sha256,
            "manifest": manifest,
            "manifest_sha256": manifest_sha256,
            "shards": shards,
        }

    def create_task(
        self,
        actor_id: str,
        task_id: str,
        name: str,
        rule_version: object,
        rule_params: object,
        manifest: object,
        shards: object,
        retry_policy: object = None,
    ) -> dict[str, Any]:
        self._require(actor_id, "task.create")
        task_id = _normalize_key(task_id, "任务编号")
        if not str(name or "").strip():
            raise ValidationFailed("任务名称不能为空")
        definition = self._build_definition(rule_version, rule_params, manifest, shards)
        policy = _normalize_retry_policy(retry_policy)
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO data_tasks(task_id,name,state,revision,rule_version,rule_params_json,rule_sha256,"
                    "manifest_json,manifest_sha256,max_attempts,retry_delay_seconds,retry_backoff_multiplier,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task_id, str(name).strip(), "running", 1,
                        definition["rule_version"], canonical_json(definition["rule_params"]),
                        definition["rule_sha256"], canonical_json(definition["manifest"]),
                        definition["manifest_sha256"], policy["max_attempts"],
                        policy["retry_delay_seconds"], policy["backoff_multiplier"], actor_id, now,
                    ),
                )
                self._insert_shards(task_id, 1, definition, now)
                self._audit("task", task_id, "task.created", actor_id, {
                    "manifest_sha256": definition["manifest_sha256"],
                    "rule_version": definition["rule_version"],
                    "rule_sha256": definition["rule_sha256"],
                    "shard_count": len(definition["shards"]),
                    "retry_policy": policy,
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"任务已存在: {task_id}") from exc
        return self.get_task(actor_id, task_id)

    def _insert_shards(self, task_id: str, revision: int, definition: Mapping[str, Any], now: str) -> None:
        for spec in definition["shards"]:
            pending_deps = len(spec["depends_on"])
            self.connection.execute(
                "INSERT INTO data_shards(task_id,shard_key,state,task_revision,input_refs_json,input_sha256,"
                "manifest_sha256,rule_sha256,depends_on_json,pending_deps,available_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id, spec["shard_key"],
                    "pending" if pending_deps else "ready",
                    revision, canonical_json(spec["input_refs"]), spec["input_sha256"],
                    definition["manifest_sha256"], definition["rule_sha256"],
                    canonical_json(spec["depends_on"]), pending_deps, now, now, now,
                ),
            )
            for dep in spec["depends_on"]:
                self.connection.execute(
                    "INSERT INTO data_shard_deps(task_id,shard_key,depends_on) VALUES(?,?,?)",
                    (task_id, spec["shard_key"], dep),
                )

    def _task_row(self, task_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM data_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"任务不存在: {task_id}")
        return row

    def _shard_row(self, task_id: str, shard_key: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM data_shards WHERE task_id=? AND shard_key=?", (task_id, shard_key)
        ).fetchone()
        if row is None:
            raise NotFound(f"分片不存在: {task_id}/{shard_key}")
        return row

    def _shard_topology(self, task_id: str) -> str:
        rows = self.connection.execute(
            "SELECT shard_key,input_refs_json,depends_on_json FROM data_shards WHERE task_id=? ORDER BY shard_key",
            (task_id,),
        ).fetchall()
        return canonical_json([
            {
                "shard_key": row["shard_key"],
                "input_refs": json.loads(row["input_refs_json"]),
                "depends_on": json.loads(row["depends_on_json"]),
            }
            for row in rows
        ])

    def revise_task(
        self,
        actor_id: str,
        task_id: str,
        expected_revision: int,
        rule_version: object,
        rule_params: object,
        manifest: object,
        shards: object,
        retry_policy: object = None,
    ) -> dict[str, Any]:
        """以新的输入清单或规则重新定义任务；未完成分片重置，完成证据保留。"""

        self._require(actor_id, "task.revise")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 1:
            raise ValidationFailed("期望版本必须是正整数")
        definition = self._build_definition(rule_version, rule_params, manifest, shards)
        policy = None if retry_policy is None else _normalize_retry_policy(retry_policy)
        now = self._now()
        with transaction(self.connection, immediate=True):
            task = self._task_row(task_id)
            if task["state"] != "running":
                raise InvalidState("只有运行中的任务可以修订")
            if task["revision"] != int(expected_revision):
                raise InvalidState("任务版本已变化，请重新读取后再修订")
            new_topology = canonical_json([
                {"shard_key": spec["shard_key"], "input_refs": spec["input_refs"], "depends_on": spec["depends_on"]}
                for spec in definition["shards"]
            ])
            if (
                definition["manifest_sha256"] == task["manifest_sha256"]
                and definition["rule_sha256"] == task["rule_sha256"]
                and new_topology == self._shard_topology(task_id)
            ):
                raise ValidationFailed("输入清单、处理规则和分片定义均未变化")
            new_revision = task["revision"] + 1
            if policy is None:
                self.connection.execute(
                    "UPDATE data_tasks SET revision=?,rule_version=?,rule_params_json=?,rule_sha256=?,"
                    "manifest_json=?,manifest_sha256=? WHERE task_id=?",
                    (
                        new_revision, definition["rule_version"], canonical_json(definition["rule_params"]),
                        definition["rule_sha256"], canonical_json(definition["manifest"]),
                        definition["manifest_sha256"], task_id,
                    ),
                )
            else:
                self.connection.execute(
                    "UPDATE data_tasks SET revision=?,rule_version=?,rule_params_json=?,rule_sha256=?,"
                    "manifest_json=?,manifest_sha256=?,max_attempts=?,retry_delay_seconds=?,"
                    "retry_backoff_multiplier=? WHERE task_id=?",
                    (
                        new_revision, definition["rule_version"], canonical_json(definition["rule_params"]),
                        definition["rule_sha256"], canonical_json(definition["manifest"]),
                        definition["manifest_sha256"], policy["max_attempts"],
                        policy["retry_delay_seconds"], policy["backoff_multiplier"], task_id,
                    ),
                )
            self.connection.execute("DELETE FROM data_shard_deps WHERE task_id=?", (task_id,))
            self.connection.execute("DELETE FROM data_shards WHERE task_id=?", (task_id,))
            self._insert_shards(task_id, new_revision, definition, now)
            self._audit("task", task_id, "task.revised", actor_id, {
                "from_revision": task["revision"],
                "to_revision": new_revision,
                "from_manifest_sha256": task["manifest_sha256"],
                "to_manifest_sha256": definition["manifest_sha256"],
                "from_rule_sha256": task["rule_sha256"],
                "to_rule_sha256": definition["rule_sha256"],
            })
        return self.get_task(actor_id, task_id)

    def cancel_task(self, actor_id: str, task_id: str, reason: str = "") -> dict[str, Any]:
        """取消任务：阻止新领取，保留全部已完成证据，在途租约仍可登记结果。"""

        self._require(actor_id, "task.cancel")
        with transaction(self.connection, immediate=True):
            task = self._task_row(task_id)
            if task["state"] != "running":
                raise InvalidState("只有运行中的任务可以取消")
            self.connection.execute(
                "UPDATE data_tasks SET state='cancelled',cancelled_at=? WHERE task_id=? AND state='running'",
                (self._now(), task_id),
            )
            counts = self._state_counts(task_id)
            self._audit("task", task_id, "task.cancelled", actor_id, {
                "reason": str(reason or ""), "states": counts,
            })
        return self.get_task(actor_id, task_id)

    # ------------------------------------------------------------------
    # 租约领取与结果登记
    # ------------------------------------------------------------------

    def claim_shard(self, worker_id: str, lease_seconds: int = 60, task_id: str | None = None) -> dict[str, Any] | None:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValidationFailed("工作进程编号不能为空")
        if isinstance(lease_seconds, bool):
            raise ValidationFailed("租约时长必须是整数秒")
        try:
            lease_seconds = int(lease_seconds)
        except (TypeError, ValueError) as exc:
            raise ValidationFailed("租约时长必须是整数秒") from exc
        if lease_seconds <= 0 or lease_seconds > MAX_LEASE_SECONDS:
            raise ValidationFailed(f"租约时长必须在 1 到 {MAX_LEASE_SECONDS} 秒之间")
        now = self._now()
        expires = utc_text(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            if task_id is not None:
                self._task_row(task_id)
            row = self.connection.execute(
                "SELECT s.task_id AS task_id, s.shard_key AS shard_key FROM data_shards s "
                "JOIN data_tasks t ON t.task_id=s.task_id "
                "WHERE t.state='running' AND ("
                "(s.state IN ('ready','failed') AND s.available_at<=?) "
                "OR (s.state='leased' AND s.lease_expires_at<=?))"
                + (" AND s.task_id=?" if task_id is not None else "")
                + " ORDER BY s.available_at,s.task_id,s.shard_key LIMIT 1",
                (now, now, task_id) if task_id is not None else (now, now),
            ).fetchone()
            if row is None:
                return None
            cursor = self.connection.execute(
                "UPDATE data_shards SET state='leased',lease_owner=?,lease_seq=lease_seq+1,"
                "lease_expires_at=?,attempts=attempts+1,last_error=NULL,updated_at=? "
                "WHERE task_id=? AND shard_key=? AND ("
                "(state IN ('ready','failed') AND available_at<=?) "
                "OR (state='leased' AND lease_expires_at<=?))",
                (worker, expires, now, row["task_id"], row["shard_key"], now, now),
            )
            if cursor.rowcount != 1:
                raise Conflict("分片领取发生竞争，请重试")
            shard = self._shard_row(row["task_id"], row["shard_key"])
            task = self._task_row(row["task_id"])
            self._audit("shard", f"{row['task_id']}/{row['shard_key']}", "shard.leased", worker, {
                "attempt": shard["attempts"],
                "lease_seq": shard["lease_seq"],
                "lease_expires_at": expires,
            })
        return self._claimed_view(task, shard)

    @staticmethod
    def _claimed_view(task: sqlite3.Row, shard: sqlite3.Row) -> dict[str, Any]:
        return {
            "task_id": shard["task_id"],
            "shard_key": shard["shard_key"],
            "state": shard["state"],
            "task_revision": shard["task_revision"],
            "attempt": shard["attempts"],
            "lease_owner": shard["lease_owner"],
            "lease_seq": shard["lease_seq"],
            "lease_expires_at": shard["lease_expires_at"],
            "rule_version": task["rule_version"],
            "rule_params": json.loads(task["rule_params_json"]),
            "manifest_sha256": shard["manifest_sha256"],
            "rule_sha256": shard["rule_sha256"],
            "input_sha256": shard["input_sha256"],
            "input_refs": json.loads(shard["input_refs_json"]),
            "depends_on": json.loads(shard["depends_on_json"]),
        }

    def _check_lease(self, shard: sqlite3.Row, worker_id: str, lease_seq: int, now: str) -> None:
        if isinstance(lease_seq, bool) or not isinstance(lease_seq, int) or lease_seq < 0:
            raise ValidationFailed("租约序号必须是非负整数")
        if (
            shard["state"] != "leased"
            or shard["lease_owner"] != worker_id
            or shard["lease_seq"] != int(lease_seq)
        ):
            raise InvalidState("分片租约不属于该工作进程，迟到结果不能覆盖新持有者")
        if shard["lease_expires_at"] <= now:
            raise InvalidState("分片租约已经过期")

    def _check_definition_unchanged(self, task: sqlite3.Row, shard: sqlite3.Row) -> None:
        if (
            shard["task_revision"] != task["revision"]
            or shard["manifest_sha256"] != task["manifest_sha256"]
            or shard["rule_sha256"] != task["rule_sha256"]
        ):
            raise Conflict("任务输入清单或处理规则已变化，完成记录被拒绝")

    def _unlock_downstream(self, task_id: str, shard_key: str, now: str) -> list[str]:
        """与完成记录同事务原子解锁下游分片。"""

        self.connection.execute(
            "UPDATE data_shards SET pending_deps=pending_deps-1,updated_at=? "
            "WHERE task_id=? AND state='pending' AND shard_key IN "
            "(SELECT shard_key FROM data_shard_deps WHERE task_id=? AND depends_on=?)",
            (now, task_id, task_id, shard_key),
        )
        self.connection.execute(
            "UPDATE data_shards SET state='ready',available_at=?,updated_at=? "
            "WHERE task_id=? AND state='pending' AND pending_deps=0 AND shard_key IN "
            "(SELECT shard_key FROM data_shard_deps WHERE task_id=? AND depends_on=?)",
            (now, now, task_id, task_id, shard_key),
        )
        rows = self.connection.execute(
            "SELECT shard_key FROM data_shards WHERE task_id=? AND state='ready' AND shard_key IN "
            "(SELECT shard_key FROM data_shard_deps WHERE task_id=? AND depends_on=?) ORDER BY shard_key",
            (task_id, task_id, shard_key),
        ).fetchall()
        return [row["shard_key"] for row in rows]

    def _maybe_complete_task(self, task_id: str, now: str) -> bool:
        remaining = self.connection.execute(
            "SELECT count(*) FROM data_shards WHERE task_id=? AND state<>'succeeded'", (task_id,)
        ).fetchone()[0]
        if remaining:
            return False
        cursor = self.connection.execute(
            "UPDATE data_tasks SET state='completed',completed_at=? WHERE task_id=? AND state='running'",
            (now, task_id),
        )
        return cursor.rowcount == 1

    def complete_shard(
        self,
        worker_id: str,
        task_id: str,
        shard_key: str,
        lease_seq: int,
        output_sha256: str,
        result: object = None,
    ) -> dict[str, Any]:
        output = str(output_sha256 or "").strip().lower()
        if not _HEX64.match(output):
            raise ValidationFailed("输出校验值必须是 64 位十六进制 SHA-256")
        if result is not None and not isinstance(result, Mapping):
            raise ValidationFailed("分片结果必须是 JSON 对象")
        now = self._now()
        with transaction(self.connection, immediate=True):
            shard = self._shard_row(task_id, shard_key)
            self._check_lease(shard, worker_id, lease_seq, now)
            task = self._task_row(task_id)
            self._check_definition_unchanged(task, shard)
            cursor = self.connection.execute(
                "UPDATE data_shards SET state='succeeded',output_sha256=?,lease_owner=NULL,"
                "lease_expires_at=NULL,last_error=NULL,updated_at=? "
                "WHERE task_id=? AND shard_key=? AND state='leased' AND lease_owner=? AND lease_seq=? "
                "AND lease_expires_at>?",
                (output, now, task_id, shard_key, worker_id, int(lease_seq), now),
            )
            if cursor.rowcount != 1:
                raise InvalidState("分片租约已易主，迟到结果不能覆盖新持有者")
            cursor = self.connection.execute(
                "INSERT INTO data_completions(task_id,shard_key,task_revision,attempt,lease_seq,worker_id,"
                "manifest_sha256,rule_version,rule_sha256,input_sha256,output_sha256,result_json,completed_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id, shard_key, shard["task_revision"], shard["attempts"], shard["lease_seq"],
                    worker_id, shard["manifest_sha256"], task["rule_version"], shard["rule_sha256"],
                    shard["input_sha256"], output, canonical_json(result or {}), now,
                ),
            )
            completion_id = cursor.lastrowid
            unlocked = self._unlock_downstream(task_id, shard_key, now)
            task_completed = self._maybe_complete_task(task_id, now)
            self._audit("shard", f"{task_id}/{shard_key}", "shard.completed", worker_id, {
                "completion_id": completion_id,
                "attempt": shard["attempts"],
                "output_sha256": output,
                "unlocked": unlocked,
            })
            if task_completed:
                self._audit("task", task_id, "task.completed", worker_id, {"revision": task["revision"]})
        return {
            "completion_id": completion_id,
            "task_id": task_id,
            "shard_key": shard_key,
            "output_sha256": output,
            "unlocked": unlocked,
            "task_completed": task_completed,
        }

    def fail_shard(
        self, worker_id: str, task_id: str, shard_key: str, lease_seq: int, error: str
    ) -> dict[str, Any]:
        message = str(error or "").strip()
        if not message:
            raise ValidationFailed("失败原因不能为空")
        now = self._now()
        with transaction(self.connection, immediate=True):
            shard = self._shard_row(task_id, shard_key)
            self._check_lease(shard, worker_id, lease_seq, now)
            task = self._task_row(task_id)
            attempts = shard["attempts"]
            if attempts >= task["max_attempts"]:
                new_state = "manual"
                available_at = shard["available_at"]
            else:
                new_state = "failed"
                delay = task["retry_delay_seconds"] * (
                    task["retry_backoff_multiplier"] ** max(attempts - 1, 0)
                )
                available_at = utc_text(self.clock.now() + timedelta(seconds=delay))
            cursor = self.connection.execute(
                "UPDATE data_shards SET state=?,available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=?,updated_at=? "
                "WHERE task_id=? AND shard_key=? AND state='leased' AND lease_owner=? AND lease_seq=?",
                (new_state, available_at, message[:1000], now, task_id, shard_key, worker_id, int(lease_seq)),
            )
            if cursor.rowcount != 1:
                raise InvalidState("分片租约已易主，迟到结果不能覆盖新持有者")
            event_type = "shard.exhausted" if new_state == "manual" else "shard.failed"
            self._audit("shard", f"{task_id}/{shard_key}", event_type, worker_id, {
                "attempt": attempts,
                "max_attempts": task["max_attempts"],
                "error": message[:1000],
                "available_at": available_at,
            })
        return {
            "task_id": task_id,
            "shard_key": shard_key,
            "state": new_state,
            "attempts": attempts,
            "available_at": available_at,
        }

    def resolve_shard(
        self,
        actor_id: str,
        task_id: str,
        shard_key: str,
        action: str,
        output_sha256: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        """人工处置：重新排队（retry）或人工登记完成（complete）。"""

        self._require(actor_id, "shard.resolve")
        if action not in {"retry", "complete"}:
            raise ValidationFailed("人工处置动作必须是 retry 或 complete")
        now = self._now()
        with transaction(self.connection, immediate=True):
            shard = self._shard_row(task_id, shard_key)
            if shard["state"] != "manual":
                raise InvalidState("只有进入人工处置的分片可以执行该操作")
            task = self._task_row(task_id)
            if action == "retry":
                new_state = "ready" if shard["pending_deps"] == 0 else "pending"
                self.connection.execute(
                    "UPDATE data_shards SET state=?,attempts=0,available_at=?,lease_owner=NULL,"
                    "lease_expires_at=NULL,last_error=NULL,updated_at=? WHERE task_id=? AND shard_key=?",
                    (new_state, now, now, task_id, shard_key),
                )
                self._audit("shard", f"{task_id}/{shard_key}", "shard.requeued", actor_id, {
                    "note": str(note or ""), "state": new_state,
                })
                return {"task_id": task_id, "shard_key": shard_key, "state": new_state, "attempts": 0}
            output = str(output_sha256 or "").strip().lower()
            if not _HEX64.match(output):
                raise ValidationFailed("人工登记完成必须提供 64 位十六进制输出校验值")
            self.connection.execute(
                "UPDATE data_shards SET state='succeeded',output_sha256=?,lease_owner=NULL,"
                "lease_expires_at=NULL,last_error=NULL,updated_at=? WHERE task_id=? AND shard_key=?",
                (output, now, task_id, shard_key),
            )
            cursor = self.connection.execute(
                "INSERT INTO data_completions(task_id,shard_key,task_revision,attempt,lease_seq,worker_id,"
                "manifest_sha256,rule_version,rule_sha256,input_sha256,output_sha256,result_json,completed_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id, shard_key, shard["task_revision"], shard["attempts"], shard["lease_seq"],
                    f"manual:{actor_id}", shard["manifest_sha256"], task["rule_version"],
                    shard["rule_sha256"], shard["input_sha256"], output,
                    canonical_json({"manual": True, "note": str(note or "")}), now,
                ),
            )
            completion_id = cursor.lastrowid
            unlocked = self._unlock_downstream(task_id, shard_key, now)
            task_completed = self._maybe_complete_task(task_id, now)
            self._audit("shard", f"{task_id}/{shard_key}", "shard.manual_completed", actor_id, {
                "completion_id": completion_id,
                "output_sha256": output,
                "note": str(note or ""),
                "unlocked": unlocked,
            })
            if task_completed:
                self._audit("task", task_id, "task.completed", actor_id, {"revision": task["revision"]})
        return {
            "completion_id": completion_id,
            "task_id": task_id,
            "shard_key": shard_key,
            "state": "succeeded",
            "output_sha256": output,
            "unlocked": unlocked,
            "task_completed": task_completed,
        }

    # ------------------------------------------------------------------
    # 状态重建与来源解释
    # ------------------------------------------------------------------

    def _state_counts(self, task_id: str) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT state,count(*) AS n FROM data_shards WHERE task_id=? GROUP BY state", (task_id,)
        ).fetchall()
        counts = {state: 0 for state in STATE_GROUPS}
        for row in rows:
            counts[row["state"]] = row["n"]
        return counts

    @staticmethod
    def _task_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "task_id": row["task_id"],
            "name": row["name"],
            "state": row["state"],
            "revision": row["revision"],
            "rule_version": row["rule_version"],
            "rule_params": json.loads(row["rule_params_json"]),
            "rule_sha256": row["rule_sha256"],
            "manifest": json.loads(row["manifest_json"]),
            "manifest_sha256": row["manifest_sha256"],
            "retry_policy": {
                "max_attempts": row["max_attempts"],
                "retry_delay_seconds": row["retry_delay_seconds"],
                "backoff_multiplier": row["retry_backoff_multiplier"],
            },
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "cancelled_at": row["cancelled_at"],
            "completed_at": row["completed_at"],
        }

    def _shard_view(self, row: sqlite3.Row) -> dict[str, Any]:
        view = {
            "task_id": row["task_id"],
            "shard_key": row["shard_key"],
            "state": row["state"],
            "state_group": STATE_GROUPS[row["state"]],
            "task_revision": row["task_revision"],
            "input_refs": json.loads(row["input_refs_json"]),
            "input_sha256": row["input_sha256"],
            "depends_on": json.loads(row["depends_on_json"]),
            "pending_deps": row["pending_deps"],
            "attempts": row["attempts"],
            "lease_owner": row["lease_owner"],
            "lease_seq": row["lease_seq"],
            "lease_expires_at": row["lease_expires_at"],
            "available_at": row["available_at"],
            "output_sha256": row["output_sha256"],
            "last_error": row["last_error"],
            "updated_at": row["updated_at"],
        }
        if row["state"] == "leased":
            view["lease_expired"] = row["lease_expires_at"] <= self._now()
        return view

    def get_task(self, actor_id: str, task_id: str) -> dict[str, Any]:
        """重建任务的待处理、运行中、失败和完成状态计数。"""

        self._require(actor_id, "task.read")
        task = self._task_row(task_id)
        states = self._state_counts(task_id)
        groups = {"pending": 0, "running": 0, "failed": 0, "completed": 0}
        for state, count in states.items():
            groups[STATE_GROUPS[state]] += count
        return {
            "task": self._task_view(task),
            "counts": groups,
            "states": states,
            "total_shards": sum(states.values()),
        }

    def list_shards(self, actor_id: str, task_id: str) -> dict[str, Any]:
        self._require(actor_id, "task.read")
        self._task_row(task_id)
        rows = self.connection.execute(
            "SELECT * FROM data_shards WHERE task_id=? ORDER BY shard_key", (task_id,)
        ).fetchall()
        return {"task_id": task_id, "shards": [self._shard_view(row) for row in rows]}

    def explain_shard(self, actor_id: str, task_id: str, shard_key: str) -> dict[str, Any]:
        """解释分片来源：定义出处、输入摘要、依赖状态、租约与完成证据。"""

        self._require(actor_id, "task.read")
        task = self._task_row(task_id)
        shard = self._shard_row(task_id, shard_key)
        view = self._shard_view(shard)
        dependencies = []
        for dep in view["depends_on"]:
            dep_row = self._shard_row(task_id, dep)
            dependencies.append({"shard_key": dep, "state": dep_row["state"], "output_sha256": dep_row["output_sha256"]})
        dependents = [
            row["shard_key"]
            for row in self.connection.execute(
                "SELECT shard_key FROM data_shard_deps WHERE task_id=? AND depends_on=? ORDER BY shard_key",
                (task_id, shard_key),
            ).fetchall()
        ]
        completions = [
            {
                "completion_id": row["completion_id"],
                "task_revision": row["task_revision"],
                "attempt": row["attempt"],
                "worker_id": row["worker_id"],
                "manifest_sha256": row["manifest_sha256"],
                "rule_version": row["rule_version"],
                "input_sha256": row["input_sha256"],
                "output_sha256": row["output_sha256"],
                "result": json.loads(row["result_json"]),
                "completed_at": row["completed_at"],
            }
            for row in self.connection.execute(
                "SELECT * FROM data_completions WHERE task_id=? AND shard_key=? ORDER BY completion_id",
                (task_id, shard_key),
            ).fetchall()
        ]
        events = [
            {
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in self.connection.execute(
                "SELECT event_type,actor_id,payload_json,created_at FROM data_audit_events "
                "WHERE entity_type='shard' AND entity_id=? ORDER BY event_id",
                (f"{task_id}/{shard_key}",),
            ).fetchall()
        ]
        return {
            "task_id": task_id,
            "shard_key": shard_key,
            "shard": view,
            "origin": {
                "task_revision": shard["task_revision"],
                "task_state": task["state"],
                "input_refs": view["input_refs"],
                "input_sha256": shard["input_sha256"],
                "manifest_sha256": shard["manifest_sha256"],
                "rule_version": task["rule_version"],
                "rule_sha256": shard["rule_sha256"],
            },
            "dependencies": dependencies,
            "dependents": dependents,
            "completions": completions,
            "events": events,
            "explanation": self._explain_text(view, dependencies, completions),
        }

    @staticmethod
    def _explain_text(
        view: Mapping[str, Any],
        dependencies: Sequence[Mapping[str, Any]],
        completions: Sequence[Mapping[str, Any]],
    ) -> str:
        origin = (
            f"分片来自任务第 {view['task_revision']} 版定义，"
            f"引用 {len(view['input_refs'])} 个清单输入对象（输入摘要 {view['input_sha256'][:12]}…）"
        )
        state = view["state"]
        if state == "pending":
            waiting = [dep["shard_key"] for dep in dependencies if dep["state"] != "succeeded"]
            return f"{origin}；当前等待 {view['pending_deps']} 个上游分片完成：{', '.join(waiting)}"
        if state == "ready":
            return f"{origin}；上游依赖已全部完成，等待工作进程领取"
        if state == "leased":
            text = (
                f"{origin}；正由工作进程 {view['lease_owner']} 持有"
                f"（第 {view['attempts']} 次尝试，租约序号 {view['lease_seq']}，{view['lease_expires_at']} 到期）"
            )
            if view.get("lease_expired"):
                text += "；租约已过期，其他进程可接管"
            return text
        if state == "failed":
            return (
                f"{origin}；第 {view['attempts']} 次尝试失败：{view['last_error']}；"
                f"{view['available_at']} 起可被重新领取"
            )
        if state == "manual":
            return f"{origin}；已连续失败 {view['attempts']} 次达到重试上限，等待人工处置"
        latest = completions[-1]
        return (
            f"{origin}；由 {latest['worker_id']} 于 {latest['completed_at']} 完成"
            f"（第 {latest['attempt']} 次尝试），输出校验值 {latest['output_sha256']}，"
            f"证据见完成记录 #{latest['completion_id']}"
        )

    def audit_trail(self, actor_id: str, task_id: str | None = None) -> dict[str, Any]:
        """返回审计事件并校验哈希链，用于核对已完成证据未被篡改。"""

        self._require(actor_id, "audit.read")
        rows = self.connection.execute(
            "SELECT * FROM data_audit_events ORDER BY event_id"
        ).fetchall()
        previous_hash = "0" * 64
        chain_valid = True
        events = []
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            digest = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or digest != row["event_hash"]:
                chain_valid = False
            previous_hash = row["event_hash"]
            if task_id is not None and not (
                row["entity_id"] == task_id or row["entity_id"].startswith(f"{task_id}/")
            ):
                continue
            events.append({
                "event_id": row["event_id"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": body["payload"],
                "created_at": row["created_at"],
            })
        return {"chain_valid": chain_valid, "events": events}
