"""数据加工任务断点恢复的领域用例。

关键并发与恢复规则：

- ``claim_shard`` 在单个 ``BEGIN IMMEDIATE`` 事务内选取可领取分片
  （依赖已解锁的等待分片，或租约已过期的运行分片），并递增 fencing 令牌；
- ``complete_shard`` 必须同时通过持有者与 fencing 校验，迟到结果会被
  :class:`StaleLease` 拒绝；完成、输出登记与后续分片解锁在同一事务提交；
- 完成前重新核对输入摘要与规则摘要，任何漂移以 :class:`InputChanged` 拒绝；
- 失败按任务上的 :class:`RetryPolicy` 指数退避重试，耗尽后进入 ``manual``
  人工处置；
- 取消只阻止新领取并取消未开始分片，已完成分片的输出证据原样保留；
- :meth:`PipelineService.job_status` 全部从持久化行派生四态并解释分片来源，
  因此进程重启后状态可完整重建。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from typing import Any, Mapping, Sequence

from .clock import SystemClock, utc_text
from .errors import Conflict, InputChanged, InvalidState, NotFound, StaleLease, ValidationFailed
from .hashing import canonical_json, content_digest, digest_value
from .models import RetryPolicy
from .storage import initialize, transaction


_HEX = set("0123456789abcdef")
_TERMINAL_SHARD_STATES = ("succeeded", "manual", "cancelled")
_RUNTIME_STATES = ("pending", "running", "failed", "done", "cancelled")


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in _HEX for char in value.lower())


class PipelineService:
    """在单个 SQLite 连接上提供分片加工流水线操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    # ------------------------------------------------------------------ 基础

    def _now_text(self) -> str:
        return utc_text(self.clock.now())

    def _event(
        self, job_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any],
        shard_key: str | None = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO pipeline_events(job_id,shard_key,event_type,actor_id,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (job_id, shard_key, event_type, actor_id, canonical_json(payload), self._now_text()),
        )

    def _job_row(self, job_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM pipeline_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFound(f"加工任务不存在: {job_id}")
        return row

    def _shard_row(self, job_id: str, shard_key: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM pipeline_shards WHERE job_id=? AND shard_key=?", (job_id, shard_key)
        ).fetchone()
        if row is None:
            raise NotFound(f"分片不存在: {job_id}/{shard_key}")
        return row

    def _policy(self, job: sqlite3.Row) -> RetryPolicy:
        return RetryPolicy(
            max_attempts=job["retry_max_attempts"],
            backoff_base_seconds=job["retry_backoff_base_seconds"],
            backoff_max_seconds=job["retry_backoff_max_seconds"],
        )

    def _validate_topology(self, shards: Sequence[Mapping[str, Any]]) -> None:
        keys: set[str] = set()
        for index, shard in enumerate(shards):
            if not isinstance(shard, Mapping):
                raise ValidationFailed(f"第 {index} 个分片必须是对象")
            key = shard.get("shard_key")
            if not isinstance(key, str) or not key.strip():
                raise ValidationFailed(f"第 {index} 个分片缺少 shard_key")
            if key in keys:
                raise ValidationFailed(f"分片键重复: {key}")
            keys.add(key)
            depends = shard.get("depends_on", [])
            if not isinstance(depends, list) or not all(isinstance(item, str) for item in depends):
                raise ValidationFailed(f"分片 {key} 的 depends_on 必须是字符串数组")
            if "input" not in shard:
                raise ValidationFailed(f"分片 {key} 缺少 input 输入说明")
        adjacency: dict[str, set[str]] = {key: set() for key in keys}
        for shard in shards:
            for dep in shard.get("depends_on", []):
                if dep not in keys:
                    raise ValidationFailed(f"分片 {shard['shard_key']} 依赖了不存在的分片 {dep}")
                if dep == shard["shard_key"]:
                    raise ValidationFailed(f"分片 {shard['shard_key']} 不能依赖自身")
                adjacency[shard["shard_key"]].add(dep)
        # Kahn 拓扑同时检测环。
        remaining = {key: set(deps) for key, deps in adjacency.items()}
        while remaining:
            ready = sorted(key for key, deps in remaining.items() if not deps)
            if not ready:
                raise ValidationFailed("分片依赖存在循环")
            for key in ready:
                remaining.pop(key)
                for deps in remaining.values():
                    deps.discard(key)

    # ---------------------------------------------------------------- 建任务

    def create_job(
        self,
        actor_id: str,
        job_id: str,
        rule: Mapping[str, Any],
        shards: Sequence[Mapping[str, Any]],
        manifest: Sequence[object] | None = None,
        retry: Mapping[str, Any] | RetryPolicy | None = None,
    ) -> dict[str, Any]:
        """登记加工任务：保存清单摘要、规则版本与分片依赖。"""

        if not isinstance(job_id, str) or not job_id.strip():
            raise ValidationFailed("job_id 不能为空")
        rule_id = rule.get("rule_id") if isinstance(rule, Mapping) else None
        if not isinstance(rule_id, str) or not rule_id.strip():
            raise ValidationFailed("规则缺少 rule_id")
        version = rule.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
            raise ValidationFailed("规则版本必须是正整数")
        if "definition" not in rule:
            raise ValidationFailed("规则缺少 definition 正文")
        rule_sha = digest_value(rule["definition"])
        if not isinstance(rule_id, str):
            raise ValidationFailed("rule_id 必须是字符串")
        if manifest is None:
            manifest = []
        if not isinstance(manifest, Sequence) or isinstance(manifest, (str, bytes)):
            raise ValidationFailed("输入清单必须是数组")
        if not isinstance(shards, Sequence) or isinstance(shards, str) or not shards:
            raise ValidationFailed("分片数组不能为空")
        self._validate_topology(shards)
        policy = retry if isinstance(retry, RetryPolicy) else RetryPolicy.from_raw(retry)
        manifest_sha = content_digest(manifest)
        now = self._now_text()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO pipeline_jobs(job_id,state,rule_id,rule_version,rule_sha256,"
                    "manifest_json,manifest_sha256,retry_max_attempts,retry_backoff_base_seconds,"
                    "retry_backoff_max_seconds,created_by,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        job_id.strip(), "active", rule_id.strip(), version, rule_sha,
                        canonical_json(list(manifest)), manifest_sha, policy.max_attempts,
                        policy.backoff_base_seconds, policy.backoff_max_seconds, actor_id, now, now,
                    ),
                )
                for ordinal, shard in enumerate(shards):
                    key = shard["shard_key"]
                    depends = list(shard.get("depends_on", ()))
                    input_sha = digest_value(shard["input"])
                    state = "blocked" if depends else "waiting"
                    self.connection.execute(
                        "INSERT INTO pipeline_shards(job_id,shard_key,ordinal,state,input_spec_json,"
                        "input_sha256,rule_sha256,dependencies_json,available_at,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            job_id.strip(), key, ordinal, state, canonical_json(shard["input"]),
                            input_sha, rule_sha, canonical_json(depends), now, now, now,
                        ),
                    )
                    for edge_ordinal, dep in enumerate(depends):
                        self.connection.execute(
                            "INSERT INTO shard_dependencies(job_id,shard_key,depends_on_key,ordinal) "
                            "VALUES(?,?,?,?)",
                            (job_id.strip(), key, dep, edge_ordinal),
                        )
                self._event(job_id.strip(), "job.created", actor_id, {
                    "rule_id": rule_id.strip(),
                    "rule_version": version,
                    "rule_sha256": rule_sha,
                    "manifest_sha256": manifest_sha,
                    "shard_count": len(shards),
                    "retry": {
                        "max_attempts": policy.max_attempts,
                        "backoff_base_seconds": policy.backoff_base_seconds,
                        "backoff_max_seconds": policy.backoff_max_seconds,
                    },
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"加工任务已存在或依赖数据冲突: {job_id}") from exc
        return self.get_job(job_id.strip())

    # ------------------------------------------------------------------ 领取

    def claim_shard(
        self, worker_id: str, lease_seconds: int = 60, job_id: str | None = None
    ) -> dict[str, Any] | None:
        """领取一个可执行分片；可指定任务，租约到期后其他进程可接管。"""

        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValidationFailed("worker_id 不能为空")
        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now_text()
        expires = utc_text(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            if job_id is None:
                row = self.connection.execute(
                    "SELECT s.* FROM pipeline_shards s JOIN pipeline_jobs j ON j.job_id=s.job_id "
                    "WHERE j.state='active' AND (("
                    "s.state='waiting' AND s.available_at<=?) OR "
                    "(s.state='leased' AND s.lease_expires_at<=?)) "
                    "ORDER BY s.available_at,s.ordinal,s.shard_id LIMIT 1",
                    (now, now),
                ).fetchone()
            else:
                self._job_row(job_id)
                row = self.connection.execute(
                    "SELECT s.* FROM pipeline_shards s JOIN pipeline_jobs j ON j.job_id=s.job_id "
                    "WHERE j.state='active' AND s.job_id=? AND (("
                    "s.state='waiting' AND s.available_at<=?) OR "
                    "(s.state='leased' AND s.lease_expires_at<=?)) "
                    "ORDER BY s.available_at,s.ordinal,s.shard_id LIMIT 1",
                    (job_id, now, now),
                ).fetchone()
            if row is None:
                return None
            # 接管过期租约：把原持有者仍在运行的尝试标记为过期，保留其来源证据。
            self.connection.execute(
                "UPDATE shard_attempts SET finished_at=?,outcome='expired' "
                "WHERE shard_id=? AND outcome='running'",
                (now, row["shard_id"]),
            )
            self.connection.execute(
                "UPDATE pipeline_shards SET state='leased',attempts=attempts+1,"
                "lease_owner=?,lease_expires_at=?,lease_fence=lease_fence+1,available_at=?,"
                "last_error=NULL,updated_at=? WHERE shard_id=?",
                (worker_id.strip(), expires, now, now, row["shard_id"]),
            )
            claimed = self.connection.execute(
                "SELECT * FROM pipeline_shards WHERE shard_id=?", (row["shard_id"],)
            ).fetchone()
            self.connection.execute(
                "INSERT INTO shard_attempts(job_id,shard_id,attempt_no,fence,worker_id,"
                "started_at,outcome) VALUES(?,?,?,?,?,?, 'running')",
                (claimed["job_id"], claimed["shard_id"], claimed["attempts"],
                 claimed["lease_fence"], worker_id.strip(), now),
            )
            self._event(claimed["job_id"], "shard.claimed", worker_id.strip(), {
                "shard_key": claimed["shard_key"],
                "attempt": claimed["attempts"],
                "fence": claimed["lease_fence"],
                "lease_expires_at": expires,
                "took_over": row["state"] == "leased",
            }, shard_key=claimed["shard_key"])
            job = self._job_row(claimed["job_id"])
        return {
            "job_id": claimed["job_id"],
            "shard_key": claimed["shard_key"],
            "ordinal": claimed["ordinal"],
            "attempt": claimed["attempts"],
            "fence": claimed["lease_fence"],
            "lease_owner": claimed["lease_owner"],
            "lease_expires_at": claimed["lease_expires_at"],
            "input": json.loads(claimed["input_spec_json"]),
            "input_sha256": claimed["input_sha256"],
            "depends_on": json.loads(claimed["dependencies_json"]),
            "rule_id": job["rule_id"],
            "rule_version": job["rule_version"],
            "rule_sha256": job["rule_sha256"],
            "manifest_sha256": job["manifest_sha256"],
        }

    def renew_lease(
        self, worker_id: str, job_id: str, shard_key: str, fence: int, lease_seconds: int
    ) -> dict[str, Any]:
        """工作进程续租；持有者或 fencing 令牌不符时拒绝。"""

        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now_text()
        expires = utc_text(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            shard = self._shard_row(job_id, shard_key)
            if shard["state"] != "leased" or shard["lease_owner"] != worker_id or shard["lease_fence"] != fence:
                raise StaleLease("租约已不属于当前工作进程，不能续租")
            if shard["lease_expires_at"] <= now:
                raise StaleLease("租约已经过期，不能续租，等待其他进程接管")
            self.connection.execute(
                "UPDATE pipeline_shards SET lease_expires_at=?,updated_at=? WHERE shard_id=?",
                (expires, now, shard["shard_id"]),
            )
        return {"job_id": job_id, "shard_key": shard_key, "fence": fence, "lease_expires_at": expires}

    def _check_holder(self, shard: sqlite3.Row, worker_id: str, fence: int) -> None:
        """fencing 校验：持有者、令牌不符或租约已过期的回调都不得写入结果。

        过期后若无人接管，旧持有者的迟到结果同样拒绝——分片留给下一次领取重做，
        避免"事实上的无期限租约"；长任务应通过 :meth:`renew_lease` 续租。
        """

        if shard["state"] != "leased" or shard["lease_owner"] != worker_id or shard["lease_fence"] != fence:
            raise StaleLease(
                f"迟到结果被拒绝：分片 {shard['job_id']}/{shard['shard_key']} "
                f"当前不属于 {worker_id}（fence={fence}）"
            )
        if shard["lease_expires_at"] <= self._now_text():
            raise StaleLease(
                f"迟到结果被拒绝：分片 {shard['job_id']}/{shard['shard_key']} 的租约已于 "
                f"{shard['lease_expires_at']} 过期"
            )

    def _unblocked_successors(self, job_id: str, completed_key: str) -> list[str]:
        """找出因本次完成而全部依赖就绪的阻塞分片。"""

        candidates = self.connection.execute(
            "SELECT DISTINCT s.shard_key FROM pipeline_shards s "
            "JOIN shard_dependencies d ON d.job_id=s.job_id AND d.shard_key=s.shard_key "
            "WHERE d.job_id=? AND d.depends_on_key=? AND s.state='blocked'",
            (job_id, completed_key),
        ).fetchall()
        unlocked: list[str] = []
        now = self._now_text()
        for candidate in candidates:
            key = candidate["shard_key"]
            unsatisfied = self.connection.execute(
                "SELECT count(*) FROM shard_dependencies d "
                "JOIN pipeline_shards p ON p.job_id=d.job_id AND p.shard_key=d.depends_on_key "
                "WHERE d.job_id=? AND d.shard_key=? AND p.state!='succeeded'",
                (job_id, key),
            ).fetchone()[0]
            if unsatisfied == 0:
                self.connection.execute(
                    "UPDATE pipeline_shards SET state='waiting',available_at=?,updated_at=? "
                    "WHERE job_id=? AND shard_key=? AND state='blocked'",
                    (now, now, job_id, key),
                )
                unlocked.append(key)
        return unlocked

    def _maybe_complete_job(self, job_id: str) -> None:
        active_count = self.connection.execute(
            "SELECT count(*) FROM pipeline_shards "
            "WHERE job_id=? AND state NOT IN ('succeeded','manual','cancelled')",
            (job_id,),
        ).fetchone()[0]
        if active_count:
            return
        now = self._now_text()
        job = self._job_row(job_id)
        if job["state"] == "active":
            unfinished = self.connection.execute(
                "SELECT count(*) FROM pipeline_shards WHERE job_id=? AND state!='succeeded'",
                (job_id,),
            ).fetchone()[0]
            if unfinished == 0:
                self.connection.execute(
                    "UPDATE pipeline_jobs SET state='completed',completed_at=?,updated_at=? WHERE job_id=?",
                    (now, now, job_id),
                )
                return
        if job["completed_at"] is None:
            # 取消后的任务：所有分片均有终态时记录收束时间，但保留 cancelled 状态。
            self.connection.execute(
                "UPDATE pipeline_jobs SET completed_at=?,updated_at=? WHERE job_id=?",
                (now, now, job_id),
            )

    # ------------------------------------------------------------------ 完成

    def complete_shard(
        self,
        worker_id: str,
        job_id: str,
        shard_key: str,
        fence: int,
        output: Mapping[str, Any],
        *,
        input_sha256: str,
        rule_sha256: str,
        result: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记完成结果；校验输入/规则未漂移，并原子解锁后续分片。"""

        if not isinstance(output, Mapping) or not _is_sha256(output.get("sha256")):
            raise ValidationFailed("输出必须包含 64 位十六进制 sha256 校验值")
        location = output.get("location")
        if location is not None and not isinstance(location, str):
            raise ValidationFailed("输出 location 必须是字符串")
        now = self._now_text()
        with transaction(self.connection, immediate=True):
            job = self._job_row(job_id)
            shard = self._shard_row(job_id, shard_key)
            self._check_holder(shard, worker_id, fence)
            # 输入与规则未变化校验：worker 回报的摘要必须与领取时冻结的摘要一致，
            # 同时重算本地输入说明摘要以发现存储层篡改。
            if input_sha256 != shard["input_sha256"] or rule_sha256 != shard["rule_sha256"]:
                raise InputChanged(
                    f"分片 {job_id}/{shard_key} 完成时输入或规则摘要与领取时不一致"
                )
            if digest_value(json.loads(shard["input_spec_json"])) != shard["input_sha256"]:
                raise InputChanged(f"分片 {job_id}/{shard_key} 的本地输入说明摘要校验失败")
            output_sha = output["sha256"].lower()
            self.connection.execute(
                "UPDATE pipeline_shards SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,"
                "output_sha256=?,output_location=?,result_json=?,last_error=NULL,completed_at=?,"
                "updated_at=? WHERE shard_id=?",
                (
                    output_sha, location,
                    None if result is None else canonical_json(result),
                    now, now, shard["shard_id"],
                ),
            )
            self.connection.execute(
                "UPDATE shard_attempts SET finished_at=?,outcome='succeeded',output_sha256=? "
                "WHERE shard_id=? AND attempt_no=? AND outcome='running'",
                (now, output_sha, shard["shard_id"], shard["attempts"]),
            )
            self._event(job_id, "shard.completed", worker_id, {
                "shard_key": shard_key,
                "fence": fence,
                "attempt": shard["attempts"],
                "output_sha256": output_sha,
                "output_location": location,
                "input_sha256": input_sha256,
                "rule_sha256": rule_sha256,
            }, shard_key=shard_key)
            unlocked: list[str] = []
            if job["state"] == "active":
                unlocked = self._unblocked_successors(job_id, shard_key)
                for key in unlocked:
                    self._event(job_id, "shard.unlocked", worker_id, {
                        "shard_key": key, "after": shard_key,
                    }, shard_key=key)
            self._maybe_complete_job(job_id)
        return {
            "job_id": job_id,
            "shard_key": shard_key,
            "state": "succeeded",
            "fence": fence,
            "output_sha256": output_sha,
            "unlocked": unlocked,
        }

    # ------------------------------------------------------------------ 失败

    def fail_shard(
        self,
        worker_id: str,
        job_id: str,
        shard_key: str,
        fence: int,
        error: str,
        retry_seconds: int | None = None,
    ) -> dict[str, Any]:
        """登记一次失败；按可配置重试策略退避重试，耗尽后转人工处置。"""

        if not isinstance(error, str) or not error.strip():
            raise ValidationFailed("失败原因不能为空")
        if retry_seconds is not None and retry_seconds < 0:
            raise ValidationFailed("重试等待不能为负数")
        now = self._now_text()
        with transaction(self.connection, immediate=True):
            job = self._job_row(job_id)
            shard = self._shard_row(job_id, shard_key)
            self._check_holder(shard, worker_id, fence)
            message = error.strip()[:1000]
            self.connection.execute(
                "UPDATE shard_attempts SET finished_at=?,outcome='failed',error=? "
                "WHERE shard_id=? AND attempt_no=? AND outcome='running'",
                (now, message, shard["shard_id"], shard["attempts"]),
            )
            # 已取消任务上的迟到失败：保留证据但绝不重新排队。
            if job["state"] != "active":
                self.connection.execute(
                    "UPDATE pipeline_shards SET state='cancelled',lease_owner=NULL,lease_expires_at=NULL,"
                    "last_error=?,updated_at=? WHERE shard_id=?",
                    (message, now, shard["shard_id"]),
                )
                self._event(job_id, "shard.cancelled_after_fail", worker_id, {
                    "shard_key": shard_key, "error": message,
                }, shard_key=shard_key)
                self._maybe_complete_job(job_id)
                return {"job_id": job_id, "shard_key": shard_key, "state": "cancelled"}
            policy = self._policy(job)
            attempts_used = shard["attempts"]
            if attempts_used >= policy.max_attempts:
                manual_reason = f"已重试 {attempts_used} 次仍失败：{message}"
                self.connection.execute(
                    "UPDATE pipeline_shards SET state='manual',lease_owner=NULL,lease_expires_at=NULL,"
                    "last_error=?,manual_reason=?,updated_at=? WHERE shard_id=?",
                    (message, manual_reason, now, shard["shard_id"]),
                )
                self._event(job_id, "shard.manual", worker_id, {
                    "shard_key": shard_key,
                    "attempts": attempts_used,
                    "max_attempts": policy.max_attempts,
                    "error": message,
                }, shard_key=shard_key)
                state = "manual"
                available_at = None
            else:
                delay = policy.delay_seconds(attempts_used) if retry_seconds is None else retry_seconds
                available_at = utc_text(self.clock.now() + timedelta(seconds=delay))
                self.connection.execute(
                    "UPDATE pipeline_shards SET state='waiting',lease_owner=NULL,lease_expires_at=NULL,"
                    "last_error=?,manual_reason=NULL,available_at=?,updated_at=? WHERE shard_id=?",
                    (message, available_at, now, shard["shard_id"]),
                )
                self._event(job_id, "shard.retry_scheduled", worker_id, {
                    "shard_key": shard_key,
                    "attempts": attempts_used,
                    "max_attempts": policy.max_attempts,
                    "retry_after_seconds": delay,
                    "available_at": available_at,
                    "error": message,
                }, shard_key=shard_key)
                state = "waiting"
            self._maybe_complete_job(job_id)
        response: dict[str, Any] = {
            "job_id": job_id, "shard_key": shard_key, "state": state,
            "attempts": attempts_used, "max_attempts": policy.max_attempts,
        }
        if available_at is not None:
            response["available_at"] = available_at
        return response

    # ------------------------------------------------------------------ 取消

    def cancel_job(self, actor_id: str, job_id: str, reason: str) -> dict[str, Any]:
        """取消任务：阻止新领取，未开始分片转取消，已完成证据全部保留。"""

        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("取消原因不能为空")
        now = self._now_text()
        with transaction(self.connection, immediate=True):
            job = self._job_row(job_id)
            if job["state"] != "active":
                raise InvalidState("只有进行中的任务可以取消")
            self.connection.execute(
                "UPDATE pipeline_jobs SET state='cancelled',cancelled_at=?,cancel_reason=?,updated_at=? "
                "WHERE job_id=?",
                (now, reason.strip(), now, job_id),
            )
            cancelled = self.connection.execute(
                "UPDATE pipeline_shards SET state='cancelled',updated_at=? "
                "WHERE job_id=? AND state IN ('blocked','waiting')",
                (now, job_id),
            ).rowcount
            # 租约恰好已过期的运行分片不可能再被领取（领取只看 active 任务），立即收束。
            expired_rows = self.connection.execute(
                "SELECT shard_key FROM pipeline_shards "
                "WHERE job_id=? AND state='leased' AND lease_expires_at<=?",
                (job_id, now),
            ).fetchall()
            for row in expired_rows:
                self.connection.execute(
                    "UPDATE shard_attempts SET finished_at=?,outcome='expired' "
                    "WHERE shard_id=(SELECT shard_id FROM pipeline_shards "
                    "WHERE job_id=? AND shard_key=?) AND outcome='running'",
                    (now, job_id, row["shard_key"]),
                )
            self.connection.execute(
                "UPDATE pipeline_shards SET state='cancelled',lease_owner=NULL,lease_expires_at=NULL,"
                "updated_at=? WHERE job_id=? AND state='leased' AND lease_expires_at<=?",
                (now, job_id, now),
            )
            self._event(job_id, "job.cancelled", actor_id, {
                "reason": reason.strip(),
                "cancelled_waiting_shards": cancelled,
                "cancelled_expired_shards": len(expired_rows),
            })
            self._maybe_complete_job(job_id)
        return self.get_job(job_id)

    def sweep_cancelled(self) -> dict[str, int]:
        """把取消任务中租约已过期的运行分片收束为取消（可在重启后调用）。"""

        now = self._now_text()
        swept = 0
        with transaction(self.connection, immediate=True):
            rows = self.connection.execute(
                "SELECT s.job_id,s.shard_key,s.shard_id FROM pipeline_shards s "
                "JOIN pipeline_jobs j ON j.job_id=s.job_id "
                "WHERE j.state='cancelled' AND s.state='leased' AND s.lease_expires_at<=?",
                (now,),
            ).fetchall()
            for row in rows:
                self.connection.execute(
                    "UPDATE shard_attempts SET finished_at=?,outcome='expired' "
                    "WHERE shard_id=? AND outcome='running'",
                    (now, row["shard_id"]),
                )
                self.connection.execute(
                    "UPDATE pipeline_shards SET state='cancelled',lease_owner=NULL,lease_expires_at=NULL,"
                    "updated_at=? WHERE shard_id=?",
                    (now, row["shard_id"]),
                )
                self._event(row["job_id"], "shard.cancelled_after_sweep", "system", {
                    "shard_key": row["shard_key"],
                }, shard_key=row["shard_key"])
                self._maybe_complete_job(row["job_id"])
                swept += 1
        return {"swept": swept}

    # -------------------------------------------------------------- 人工处置

    def requeue_shard(
        self, actor_id: str, job_id: str, shard_key: str, *, reset_attempts: bool = True
    ) -> dict[str, Any]:
        """人工判定后重新排队一个 ``manual`` 分片。"""

        now = self._now_text()
        with transaction(self.connection, immediate=True):
            job = self._job_row(job_id)
            if job["state"] != "active":
                raise InvalidState("任务已取消或结束，不能重新排队")
            shard = self._shard_row(job_id, shard_key)
            if shard["state"] != "manual":
                raise InvalidState("只有人工处置中的分片可以重新排队")
            if reset_attempts:
                self.connection.execute(
                    "UPDATE pipeline_shards SET attempts=0,last_error=NULL,manual_reason=NULL "
                    "WHERE shard_id=?",
                    (shard["shard_id"],),
                )
            self.connection.execute(
                "UPDATE pipeline_shards SET state='waiting',available_at=?,lease_owner=NULL,"
                "lease_expires_at=NULL,updated_at=? WHERE job_id=? AND shard_key=?",
                (now, now, job_id, shard_key),
            )
            self._event(job_id, "shard.requeued", actor_id, {
                "shard_key": shard_key, "reset_attempts": reset_attempts,
            }, shard_key=shard_key)
        return {"job_id": job_id, "shard_key": shard_key, "state": "waiting", "available_at": now}

    def discard_shard(self, actor_id: str, job_id: str, shard_key: str, reason: str) -> dict[str, Any]:
        """人工放弃一个 ``manual`` 分片；其下游阻塞分片级联取消。"""

        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("处置原因不能为空")
        now = self._now_text()
        with transaction(self.connection, immediate=True):
            job = self._job_row(job_id)
            shard = self._shard_row(job_id, shard_key)
            if shard["state"] != "manual":
                raise InvalidState("只有人工处置中的分片可以放弃")
            self.connection.execute(
                "UPDATE pipeline_shards SET state='cancelled',manual_reason=?,updated_at=? "
                "WHERE job_id=? AND shard_key=?",
                (f"人工放弃：{reason.strip()}", now, job_id, shard_key),
            )
            self._event(job_id, "shard.discarded", actor_id, {
                "shard_key": shard_key, "reason": reason.strip(),
            }, shard_key=shard_key)
            # 级联取消所有（传递性）依赖它的阻塞分片。
            frontier = [shard_key]
            cascaded: list[str] = []
            while frontier:
                current = frontier.pop()
                children = self.connection.execute(
                    "SELECT s.shard_key FROM pipeline_shards s "
                    "JOIN shard_dependencies d ON d.job_id=s.job_id AND d.shard_key=s.shard_key "
                    "WHERE d.job_id=? AND d.depends_on_key=? AND s.state='blocked'",
                    (job_id, current),
                ).fetchall()
                for child in children:
                    self.connection.execute(
                        "UPDATE pipeline_shards SET state='cancelled',updated_at=? "
                        "WHERE job_id=? AND shard_key=? AND state='blocked'",
                        (now, job_id, child["shard_key"]),
                    )
                    self._event(job_id, "shard.cancelled_by_upstream", actor_id, {
                        "shard_key": child["shard_key"], "upstream": current,
                    }, shard_key=child["shard_key"])
                    cascaded.append(child["shard_key"])
                    frontier.append(child["shard_key"])
            self._maybe_complete_job(job_id)
        return {
            "job_id": job_id, "shard_key": shard_key, "state": "cancelled",
            "cascaded_cancelled": cascaded,
        }

    # ------------------------------------------------------------------ 查询

    def get_job(self, job_id: str) -> dict[str, Any]:
        job = self._job_row(job_id)
        result = dict(job)
        result["manifest"] = json.loads(job["manifest_json"])
        counts = self.connection.execute(
            "SELECT state,count(*) AS n FROM pipeline_shards WHERE job_id=? GROUP BY state",
            (job_id,),
        ).fetchall()
        result["shard_states"] = {row["state"]: row["n"] for row in counts}
        return result

    @staticmethod
    def _runtime_state(shard: sqlite3.Row, now: str) -> str:
        state = shard["state"]
        if state in ("blocked", "waiting"):
            return "pending"
        if state == "leased":
            return "running"
        if state == "manual":
            return "failed"
        if state == "succeeded":
            return "done"
        return "cancelled"

    def _attempts(self, shard_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT attempt_no,fence,worker_id,started_at,finished_at,outcome,error,output_sha256 "
            "FROM shard_attempts WHERE shard_id=? ORDER BY attempt_id",
            (shard_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _dependency_state(self, job_id: str, shard_key: str) -> list[dict[str, str]]:
        rows = self.connection.execute(
            "SELECT d.depends_on_key AS key,p.state FROM shard_dependencies d "
            "JOIN pipeline_shards p ON p.job_id=d.job_id AND p.shard_key=d.depends_on_key "
            "WHERE d.job_id=? AND d.shard_key=? ORDER BY d.ordinal",
            (job_id, shard_key),
        ).fetchall()
        return [{"shard_key": row["key"], "state": row["state"]} for row in rows]

    def _explain(
        self, shard: sqlite3.Row, job: sqlite3.Row, now: str,
        attempts: Sequence[Mapping[str, Any]], deps: Sequence[Mapping[str, str]],
    ) -> str:
        state = shard["state"]
        if state == "blocked":
            pending = [f"{d['shard_key']}({d['state']})" for d in deps if d["state"] != "succeeded"]
            return f"等待上游分片完成：{', '.join(pending)}"
        if state == "waiting":
            if shard["attempts"] > 0:
                return (
                    f"第 {shard['attempts']} 次尝试失败后按退避策略等待，"
                    f"{shard['available_at']} 起可重新领取；最近错误：{shard['last_error']}"
                )
            origin = "无上游依赖" if not deps else "上游全部完成"
            return f"来自输入清单第 {shard['ordinal']} 号分片，{origin}，等待工作进程领取"
        if state == "leased":
            expired = shard["lease_expires_at"] <= now
            tail = "租约已过期，可被其他进程接管" if expired else "租约有效"
            return (
                f"由 {shard['lease_owner']} 第 {shard['attempts']} 次持有"
                f"（fence={shard['lease_fence']}），租约至 {shard['lease_expires_at']}，{tail}"
            )
        if state == "succeeded":
            return (
                f"由 {attempts[-1]['worker_id'] if attempts else shard['lease_owner']} "
                f"于 {shard['completed_at']} 完成，输出校验值 {shard['output_sha256']}"
                + (f"，位于 {shard['output_location']}" if shard["output_location"] else "")
            )
        if state == "manual":
            return f"人工处置：{shard['manual_reason'] or shard['last_error']}"
        if state == "cancelled":
            if job["state"] == "cancelled" and job["cancel_reason"]:
                return f"任务已取消（{job['cancel_reason']}），分片未执行或终止"
            return shard["manual_reason"] or "随上游放弃而取消"
        return state

    def shard_detail(self, job_id: str, shard_key: str) -> dict[str, Any]:
        """重建单个分片的当前状态与来源解释。"""

        now = self._now_text()
        job = self._job_row(job_id)
        shard = self._shard_row(job_id, shard_key)
        attempts = self._attempts(shard["shard_id"])
        deps = self._dependency_state(job_id, shard_key)
        return self._shard_view(shard, job, now, attempts, deps)

    def _shard_view(
        self, shard: sqlite3.Row, job: sqlite3.Row, now: str,
        attempts: Sequence[Mapping[str, Any]], deps: Sequence[Mapping[str, str]],
    ) -> dict[str, Any]:
        return {
            "shard_key": shard["shard_key"],
            "ordinal": shard["ordinal"],
            "state": shard["state"],
            "runtime_state": self._runtime_state(shard, now),
            "attempts_count": shard["attempts"],
            "lease_owner": shard["lease_owner"],
            "lease_expires_at": shard["lease_expires_at"],
            "lease_fence": shard["lease_fence"],
            "lease_expired": shard["state"] == "leased" and shard["lease_expires_at"] <= now,
            "available_at": shard["available_at"],
            "input": json.loads(shard["input_spec_json"]),
            "input_sha256": shard["input_sha256"],
            "depends_on": deps,
            "output_sha256": shard["output_sha256"],
            "output_location": shard["output_location"],
            "result": None if shard["result_json"] is None else json.loads(shard["result_json"]),
            "last_error": shard["last_error"],
            "manual_reason": shard["manual_reason"],
            "completed_at": shard["completed_at"],
            "created_at": shard["created_at"],
            "updated_at": shard["updated_at"],
            "provenance": {
                "job_id": job["job_id"],
                "manifest_sha256": job["manifest_sha256"],
                "rule_id": job["rule_id"],
                "rule_version": job["rule_version"],
                "rule_sha256": job["rule_sha256"],
                "ordinal": shard["ordinal"],
                "explanation": self._explain(shard, job, now, attempts, deps),
                "attempt_history": attempts,
            },
        }

    def job_status(self, job_id: str, runtime_state: str | None = None) -> dict[str, Any]:
        """重建任务的待处理/运行中/失败/完成视图，并解释每个分片的来源。"""

        if runtime_state is not None and runtime_state not in _RUNTIME_STATES:
            raise ValidationFailed(f"运行态过滤必须是 {_RUNTIME_STATES} 之一")
        now = self._now_text()
        job = self._job_row(job_id)
        rows = self.connection.execute(
            "SELECT * FROM pipeline_shards WHERE job_id=? ORDER BY ordinal,shard_id", (job_id,)
        ).fetchall()
        shards: list[dict[str, Any]] = []
        counts = {name: 0 for name in _RUNTIME_STATES}
        for shard in rows:
            attempts = self._attempts(shard["shard_id"])
            deps = self._dependency_state(job_id, shard["shard_key"])
            view = self._shard_view(shard, job, now, attempts, deps)
            counts[view["runtime_state"]] += 1
            if runtime_state is None or view["runtime_state"] == runtime_state:
                shards.append(view)
        events = self.connection.execute(
            "SELECT event_type,actor_id,shard_key,payload_json,created_at FROM pipeline_events "
            "WHERE job_id=? ORDER BY event_id", (job_id,),
        ).fetchall()
        job_view = dict(job)
        job_view["manifest"] = json.loads(job["manifest_json"])
        return {
            "job": job_view,
            "rebuilt_at": now,
            "counts": counts,
            "shards": shards,
            "events": [
                dict(row) | {"payload": json.loads(row["payload_json"])} for row in events
            ],
        }
