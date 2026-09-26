"""数据加工断点恢复能力的离线验收入口。

在同一临时 SQLite 文件上模拟两次进程重启，验证：
租约接管、迟到结果拒绝、失败重试与人工处置、取消保留证据、
重启后状态重建与分片来源解释。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import InvalidState
from .service import ForgeService
from .storage import connect, inspect_schema


def _output_digest(*parts: object) -> str:
    return hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def _load_definition(workspace: Path) -> dict[str, object]:
    return json.loads((workspace / "fixtures" / "demo_processing_task.json").read_text(encoding="utf-8"))


def _complete_ready(service: ForgeService, task_id: str, worker: str, expect: str) -> dict[str, object]:
    shard = service.claim_shard(worker, lease_seconds=120, task_id=task_id)
    if shard is None or shard["shard_key"] != expect:
        raise RuntimeError(f"未能按预期领取分片 {expect}: {shard}")
    return service.complete_shard(
        worker, task_id, shard["shard_key"], shard["lease_seq"],
        _output_digest(task_id, shard["shard_key"], shard["attempt"]),
        {"rows": 128, "worker": worker},
    )


def run(workspace: Path) -> dict[str, object]:
    definition = _load_definition(workspace)
    clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
    with tempfile.TemporaryDirectory(prefix="data-forge-") as temporary:
        database = Path(temporary) / "forge.sqlite3"

        # 第一阶段：创建任务并完成首个分片，随后模拟进程崩溃。
        connection = connect(database)
        service = ForgeService(connection, clock)
        service.create_user("planner-1", "数据平台规划员", "planner")
        service.create_user("supervisor-1", "加工值班主管", "supervisor")
        service.create_user("auditor-1", "审计人员", "auditor")
        created = service.create_task(
            "planner-1", definition["task_id"], definition["name"], definition["rule_version"],
            definition["rule_params"], definition["manifest"], definition["shards"],
            definition["retry_policy"],
        )
        if created["counts"] != {"pending": 5, "running": 0, "failed": 0, "completed": 0}:
            raise RuntimeError("任务初始状态计数不符合预期")
        first = _complete_ready(service, definition["task_id"], "worker-1", "cleanse-a")
        stalled = service.claim_shard("worker-2", lease_seconds=30, task_id=definition["task_id"])
        if stalled["shard_key"] != "cleanse-b":
            raise RuntimeError("worker-2 应持有 cleanse-b")
        connection.close()  # 模拟持有租约的进程崩溃

        # 第二阶段：重启后租约接管，迟到结果不得覆盖新持有者。
        clock.advance(seconds=31)
        connection = connect(database)
        service = ForgeService(connection, clock)
        rebuilt = service.get_task("auditor-1", definition["task_id"])
        if rebuilt["counts"]["completed"] != 1 or rebuilt["counts"]["running"] != 1:
            raise RuntimeError("重启后状态重建失败")
        takeover = service.claim_shard("worker-3", lease_seconds=120, task_id=definition["task_id"])
        if takeover["shard_key"] != "cleanse-b" or takeover["lease_seq"] <= stalled["lease_seq"]:
            raise RuntimeError("租约过期后未被接管或防护序号未递增")
        try:
            service.complete_shard(
                "worker-2", definition["task_id"], "cleanse-b", stalled["lease_seq"],
                _output_digest("late"), {},
            )
        except InvalidState:
            late_rejected = True
        else:
            raise RuntimeError("迟到结果覆盖了新持有者")
        service.complete_shard(
            "worker-3", definition["task_id"], "cleanse-b", takeover["lease_seq"],
            _output_digest(definition["task_id"], "cleanse-b", takeover["attempt"]),
            {"rows": 128, "worker": "worker-3"},
        )

        # 第三阶段：失败按策略退避重试，耗尽后进入人工处置。
        flaky = service.claim_shard("worker-4", lease_seconds=120, task_id=definition["task_id"])
        failed = service.fail_shard("worker-4", definition["task_id"], flaky["shard_key"], flaky["lease_seq"], "上游对象存储超时")
        if failed["state"] != "failed":
            raise RuntimeError("首次失败应进入退避重试")
        if service.claim_shard("worker-5", lease_seconds=10, task_id=definition["task_id"]) is not None:
            raise RuntimeError("退避窗口内不应有可领取分片")
        clock.advance(seconds=45)
        retry = service.claim_shard("worker-5", lease_seconds=120, task_id=definition["task_id"])
        exhausted = service.fail_shard("worker-5", definition["task_id"], retry["shard_key"], retry["lease_seq"], "再次超时")
        if exhausted["state"] != "manual":
            raise RuntimeError("重试耗尽后应进入人工处置")
        resolved = service.resolve_shard(
            "supervisor-1", definition["task_id"], retry["shard_key"], "complete",
            _output_digest("manual", retry["shard_key"]), "人工核对后登记完成",
        )
        if resolved["unlocked"] != ["tokenize"]:
            raise RuntimeError("人工完成应原子解锁下游分片")

        # 第四阶段：跑完剩余分片，任务自动完成。
        _complete_ready(service, definition["task_id"], "worker-1", "tokenize")
        final = _complete_ready(service, definition["task_id"], "worker-2", "feature-stats")
        if not final["task_completed"]:
            raise RuntimeError("全部分片完成后任务应自动完成")
        connection.close()

        # 第五阶段：再次重启，验证状态重建、来源解释与审计链。
        connection = connect(database)
        service = ForgeService(connection, clock)
        status = service.get_task("auditor-1", definition["task_id"])
        explained = service.explain_shard("auditor-1", definition["task_id"], "feature-stats")
        trail = service.audit_trail("auditor-1", definition["task_id"])
        schema = inspect_schema(connection)

        # 第六阶段：取消只阻止新领取，已完成证据保留。
        service.create_task(
            "planner-1", "demo-cancelled", "取消语义验证", "rules-v1", {},
            definition["manifest"][:1],
            [
                {"shard_key": "part-a", "inputs": [definition["manifest"][0]["uri"]]},
                {"shard_key": "part-b", "inputs": [definition["manifest"][0]["uri"]]},
            ],
        )
        done = _complete_ready(service, "demo-cancelled", "worker-9", "part-a")
        cancelled = service.cancel_task("planner-1", "demo-cancelled", "需求变更")
        if service.claim_shard("worker-9", lease_seconds=10, task_id="demo-cancelled") is not None:
            raise RuntimeError("取消后不应再领取分片")
        evidence = service.explain_shard("auditor-1", "demo-cancelled", "part-a")
        connection.close()

    checks = {
        "late_result_rejected": late_rejected,
        "task_completed": status["task"]["state"] == "completed",
        "counts_rebuilt": status["counts"] == {"pending": 0, "running": 0, "failed": 0, "completed": 5},
        "evidence_preserved_after_cancel": len(evidence["completions"]) == 1
        and cancelled["counts"]["completed"] == 1,
        "audit_chain_valid": trail["chain_valid"],
        "schema_ok": not schema["missing_tables"] and schema["schema_version"] == "1",
    }
    if not all(checks.values()):
        raise RuntimeError(f"离线验收检查失败: {checks}")
    return {
        "status": "ok",
        "task_id": definition["task_id"],
        "task_state": status["task"]["state"],
        "counts": status["counts"],
        "first_completion_id": first["completion_id"],
        "cancelled_task_completed_shards": cancelled["counts"]["completed"],
        "cancelled_completion_id": done["completion_id"],
        "explanation": explained["explanation"],
        "audit_events": len(trail["events"]),
        "audit_chain_valid": trail["chain_valid"],
        "schema": schema,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行数据加工断点恢复能力的离线自检")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    result = run(args.workspace.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
