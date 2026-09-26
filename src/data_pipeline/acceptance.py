"""数据加工断点恢复流水线的离线验收入口。

在临时 SQLite 文件上演练：分片依赖解锁、租约过期接管与 fencing、
迟到结果拒绝、退避重试与人工处置、取消保留证据，以及关闭进程后
用新连接重建全部状态与分片来源解释。
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import StaleLease
from .hashing import digest_value
from .service import PipelineService
from .storage import connect, inspect_schema


RULE = {
    "rule_id": "clean-split-stats",
    "version": 1,
    "definition": {"steps": ["清洗", "切分", "特征统计"], "normalize": "trim", "seed": 7},
}
MANIFEST = [
    {"uri": "s3://demo/part-000.jsonl", "bytes": 1024, "etag": "etag-0"},
    {"uri": "s3://demo/part-001.jsonl", "bytes": 2048, "etag": "etag-1"},
]


def _output(tag: str) -> dict[str, str]:
    return {"sha256": digest_value({"output": tag}), "location": f"oss://demo/{tag}.parquet"}


def _build_job(service: PipelineService) -> None:
    rule_sha = digest_value(RULE["definition"])
    shards = [
        {"shard_key": "s0", "input": {"file": "part-000.jsonl", "rows": 2}},
        {"shard_key": "s1", "input": {"file": "part-001.jsonl", "rows": 3}},
        {"shard_key": "s2", "input": {"join": ["s0"]}, "depends_on": ["s0"]},
        {"shard_key": "s3", "input": {"join": ["s0", "s1"]}, "depends_on": ["s0", "s1"]},
        {"shard_key": "s4", "input": {"features": ["s2"]}, "depends_on": ["s2"]},
        {"shard_key": "s5", "input": {"features": ["s3"]}, "depends_on": ["s3"]},
    ]
    service.create_job(
        "planner-1", "job-demo", RULE, shards, manifest=MANIFEST,
        retry={"max_attempts": 2, "backoff_base_seconds": 1, "backoff_max_seconds": 10},
    )

    # s0 正常完成并原子解锁 s2。
    claim = service.claim_shard("worker-1", lease_seconds=60)
    assert claim["shard_key"] == "s0" and claim["rule_sha256"] == rule_sha
    done = service.complete_shard(
        "worker-1", "job-demo", "s0", claim["fence"], _output("s0"),
        input_sha256=claim["input_sha256"], rule_sha256=rule_sha,
    )
    assert done["unlocked"] == ["s2"]

    # s1：worker-1 租约过期，worker-2 接管；worker-1 的迟到完成必须被 fencing 拒绝。
    claim1 = service.claim_shard("worker-1", lease_seconds=60)
    assert claim1["shard_key"] == "s1"
    service.clock.advance(seconds=61)
    claim2 = service.claim_shard("worker-2", lease_seconds=60)
    assert claim2["shard_key"] == "s1" and claim2["attempt"] == 2 and claim2["fence"] == 2
    try:
        service.complete_shard(
            "worker-1", "job-demo", "s1", claim1["fence"], _output("s1-stale"),
            input_sha256=claim1["input_sha256"], rule_sha256=rule_sha,
        )
        raise AssertionError("迟到结果不应被接受")
    except StaleLease:
        pass

    # s2 在 s1 仍被持有时走完重试/人工/requeue 全程，避免与 s3 的领取次序互相干扰。
    claim = service.claim_shard("worker-3", lease_seconds=60)
    assert claim["shard_key"] == "s2"
    failed = service.fail_shard("worker-3", "job-demo", "s2", claim["fence"], "下游暂时不可用")
    assert failed["state"] == "waiting"
    service.clock.advance(seconds=1)
    claim = service.claim_shard("worker-3", lease_seconds=60)
    assert claim["attempt"] == 2
    manual = service.fail_shard("worker-3", "job-demo", "s2", claim["fence"], "仍然失败")
    assert manual["state"] == "manual"
    service.requeue_shard("ops-1", "job-demo", "s2")
    service.clock.advance(seconds=1)
    claim = service.claim_shard("worker-3", lease_seconds=60)
    assert claim["attempt"] == 1
    service.complete_shard(
        "worker-3", "job-demo", "s2", claim["fence"], _output("s2"),
        input_sha256=claim["input_sha256"], rule_sha256=rule_sha,
    )

    # s1 由新持有者完成并原子解锁 s3；随后 s3、s4、s5 依次完成，任务收束。
    service.complete_shard(
        "worker-2", "job-demo", "s1", claim2["fence"], _output("s1"),
        input_sha256=claim2["input_sha256"], rule_sha256=rule_sha,
    )
    for key in ("s3", "s4", "s5"):
        claim = service.claim_shard("worker-4", lease_seconds=60)
        assert claim["shard_key"] == key
        service.complete_shard(
            "worker-4", "job-demo", key, claim["fence"], _output(key),
            input_sha256=claim["input_sha256"], rule_sha256=rule_sha,
        )
    assert service.get_job("job-demo")["state"] == "completed"


def _cancel_job(service: PipelineService) -> dict[str, str]:
    """第二个任务：完成一个分片后取消，已完成输出证据必须保留。"""

    service.create_job(
        "planner-1", "job-cancel", RULE,
        [
            {"shard_key": "c0", "input": {"file": "c0"}},
            {"shard_key": "c1", "input": {"after": "c0"}, "depends_on": ["c0"]},
        ],
        manifest=MANIFEST,
        retry={"max_attempts": 1, "backoff_base_seconds": 0, "backoff_max_seconds": 0},
    )
    rule_sha = digest_value(RULE["definition"])
    claim = service.claim_shard("worker-9", lease_seconds=60, job_id="job-cancel")
    service.complete_shard(
        "worker-9", "job-cancel", "c0", claim["fence"], _output("c0"),
        input_sha256=claim["input_sha256"], rule_sha256=rule_sha,
    )
    service.cancel_job("planner-1", "job-cancel", "输入清单作废")
    return {"c0_output": _output("c0")["sha256"]}


def run(workspace: Path) -> dict[str, object]:
    start = datetime(2026, 9, 26, 1, 0, tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory(prefix="data-pipeline-") as temporary:
        database = Path(temporary) / "pipeline.sqlite3"
        clock = FrozenClock(start)
        connection = connect(database)
        try:
            service = PipelineService(connection, clock)
            _build_job(service)
            cancel_evidence = _cancel_job(service)
        finally:
            connection.close()

        # 模拟进程重启：新连接、新服务实例，全部状态必须能从 SQLite 重建。
        restarted = connect(database)
        try:
            rebuilt = PipelineService(restarted, FrozenClock(start + timedelta(minutes=30)))
            schema = inspect_schema(restarted)
            status = rebuilt.job_status("job-demo")
            cancel_status = rebuilt.job_status("job-cancel")
            detail_s1 = rebuilt.shard_detail("job-demo", "s1")
        finally:
            restarted.close()

    if schema["missing_tables"] or schema["schema_version"] != "1":
        raise RuntimeError("SQLite 基础结构检查失败")
    if status["counts"] != {"pending": 0, "running": 0, "failed": 0, "done": 6, "cancelled": 0}:
        raise RuntimeError(f"完成任务状态计数异常: {status['counts']}")
    if any(not shard["provenance"]["explanation"] for shard in status["shards"]):
        raise RuntimeError("存在缺少来源解释的分片")
    # s1 经历两次领取：第一次过期、第二次成功。
    outcomes = [attempt["outcome"] for attempt in detail_s1["provenance"]["attempt_history"]]
    if outcomes != ["expired", "succeeded"]:
        raise RuntimeError(f"s1 尝试史重建异常: {outcomes}")
    c0 = next(shard for shard in cancel_status["shards"] if shard["shard_key"] == "c0")
    c1 = next(shard for shard in cancel_status["shards"] if shard["shard_key"] == "c1")
    if c0["state"] != "succeeded" or c0["output_sha256"] != cancel_evidence["c0_output"]:
        raise RuntimeError("取消后已完成分片的输出证据丢失")
    if c1["state"] != "cancelled":
        raise RuntimeError("取消后未开始的分片应当为 cancelled")
    return {
        "status": "ok",
        "schema": schema,
        "job_demo_counts": status["counts"],
        "job_demo_state": status["job"]["state"],
        "s1_attempts": detail_s1["provenance"]["attempt_history"],
        "s1_explanation": detail_s1["provenance"]["explanation"],
        "cancel_job_state": cancel_status["job"]["state"],
        "cancelled_kept_output_sha256": c0["output_sha256"],
        "c1_explanation": c1["provenance"]["explanation"],
        "event_count": len(status["events"]) + len(cancel_status["events"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行数据加工断点恢复流水线的离线自检")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    result = run(args.workspace.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
