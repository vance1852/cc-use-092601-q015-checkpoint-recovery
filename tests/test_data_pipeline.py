"""数据加工断点恢复流水线的服务层测试。"""

from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone

from data_pipeline.clock import FrozenClock
from data_pipeline.errors import (
    Conflict,
    InputChanged,
    InvalidState,
    StaleLease,
    ValidationFailed,
)
from data_pipeline.hashing import digest_value
from data_pipeline.models import RetryPolicy
from data_pipeline.service import PipelineService


RULE = {"rule_id": "clean", "version": 1, "definition": {"op": "clean", "v": 1}}


def make_shards():
    return [
        {"shard_key": "a", "input": {"file": "a"}},
        {"shard_key": "b", "input": {"file": "b"}},
        {"shard_key": "c", "input": {"join": ["a", "b"]}, "depends_on": ["a", "b"]},
        {"shard_key": "d", "input": {"feat": ["c"]}, "depends_on": ["c"]},
    ]


class PipelineTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.service = PipelineService(self.connection, self.clock)
        self.rule_sha = digest_value(RULE["definition"])
        self.output = {"sha256": "f" * 64, "location": "oss://out/a"}

    def tearDown(self) -> None:
        self.connection.close()

    def create(self, job_id="job-1", retry=None, shards=None):
        return self.service.create_job(
            "planner", job_id, RULE, shards or make_shards(),
            manifest=[{"uri": "in.jsonl"}],
            retry=retry or {"max_attempts": 2, "backoff_base_seconds": 10, "backoff_max_seconds": 100},
        )

    def complete(self, claim, output=None):
        return self.service.complete_shard(
            claim["lease_owner"], claim["job_id"], claim["shard_key"], claim["fence"],
            output or self.output,
            input_sha256=claim["input_sha256"], rule_sha256=claim["rule_sha256"],
        )


class JobCreationTests(PipelineTestBase):
    def test_persists_manifest_and_rule_digests(self) -> None:
        job = self.create()
        self.assertEqual(job["state"], "active")
        self.assertEqual(len(job["manifest_sha256"]), 64)
        self.assertEqual(job["rule_sha256"], self.rule_sha)
        self.assertEqual(job["shard_states"], {"blocked": 2, "waiting": 2})

    def test_unknown_dependency_rejected(self) -> None:
        bad = [{"shard_key": "a", "input": {}, "depends_on": ["ghost"]}]
        with self.assertRaises(ValidationFailed):
            self.create(shards=bad)

    def test_cyclic_dependency_rejected(self) -> None:
        bad = [
            {"shard_key": "a", "input": {}, "depends_on": ["b"]},
            {"shard_key": "b", "input": {}, "depends_on": ["a"]},
        ]
        with self.assertRaises(ValidationFailed):
            self.create(shards=bad)

    def test_self_dependency_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.create(shards=[{"shard_key": "a", "input": {}, "depends_on": ["a"]}])

    def test_duplicate_job_id_conflicts(self) -> None:
        self.create()
        with self.assertRaises(Conflict):
            self.create()


class ClaimAndDependencyTests(PipelineTestBase):
    def test_claim_order_respects_dependencies(self) -> None:
        self.create()
        first = self.service.claim_shard("w1", 60)
        second = self.service.claim_shard("w1", 60)
        self.assertEqual([first["shard_key"], second["shard_key"]], ["a", "b"])
        # c 仍被阻塞，没有可领取分片。
        self.assertIsNone(self.service.claim_shard("w1", 60))

    def test_completion_atomically_unlocks_successors(self) -> None:
        self.create()
        claim_a = self.service.claim_shard("w1", 60)
        self.complete(claim_a)
        # 只完成 a 时 c 仍需等待 b。
        detail_c = self.service.shard_detail("job-1", "c")
        self.assertEqual(detail_c["state"], "blocked")
        claim_b = self.service.claim_shard("w1", 60)
        self.assertEqual(claim_b["shard_key"], "b")
        result = self.complete(claim_b)
        self.assertEqual(result["unlocked"], ["c"])
        claim_c = self.service.claim_shard("w1", 60)
        self.assertEqual(claim_c["shard_key"], "c")
        result = self.complete(claim_c)
        self.assertEqual(result["unlocked"], ["d"])

    def test_full_completion_completes_job(self) -> None:
        self.create()
        for _ in range(4):
            claim = self.service.claim_shard("w1", 60)
            self.complete(claim, {"sha256": digest_value({"k": claim["shard_key"]})})
        self.assertEqual(self.service.get_job("job-1")["state"], "completed")


class LeaseTests(PipelineTestBase):
    def test_expired_lease_can_be_taken_over(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        first = self.service.claim_shard("w1", 60)
        self.assertEqual(first["fence"], 1)
        self.clock.advance(seconds=61)
        second = self.service.claim_shard("w2", 60)
        self.assertEqual(second["shard_key"], "a")
        self.assertEqual(second["attempt"], 2)
        self.assertEqual(second["fence"], 2)
        detail = self.service.shard_detail("job-1", "a")
        self.assertTrue(detail["lease_expired"] is False)
        self.assertEqual([a["outcome"] for a in detail["provenance"]["attempt_history"]],
                         ["expired", "running"])

    def test_unexpired_lease_is_not_taken_over(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        self.service.claim_shard("w1", 60)
        self.clock.advance(seconds=30)
        self.assertIsNone(self.service.claim_shard("w2", 60))

    def test_late_completion_from_old_holder_rejected(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        first = self.service.claim_shard("w1", 60)
        self.clock.advance(seconds=61)
        second = self.service.claim_shard("w2", 60)
        with self.assertRaises(StaleLease):
            self.service.complete_shard(
                "w1", "job-1", "a", first["fence"], self.output,
                input_sha256=first["input_sha256"], rule_sha256=self.rule_sha,
            )
        # 旧持有者的迟到失败同样被拒。
        with self.assertRaises(StaleLease):
            self.service.fail_shard("w1", "job-1", "a", first["fence"], "迟到失败")
        # 新持有者正常完成。
        self.complete(second)

    def test_late_completion_after_expiry_without_takeover_rejected(self) -> None:
        # 即使没有其他进程接管，租约过期后旧持有者的迟到结果也拒绝（非无期限租约）。
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 30)
        self.clock.advance(seconds=31)
        with self.assertRaises(StaleLease):
            self.service.complete_shard(
                "w1", "job-1", "a", claim["fence"], self.output,
                input_sha256=claim["input_sha256"], rule_sha256=self.rule_sha,
            )
        with self.assertRaises(StaleLease):
            self.service.fail_shard("w1", "job-1", "a", claim["fence"], "迟到失败")

    def test_wrong_fence_rejected_even_before_expiry(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        self.service.claim_shard("w1", 60)
        with self.assertRaises(StaleLease):
            self.service.complete_shard(
                "w1", "job-1", "a", 99, self.output,
                input_sha256="a" * 64, rule_sha256=self.rule_sha,
            )

    def test_renew_lease(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 60)
        self.clock.advance(seconds=30)
        renewed = self.service.renew_lease("w1", "job-1", "a", claim["fence"], 60)
        self.clock.advance(seconds=31)
        # 续租后旧到期时间已失效，其他进程不能接管。
        self.assertIsNone(self.service.claim_shard("w2", 60))
        self.assertIn("lease_expires_at", renewed)
        with self.assertRaises(StaleLease):
            self.service.renew_lease("w2", "job-1", "a", claim["fence"], 60)


class CompletionValidationTests(PipelineTestBase):
    def test_input_digest_change_rejected(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 60)
        with self.assertRaises(InputChanged):
            self.service.complete_shard(
                "w1", "job-1", "a", claim["fence"], self.output,
                input_sha256="0" * 64, rule_sha256=self.rule_sha,
            )

    def test_rule_digest_change_rejected(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 60)
        with self.assertRaises(InputChanged):
            self.service.complete_shard(
                "w1", "job-1", "a", claim["fence"], self.output,
                input_sha256=claim["input_sha256"], rule_sha256="1" * 64,
            )

    def test_output_requires_sha256(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 60)
        with self.assertRaises(ValidationFailed):
            self.service.complete_shard(
                "w1", "job-1", "a", claim["fence"], {"location": "x"},
                input_sha256=claim["input_sha256"], rule_sha256=self.rule_sha,
            )

    def test_succeeded_shard_keeps_output_evidence(self) -> None:
        self.create(shards=[{"shard_key": "a", "input": {}}])
        claim = self.service.claim_shard("w1", 60)
        self.complete(claim)
        detail = self.service.shard_detail("job-1", "a")
        self.assertEqual(detail["output_sha256"], "f" * 64)
        self.assertEqual(detail["state"], "succeeded")


class FailureRetryTests(PipelineTestBase):
    def test_backoff_schedule(self) -> None:
        policy = RetryPolicy(max_attempts=4, backoff_base_seconds=5, backoff_max_seconds=100)
        self.assertEqual(policy.delay_seconds(1), 5)
        self.assertEqual(policy.delay_seconds(2), 10)
        self.assertEqual(policy.delay_seconds(3), 20)
        self.assertEqual(policy.delay_seconds(4), 40)
        capped = RetryPolicy(max_attempts=10, backoff_base_seconds=5, backoff_max_seconds=60)
        self.assertEqual(capped.delay_seconds(10), 60)

    def test_failure_retries_with_backoff_then_manual(self) -> None:
        self.create(retry={"max_attempts": 2, "backoff_base_seconds": 10, "backoff_max_seconds": 100})
        first = self.service.claim_shard("w1", 60)
        failed = self.service.fail_shard("w1", "job-1", "a", first["fence"], "瞬时错误")
        self.assertEqual(failed["state"], "waiting")
        self.clock.advance(seconds=9)
        # a 仍在退避窗口内；b 无依赖可以被领取。
        other = self.service.claim_shard("w2", 60)
        self.assertEqual(other["shard_key"], "b")
        self.clock.advance(seconds=2)
        second = self.service.claim_shard("w1", 60, job_id="job-1")
        self.assertEqual(second["shard_key"], "a")
        self.assertEqual(second["attempt"], 2)
        # 先把 b 正常完成，排除过期接管对后续断言的干扰。
        self.service.complete_shard(
            "w2", "job-1", "b", other["fence"],
            {"sha256": "b" * 64}, input_sha256=other["input_sha256"],
            rule_sha256=self.rule_sha,
        )
        manual = self.service.fail_shard("w1", "job-1", "a", second["fence"], "持续错误")
        self.assertEqual(manual["state"], "manual")
        detail = self.service.shard_detail("job-1", "a")
        self.assertEqual(detail["runtime_state"], "failed")
        self.assertIn("持续错误", detail["provenance"]["explanation"])
        # 人工处置后即使等待很久也不会被自动领取（c/d 仍被阻塞）。
        self.clock.advance(seconds=10000)
        self.assertIsNone(self.service.claim_shard("w1", 60, job_id="job-1"))

    def test_requeue_after_manual(self) -> None:
        self.create(retry={"max_attempts": 1, "backoff_base_seconds": 0, "backoff_max_seconds": 0})
        claim = self.service.claim_shard("w1", 60)
        self.assertEqual(self.service.fail_shard("w1", "job-1", "a", claim["fence"], "x")["state"], "manual")
        self.service.requeue_shard("ops", "job-1", "a")
        again = self.service.claim_shard("w1", 60)
        self.assertEqual(again["shard_key"], "a")
        self.assertEqual(again["attempt"], 1)

    def test_discard_cascades_to_downstream_blocked(self) -> None:
        self.create(retry={"max_attempts": 1, "backoff_base_seconds": 0, "backoff_max_seconds": 0})
        claim = self.service.claim_shard("w1", 60)
        self.service.fail_shard("w1", "job-1", "a", claim["fence"], "坏数据")
        result = self.service.discard_shard("ops", "job-1", "a", "确认无法修复")
        self.assertEqual(set(result["cascaded_cancelled"]), {"c", "d"})
        for key in ("a", "c", "d"):
            self.assertEqual(self.service.shard_detail("job-1", key)["state"], "cancelled")
        # b 不受影响。
        self.assertEqual(self.service.shard_detail("job-1", "b")["state"], "waiting")

    def test_explicit_retry_seconds_overrides_policy(self) -> None:
        self.create()
        claim = self.service.claim_shard("w1", 60)
        failed = self.service.fail_shard(
            "w1", "job-1", "a", claim["fence"], "x", retry_seconds=0
        )
        self.assertEqual(failed["state"], "waiting")
        immediate = self.service.claim_shard("w1", 60)
        self.assertEqual(immediate["shard_key"], "a")


class CancelTests(PipelineTestBase):
    def test_cancel_blocks_new_claims_but_keeps_outputs(self) -> None:
        self.create()
        claim_a = self.service.claim_shard("w1", 60)
        self.complete(claim_a)
        claim_b = self.service.claim_shard("w1", 60)
        self.service.cancel_job("planner", "job-1", "清单作废")
        # 新领取被阻止。
        self.assertIsNone(self.service.claim_shard("w2", 60))
        status = self.service.job_status("job-1")
        self.assertEqual(status["job"]["state"], "cancelled")
        states = {s["shard_key"]: s["state"] for s in status["shards"]}
        self.assertEqual(states, {"a": "succeeded", "b": "leased", "c": "cancelled", "d": "cancelled"})
        # 已完成的输出证据保留。
        self.assertEqual(self.service.shard_detail("job-1", "a")["output_sha256"], "f" * 64)
        # 取消瞬间持有有效租约的在途分片仍可登记完成证据，但不会解锁任何新分片。
        result = self.complete(claim_b, {"sha256": "b" * 64, "location": "oss://b"})
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["unlocked"], [])
        self.assertEqual(self.service.shard_detail("job-1", "c")["state"], "cancelled")

    def test_cancel_then_lease_expires_sweeps(self) -> None:
        self.create()
        claim = self.service.claim_shard("w1", 60)
        self.service.cancel_job("planner", "job-1", "作废")
        self.clock.advance(seconds=61)
        self.assertEqual(self.service.sweep_cancelled()["swept"], 1)
        detail = self.service.shard_detail("job-1", claim["shard_key"])
        self.assertEqual(detail["state"], "cancelled")
        self.assertEqual(detail["provenance"]["attempt_history"][-1]["outcome"], "expired")

    def test_cancel_non_active_rejected(self) -> None:
        self.create()
        self.service.cancel_job("planner", "job-1", "x")
        with self.assertRaises(InvalidState):
            self.service.cancel_job("planner", "job-1", "y")

    def test_fail_on_cancelled_job_never_requeues(self) -> None:
        self.create()
        claim = self.service.claim_shard("w1", 60)
        self.service.cancel_job("planner", "job-1", "作废")
        # 取消瞬间完成仍可登记吗？设计上取消只阻止领取；持有者立即完成应当成功。
        result = self.service.fail_shard("w1", "job-1", "a", claim["fence"], "取消后失败")
        self.assertEqual(result["state"], "cancelled")
        self.assertIsNone(self.service.claim_shard("w1", 60))


class StatusRebuildTests(PipelineTestBase):
    def test_status_counts_and_explanations(self) -> None:
        self.create()
        claim = self.service.claim_shard("w1", 60)
        status = self.service.job_status("job-1")
        self.assertEqual(status["counts"],
                         {"pending": 3, "running": 1, "failed": 0, "done": 0, "cancelled": 0})
        explanations = {s["shard_key"]: s["provenance"]["explanation"] for s in status["shards"]}
        self.assertIn("等待上游", explanations["c"])
        self.assertIn("w1", explanations["a"])
        self.assertIn("输入清单第 1 号分片", explanations["b"])
        self.complete(claim)
        done = self.service.job_status("job-1", runtime_state="done")
        self.assertEqual([s["shard_key"] for s in done["shards"]], ["a"])

    def test_status_rebuilt_after_restart(self) -> None:
        self.create()
        claim = self.service.claim_shard("w1", 60)
        self.complete(claim)
        claim_b = self.service.claim_shard("w1", 60)
        self.service.fail_shard("w1", "job-1", "b", claim_b["fence"], "err")
        # 用新的服务实例（新进程）重新打开同一内存库不可行，这里用新实例共享连接，
        # 文件级重启由 acceptance 脚本验证。
        rebuilt = PipelineService(self.connection, self.clock)
        status = rebuilt.job_status("job-1")
        # b 在退避窗口（waiting）与 c/d（blocked）都重建为 pending。
        self.assertEqual(status["counts"],
                         {"pending": 3, "running": 0, "failed": 0, "done": 1, "cancelled": 0})
        keys = {(s["shard_key"], s["state"]) for s in status["shards"]}
        self.assertIn(("a", "succeeded"), keys)
        self.assertIn(("b", "waiting"), keys)
        self.assertIn(("c", "blocked"), keys)
        self.assertTrue(all(s["provenance"]["attempt_history"] for s in status["shards"]
                            if s["shard_key"] in {"a", "b"}))


if __name__ == "__main__":
    unittest.main()
