from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from data_forge.acceptance import run as acceptance_run
from data_forge.api import JsonApplication
from data_forge.clock import FrozenClock
from data_forge.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from data_forge.service import ForgeService
from data_forge.storage import connect


ROOT = Path(__file__).resolve().parents[1]

MANIFEST = [
    {"uri": "s3://corpus/part-1.jsonl", "sha256": hashlib.sha256(b"part-1").hexdigest()},
    {"uri": "s3://corpus/part-2.jsonl", "sha256": hashlib.sha256(b"part-2").hexdigest()},
]

SHARDS = [
    {"shard_key": "clean-a", "inputs": ["s3://corpus/part-1.jsonl"]},
    {"shard_key": "clean-b", "inputs": ["s3://corpus/part-2.jsonl"]},
    {"shard_key": "stats", "depends_on": ["clean-a", "clean-b"]},
]

RETRY_POLICY = {"max_attempts": 2, "retry_delay_seconds": 10, "backoff_multiplier": 2.0}


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ForgeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.service = ForgeService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("sup", "supervisor"), ("aud", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_task(
            "plan", "task-1", "清洗与统计", "rules-v1", {"bins": 8},
            MANIFEST, SHARDS, RETRY_POLICY,
        )

    def tearDown(self) -> None:
        self.connection.close()

    def _claim(self, worker: str, key: str, lease_seconds: int = 60) -> dict:
        shard = self.service.claim_shard(worker, lease_seconds, "task-1")
        self.assertIsNotNone(shard)
        self.assertEqual(shard["shard_key"], key)
        return shard

    def _complete(self, worker: str, shard: dict, tag: str = "out") -> dict:
        return self.service.complete_shard(
            worker, "task-1", shard["shard_key"], shard["lease_seq"],
            digest(f"{tag}:{shard['shard_key']}"), {"rows": 10},
        )

    def test_create_task_initializes_states_and_digests(self) -> None:
        status = self.service.get_task("aud", "task-1")
        self.assertEqual(status["counts"], {"pending": 3, "running": 0, "failed": 0, "completed": 0})
        self.assertEqual(status["states"]["ready"], 2)
        self.assertEqual(status["states"]["pending"], 1)
        self.assertEqual(len(status["task"]["manifest_sha256"]), 64)
        self.assertEqual(len(status["task"]["rule_sha256"]), 64)
        self.assertEqual(status["task"]["retry_policy"]["max_attempts"], 2)
        shards = {row["shard_key"]: row for row in self.service.list_shards("aud", "task-1")["shards"]}
        self.assertEqual(shards["stats"]["pending_deps"], 2)
        self.assertEqual(len(shards["clean-a"]["input_sha256"]), 64)

    def test_create_task_validates_definition(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.create_task("plan", "bad-1", "空清单", "v1", {}, [], SHARDS)
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-2", "清单外输入", "v1", {}, MANIFEST,
                [{"shard_key": "x", "inputs": ["s3://corpus/ghost.jsonl"]}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-3", "依赖缺失", "v1", {}, MANIFEST,
                [{"shard_key": "x", "depends_on": ["ghost"]}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-4", "环路", "v1", {}, MANIFEST,
                [
                    {"shard_key": "x", "depends_on": ["y"]},
                    {"shard_key": "y", "depends_on": ["x"]},
                ],
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-5", "无输入无依赖", "v1", {}, MANIFEST, [{"shard_key": "x"}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-6", "非法重试策略", "v1", {}, MANIFEST, SHARDS,
                {"max_attempts": 0},
            )

    def test_validation_edge_cases(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-7", "空 uri", "v1", {},
                [{"uri": None, "sha256": "a" * 64}], SHARDS,
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-8", "策略类型错误", "v1", {}, MANIFEST, SHARDS, "often",
            )
        with self.assertRaises(ValidationFailed):
            self.service.create_task(
                "plan", "bad-9", "输入不是数组", "v1", {}, MANIFEST,
                [{"shard_key": "x", "inputs": "s3://corpus/part-1.jsonl"}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.claim_shard("w1", 0, "task-1")
        with self.assertRaises(ValidationFailed):
            self.service.claim_shard("", 60, "task-1")
        shard = self._claim("w1", "clean-a")
        with self.assertRaises(ValidationFailed):
            self.service.complete_shard("w1", "task-1", "clean-a", shard["lease_seq"], "not-hex")
        with self.assertRaises(ValidationFailed):
            self.service.complete_shard("w1", "task-1", "clean-a", "一", digest("x"))
        with self.assertRaises(NotFound):
            self.service.claim_shard("w1", 60, "ghost-task")

    def test_claim_returns_definition_snapshot(self) -> None:
        shard = self._claim("w1", "clean-a")
        self.assertEqual(shard["attempt"], 1)
        self.assertEqual(shard["lease_seq"], 1)
        self.assertEqual(shard["rule_version"], "rules-v1")
        self.assertEqual(shard["rule_params"], {"bins": 8})
        self.assertEqual([entry["uri"] for entry in shard["input_refs"]], ["s3://corpus/part-1.jsonl"])
        self.assertEqual(len(shard["manifest_sha256"]), 64)

    def test_complete_unlocks_downstream_atomically(self) -> None:
        first = self._claim("w1", "clean-a")
        result = self._complete("w1", first)
        self.assertEqual(result["unlocked"], [])
        self.assertFalse(result["task_completed"])
        shards = {row["shard_key"]: row for row in self.service.list_shards("aud", "task-1")["shards"]}
        self.assertEqual(shards["stats"]["state"], "pending")
        self.assertEqual(shards["stats"]["pending_deps"], 1)
        second = self._claim("w2", "clean-b")
        result = self._complete("w2", second)
        self.assertEqual(result["unlocked"], ["stats"])
        shards = {row["shard_key"]: row for row in self.service.list_shards("aud", "task-1")["shards"]}
        self.assertEqual(shards["stats"]["state"], "ready")
        completion = self.connection.execute(
            "SELECT * FROM data_completions WHERE task_id='task-1' AND shard_key='clean-a'"
        ).fetchone()
        self.assertEqual(completion["worker_id"], "w1")
        self.assertEqual(completion["output_sha256"], digest("out:clean-a"))
        self.assertEqual(completion["task_revision"], 1)

    def test_task_completes_when_all_shards_succeed(self) -> None:
        self._complete("w1", self._claim("w1", "clean-a"))
        self._complete("w2", self._claim("w2", "clean-b"))
        final = self._complete("w1", self._claim("w1", "stats"))
        self.assertTrue(final["task_completed"])
        status = self.service.get_task("aud", "task-1")
        self.assertEqual(status["task"]["state"], "completed")
        self.assertEqual(status["counts"]["completed"], 3)
        self.assertIsNone(self.service.claim_shard("w1", 60, "task-1"))

    def test_late_result_cannot_overwrite_new_holder(self) -> None:
        stalled = self._claim("w1", "clean-a", lease_seconds=10)
        self.clock.advance(seconds=11)
        takeover = self._claim("w2", "clean-a")
        self.assertEqual(takeover["lease_seq"], stalled["lease_seq"] + 1)
        self.assertEqual(takeover["attempt"], 2)
        with self.assertRaises(InvalidState):
            self._complete("w1", stalled, "late")
        with self.assertRaises(InvalidState):
            self.service.fail_shard("w1", "task-1", "clean-a", stalled["lease_seq"], "迟到失败")
        self._complete("w2", takeover)
        shard = {row["shard_key"]: row for row in self.service.list_shards("aud", "task-1")["shards"]}["clean-a"]
        self.assertEqual(shard["state"], "succeeded")
        self.assertEqual(shard["output_sha256"], digest("out:clean-a"))
        count = self.connection.execute(
            "SELECT count(*) FROM data_completions WHERE task_id='task-1' AND shard_key='clean-a'"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_expired_lease_itself_cannot_complete(self) -> None:
        shard = self._claim("w1", "clean-a", lease_seconds=10)
        self.clock.advance(seconds=11)
        with self.assertRaises(InvalidState):
            self._complete("w1", shard)

    def test_fail_retries_with_backoff_then_manual(self) -> None:
        shard = self._claim("w1", "clean-a")
        self._claim("w2", "clean-b", lease_seconds=7200)
        failed = self.service.fail_shard("w1", "task-1", "clean-a", shard["lease_seq"], "临时故障")
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["available_at"], "2026-09-26T08:00:10Z")
        # 退避窗口内无可领取分片：clean-a 退避中，clean-b 已租出，stats 等待依赖
        self.assertIsNone(self.service.claim_shard("w3", 60, "task-1"))
        self.clock.advance(seconds=10)
        retry = self._claim("w3", "clean-a")
        self.assertEqual(retry["attempt"], 2)
        exhausted = self.service.fail_shard("w3", "task-1", "clean-a", retry["lease_seq"], "再次故障")
        self.assertEqual(exhausted["state"], "manual")
        # 进入人工处置后不再被领取
        self.clock.advance(seconds=3600)
        self.assertIsNone(self.service.claim_shard("w4", 60, "task-1"))

    def test_manual_resolve_retry_requeues(self) -> None:
        shard = self._claim("w1", "clean-a")
        self._claim("w9", "clean-b", lease_seconds=7200)
        self.service.fail_shard("w1", "task-1", "clean-a", shard["lease_seq"], "故障一")
        self.clock.advance(seconds=10)
        shard = self._claim("w1", "clean-a")
        self.service.fail_shard("w1", "task-1", "clean-a", shard["lease_seq"], "故障二")
        resolved = self.service.resolve_shard("sup", "task-1", "clean-a", "retry", note="补充资源后重试")
        self.assertEqual(resolved["state"], "ready")
        self.assertEqual(resolved["attempts"], 0)
        retry = self._claim("w2", "clean-a")
        self.assertEqual(retry["attempt"], 1)
        self._complete("w2", retry)

    def test_manual_resolve_complete_records_evidence(self) -> None:
        shard = self._claim("w1", "clean-a")
        self._claim("w9", "clean-b", lease_seconds=7200)
        self.service.fail_shard("w1", "task-1", "clean-a", shard["lease_seq"], "故障一")
        self.clock.advance(seconds=10)
        shard = self._claim("w1", "clean-a")
        self.service.fail_shard("w1", "task-1", "clean-a", shard["lease_seq"], "故障二")
        resolved = self.service.resolve_shard(
            "sup", "task-1", "clean-a", "complete", digest("manual:clean-a"), "人工核对完成"
        )
        self.assertEqual(resolved["state"], "succeeded")
        explained = self.service.explain_shard("aud", "task-1", "clean-a")
        self.assertEqual(explained["completions"][0]["worker_id"], "manual:sup")
        self.assertTrue(explained["completions"][0]["result"]["manual"])
        self.assertEqual(explained["events"][-1]["event_type"], "shard.manual_completed")
        with self.assertRaises(InvalidState):
            self.service.resolve_shard("sup", "task-1", "clean-a", "retry")

    def test_cancel_blocks_new_claims_but_keeps_evidence(self) -> None:
        first = self._claim("w1", "clean-a")
        self._complete("w1", first)
        inflight = self._claim("w2", "clean-b")
        cancelled = self.service.cancel_task("plan", "task-1", "需求变更")
        self.assertEqual(cancelled["task"]["state"], "cancelled")
        self.assertIsNone(self.service.claim_shard("w3", 60, "task-1"))
        # 在途租约仍可登记，证据继续保留
        self._complete("w2", inflight)
        status = self.service.get_task("aud", "task-1")
        self.assertEqual(status["counts"]["completed"], 2)
        explained = self.service.explain_shard("aud", "task-1", "clean-a")
        self.assertEqual(len(explained["completions"]), 1)
        self.assertEqual(explained["completions"][0]["output_sha256"], digest("out:clean-a"))
        with self.assertRaises(InvalidState):
            self.service.cancel_task("plan", "task-1")

    def test_revise_rejects_stale_completion_and_resets(self) -> None:
        completed = self._claim("w1", "clean-a")
        self._complete("w1", completed)
        inflight = self._claim("w2", "clean-b")
        revised = self.service.revise_task(
            "plan", "task-1", 1, "rules-v2", {"bins": 16}, MANIFEST, SHARDS,
        )
        self.assertEqual(revised["task"]["revision"], 2)
        with self.assertRaises(InvalidState):
            self._complete("w2", inflight)
        shards = {row["shard_key"]: row for row in self.service.list_shards("aud", "task-1")["shards"]}
        self.assertEqual(shards["clean-a"]["state"], "ready")
        self.assertEqual(shards["clean-a"]["attempts"], 0)
        self.assertEqual(shards["clean-a"]["task_revision"], 2)
        # 旧版本完成证据仍然保留并可追溯
        explained = self.service.explain_shard("aud", "task-1", "clean-a")
        self.assertEqual(len(explained["completions"]), 1)
        self.assertEqual(explained["completions"][0]["task_revision"], 1)
        self.assertEqual(explained["completions"][0]["rule_version"], "rules-v1")

    def test_revise_unchanged_definition_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.revise_task("plan", "task-1", 1, "rules-v1", {"bins": 8}, MANIFEST, SHARDS)
        with self.assertRaises(InvalidState):
            self.service.revise_task("plan", "task-1", 99, "rules-v2", {}, MANIFEST, SHARDS)

    def test_explain_shard_provenance(self) -> None:
        explained = self.service.explain_shard("aud", "task-1", "stats")
        self.assertEqual(explained["origin"]["task_revision"], 1)
        self.assertEqual(explained["origin"]["rule_version"], "rules-v1")
        self.assertEqual(
            [dep["shard_key"] for dep in explained["dependencies"]], ["clean-a", "clean-b"]
        )
        self.assertIn("等待 2 个上游分片", explained["explanation"])
        self.assertIn("clean-a", explained["explanation"])
        with self.assertRaises(NotFound):
            self.service.explain_shard("aud", "task-1", "ghost")

    def test_role_separation(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_task("aud", "nope", "无权限", "v1", {}, MANIFEST, SHARDS)
        with self.assertRaises(Forbidden):
            self.service.cancel_task("sup", "task-1")
        with self.assertRaises(Forbidden):
            self.service.resolve_shard("plan", "task-1", "clean-a", "retry")
        with self.assertRaises(Forbidden):
            self.service.audit_trail("plan", "task-1")

    def test_audit_chain_valid(self) -> None:
        self._complete("w1", self._claim("w1", "clean-a"))
        trail = self.service.audit_trail("aud", "task-1")
        self.assertTrue(trail["chain_valid"])
        self.assertGreaterEqual(len(trail["events"]), 3)


class RestartRebuildTests(unittest.TestCase):
    def test_restart_rebuilds_state_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-test-") as temporary:
            database = Path(temporary) / "forge.sqlite3"
            clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
            connection = connect(database)
            service = ForgeService(connection, clock)
            service.create_user("plan", "规划", "planner")
            service.create_user("aud", "审计", "auditor")
            service.create_task("plan", "task-1", "任务", "rules-v1", {}, MANIFEST, SHARDS, RETRY_POLICY)
            shard = service.claim_shard("w1", 60, "task-1")
            service.complete_shard("w1", "task-1", shard["shard_key"], shard["lease_seq"], digest("out"), {})
            leased = service.claim_shard("w2", 60, "task-1")
            connection.close()

            connection = connect(database)
            service = ForgeService(connection, clock)
            status = service.get_task("aud", "task-1")
            self.assertEqual(status["counts"], {"pending": 1, "running": 1, "failed": 0, "completed": 1})
            explained = service.explain_shard("aud", "task-1", leased["shard_key"])
            self.assertEqual(explained["shard"]["lease_owner"], "w2")
            self.assertIn("w2", explained["explanation"])
            finished = service.complete_shard(
                "w2", "task-1", leased["shard_key"], leased["lease_seq"], digest("out-2"), {}
            )
            self.assertEqual(finished["unlocked"], ["stats"])
            connection.close()


class ForgeApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(ForgeService(self.connection))
        self.app.handle("POST", "/users", body=json.dumps(
            {"user_id": "plan", "display_name": "规划", "role": "planner"}
        ).encode())

    def tearDown(self) -> None:
        self.connection.close()

    def _create_task(self) -> None:
        response = self.app.handle(
            "POST", "/tasks", {"x-actor-id": "plan"},
            json.dumps({
                "task_id": "task-1", "name": "任务", "rule_version": "v1",
                "manifest": MANIFEST, "shards": SHARDS, "retry_policy": RETRY_POLICY,
            }).encode(),
        )
        self.assertEqual(response.status, 201, response.body)

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_claim_complete_roundtrip_over_http(self) -> None:
        self._create_task()
        claimed = self.app.handle(
            "POST", "/shards/claim", body=json.dumps({"worker_id": "w1", "lease_seconds": 30}).encode()
        )
        self.assertEqual(claimed.status, 200)
        shard = claimed.body["shard"]
        self.assertEqual(shard["shard_key"], "clean-a")
        completed = self.app.handle(
            "POST", f"/tasks/task-1/shards/{shard['shard_key']}/complete",
            body=json.dumps({
                "worker_id": "w1", "lease_seq": shard["lease_seq"],
                "output_sha256": digest("out"), "result": {"rows": 5},
            }).encode(),
        )
        self.assertEqual(completed.status, 200, completed.body)
        status = self.app.handle("GET", "/tasks/task-1", {"x-actor-id": "plan"})
        self.assertEqual(status.body["counts"]["completed"], 1)
        explained = self.app.handle("GET", "/tasks/task-1/shards/clean-a/explain", {"x-actor-id": "plan"})
        self.assertEqual(explained.status, 200)
        self.assertEqual(len(explained.body["completions"]), 1)

    def test_worker_endpoints_do_not_require_actor(self) -> None:
        self._create_task()
        claimed = self.app.handle("POST", "/shards/claim", body=json.dumps({"worker_id": "w1"}).encode())
        self.assertEqual(claimed.status, 200)
        failed = self.app.handle(
            "POST", f"/tasks/task-1/shards/{claimed.body['shard']['shard_key']}/fail",
            body=json.dumps({"worker_id": "w1", "lease_seq": 1, "error": "故障"}).encode(),
        )
        self.assertEqual(failed.status, 200)

    def test_error_shapes(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")
        response = self.app.handle("GET", "/tasks/task-1")
        self.assertEqual(response.status, 422)
        response = self.app.handle("GET", "/tasks/ghost", {"x-actor-id": "plan"})
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "not_found")
        response = self.app.handle("GET", "/nope")
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "route_not_found")


class ForgeAcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = acceptance_run(ROOT)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["task_state"], "completed")
        self.assertEqual(result["counts"]["completed"], 5)
        self.assertTrue(result["audit_chain_valid"])
        self.assertEqual(result["cancelled_task_completed_shards"], 1)
        self.assertEqual(result["schema"]["missing_tables"], [])


if __name__ == "__main__":
    unittest.main()
