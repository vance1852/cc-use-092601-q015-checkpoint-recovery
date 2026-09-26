"""数据加工流水线 HTTP JSON 接口测试。"""

from __future__ import annotations

import json
import sqlite3
import unittest

from data_pipeline.api import JsonApplication
from data_pipeline.hashing import digest_value
from data_pipeline.service import PipelineService


RULE = {"rule_id": "clean", "version": 1, "definition": {"op": "clean"}}


def body(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


class PipelineApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(PipelineService(self.connection))
        shards = [
            {"shard_key": "a", "input": {"file": "a"}},
            {"shard_key": "b", "input": {"after": "a"}, "depends_on": ["a"]},
        ]
        response = self.app.handle(
            "POST", "/jobs", headers={"x-actor-id": "planner"},
            body=body({
                "job_id": "j1", "rule": RULE, "shards": shards,
                "manifest": [{"uri": "in.jsonl"}],
                "retry": {"max_attempts": 1, "backoff_base_seconds": 0, "backoff_max_seconds": 0},
            }),
        )
        self.assertEqual(response.status, 201)

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")
        self.assertEqual(response.body["schema"]["missing_tables"], [])

    def test_claim_complete_status_flow(self) -> None:
        claimed = self.app.handle("POST", "/shards/claim", body=body({"worker_id": "w1", "lease_seconds": 30}))
        self.assertEqual(claimed.status, 200)
        shard = claimed.body["shard"]
        self.assertEqual(shard["shard_key"], "a")
        completed = self.app.handle(
            "POST", f"/jobs/j1/shards/a/complete",
            body=body({
                "worker_id": "w1", "fence": shard["fence"],
                "output": {"sha256": digest_value({"o": 1}), "location": "oss://a"},
                "input_sha256": shard["input_sha256"],
                "rule_sha256": shard["rule_sha256"],
            }),
        )
        self.assertEqual(completed.status, 200)
        self.assertEqual(completed.body["unlocked"], ["b"])
        status = self.app.handle("GET", "/jobs/j1/status")
        self.assertEqual(status.status, 200)
        self.assertEqual(status.body["counts"]["done"], 1)

    def test_stale_lease_maps_to_conflict(self) -> None:
        self.app.handle("POST", "/shards/claim", body=body({"worker_id": "w1", "lease_seconds": 30}))
        response = self.app.handle(
            "POST", "/jobs/j1/shards/a/complete",
            body=body({
                "worker_id": "w1", "fence": 999,
                "output": {"sha256": "a" * 64},
                "input_sha256": "0" * 64, "rule_sha256": "1" * 64,
            }),
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(response.body["error"]["code"], "stale_lease")

    def test_input_changed_maps_to_conflict(self) -> None:
        shard = self.app.handle(
            "POST", "/shards/claim", body=body({"worker_id": "w1"})
        ).body["shard"]
        response = self.app.handle(
            "POST", "/jobs/j1/shards/a/complete",
            body=body({
                "worker_id": "w1", "fence": shard["fence"],
                "output": {"sha256": "a" * 64},
                "input_sha256": "0" * 64, "rule_sha256": shard["rule_sha256"],
            }),
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(response.body["error"]["code"], "input_changed")

    def test_cancel_and_filter(self) -> None:
        cancelled = self.app.handle(
            "POST", "/jobs/j1/cancel", headers={"x-actor-id": "planner"},
            body=body({"reason": "作废"}),
        )
        self.assertEqual(cancelled.status, 200)
        filtered = self.app.handle("GET", "/jobs/j1/status?state=cancelled")
        self.assertEqual(filtered.status, 200)
        self.assertTrue(all(s["runtime_state"] == "cancelled" for s in filtered.body["shards"]))
        self.assertGreaterEqual(len(filtered.body["shards"]), 1)

    def test_manual_requeue_and_discard_routes(self) -> None:
        shard = self.app.handle(
            "POST", "/shards/claim", body=body({"worker_id": "w1"})
        ).body["shard"]
        failed = self.app.handle(
            "POST", "/jobs/j1/shards/a/fail",
            body=body({"worker_id": "w1", "fence": shard["fence"], "error": "坏数据"}),
        )
        self.assertEqual(failed.body["state"], "manual")
        discarded = self.app.handle(
            "POST", "/jobs/j1/shards/a/discard", headers={"x-actor-id": "planner"},
            body=body({"reason": "无法修复"}),
        )
        self.assertEqual(discarded.status, 200)
        self.assertIn("b", discarded.body["cascaded_cancelled"])

    def test_unknown_route(self) -> None:
        response = self.app.handle("GET", "/nope")
        self.assertEqual(response.status, 404)

    def test_bad_json(self) -> None:
        response = self.app.handle("POST", "/jobs", body=b"not-json")
        self.assertEqual(response.status, 422)


if __name__ == "__main__":
    unittest.main()
