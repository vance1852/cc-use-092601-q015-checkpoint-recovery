"""数据加工断点恢复流水线的无第三方依赖 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from .errors import PipelineError, ValidationFailed
from .service import PipelineService
from .storage import connect, inspect_schema


class JsonApplication:
    """将 HTTP 路由映射到流水线服务，便于无网络单元测试。"""

    def __init__(self, service: PipelineService) -> None:
        self.service = service

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(
        self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b""
    ):
        normalized = {key.lower(): value for key, value in (headers or {}).items()}
        parsed = urlparse(target)
        path = parsed.path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        query = parse_qs(parsed.query)
        try:
            if method == "GET" and path == "/health":
                return _Response(200, {"status": "ok", "schema": inspect_schema(self.service.connection)})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}

            if method == "POST" and path == "/jobs":
                result = self.service.create_job(
                    self._actor(normalized),
                    payload["job_id"],
                    payload["rule"],
                    payload.get("shards", []),
                    manifest=payload.get("manifest"),
                    retry=payload.get("retry"),
                )
                return _Response(201, result)

            if method == "GET" and len(parts) == 2 and parts[0] == "jobs":
                return _Response(200, self.service.get_job(parts[1]))

            if method == "GET" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "status":
                state = query.get("state", [None])[0]
                return _Response(200, self.service.job_status(parts[1], state))

            if method == "POST" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "cancel":
                result = self.service.cancel_job(
                    self._actor(normalized), parts[1], payload["reason"]
                )
                return _Response(200, result)

            if method == "POST" and path == "/sweep":
                return _Response(200, self.service.sweep_cancelled())

            if method == "POST" and path == "/shards/claim":
                result = self.service.claim_shard(
                    payload["worker_id"],
                    int(payload.get("lease_seconds", 60)),
                    payload.get("job_id"),
                )
                return _Response(200, {"shard": result})

            if method == "POST" and path == "/shards/renew":
                result = self.service.renew_lease(
                    payload["worker_id"], payload["job_id"], payload["shard_key"],
                    int(payload["fence"]), int(payload["lease_seconds"]),
                )
                return _Response(200, result)

            # /jobs/{id}/shards/{key}/{action}
            if (
                len(parts) == 5 and parts[0] == "jobs" and parts[2] == "shards"
            ):
                job_id, shard_key, action = parts[1], parts[3], parts[4]
                if method == "GET" and action == "detail":
                    return _Response(200, self.service.shard_detail(job_id, shard_key))
                if method == "POST" and action == "complete":
                    result = self.service.complete_shard(
                        payload["worker_id"], job_id, shard_key, int(payload["fence"]),
                        payload["output"],
                        input_sha256=payload["input_sha256"],
                        rule_sha256=payload["rule_sha256"],
                        result=payload.get("result"),
                    )
                    return _Response(200, result)
                if method == "POST" and action == "fail":
                    result = self.service.fail_shard(
                        payload["worker_id"], job_id, shard_key, int(payload["fence"]),
                        payload["error"],
                        None if payload.get("retry_seconds") is None else int(payload["retry_seconds"]),
                    )
                    return _Response(200, result)
                if method == "POST" and action == "requeue":
                    result = self.service.requeue_shard(
                        self._actor(normalized), job_id, shard_key,
                        reset_attempts=bool(payload.get("reset_attempts", True)),
                    )
                    return _Response(200, result)
                if method == "POST" and action == "discard":
                    result = self.service.discard_shard(
                        self._actor(normalized), job_id, shard_key, payload["reason"]
                    )
                    return _Response(200, result)

            return _Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except PipelineError as exc:
            return _Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return _Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


class _Response:
    __slots__ = ("status", "body")

    def __init__(self, status: int, body: Mapping[str, Any]) -> None:
        self.status = status
        self.body = body


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "DataPipeline/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动数据加工断点恢复流水线 HTTP 服务")
    parser.add_argument("--database", type=Path, default=Path("data_pipeline.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8083)
    args = parser.parse_args(argv)
    connection = connect(args.database)
    application = JsonApplication(PipelineService(connection))
    server = ThreadingHTTPServer((args.host, args.port), make_handler(application))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
