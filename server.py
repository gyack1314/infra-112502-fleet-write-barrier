"""车队更账协调服务的本地 HTTP/JSON 端（仅标准库）。

启动：
    python3 server.py --host 127.0.0.1 --port 8765 --data-dir .runtime

设计为多“案例(case)”：每个演练文件/每套演练拓扑一个独立 Coordinator，
各自一个持久日志目录，避免两个场景的拓扑互相污染。

主要路由：
    GET  /health
    GET  /api/cases
    POST /api/cases                         {"name": "..."}
    POST /api/cases/<case>/events           完整信封或 {kind, record, at?}
    POST /api/cases/<case>/restart          用同一数据目录新建 Coordinator
    GET  /api/cases/<case>/snapshot
    GET  /api/cases/<case>/topology
    GET  /api/cases/<case>/windows[/<wid>]
    GET  /api/cases/<case>/fleets[/<fid>]
    POST /api/cases/<case>/intake           {"fleet","numbering?","client_authorized_new?","service?"}
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coordinator import Coordinator, InputError, dumps  # noqa: E402

DEFAULT_DATA_DIR = _ROOT / ".runtime"


class CaseManager:
    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.cases_dir = self.data_dir / "cases"
        self.cases_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.cases: dict[str, Coordinator] = {}
        for child in sorted(self.cases_dir.iterdir()):
            if child.is_dir() and (child / "journal.jsonl").exists():
                self.cases[child.name] = Coordinator(child)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self.cases)

    def get(self, name: str) -> Coordinator | None:
        with self._lock:
            return self.cases.get(name)

    def create(self, name: str) -> Coordinator:
        if not name or not all(ch.isalnum() or ch in "-_" for ch in name):
            raise InputError("case 名只能含字母数字、-、_")
        with self._lock:
            if name not in self.cases:
                self.cases[name] = Coordinator(self.cases_dir / name)
            return self.cases[name]

    def restart(self, name: str) -> Coordinator | None:
        with self._lock:
            if name not in self.cases and not (self.cases_dir / name).exists():
                return None
            # 丢弃内存对象，从同一 journal 目录全新加载 = 真实进程重启。
            c = Coordinator(self.cases_dir / name)
            self.cases[name] = c
            return c


def build_server(host: str, port: int, data_dir: str | Path, *, verbose: bool = False):
    manager = CaseManager(data_dir)

    class Handler(BaseHTTPRequestHandler):
        server_version = "ChangeGuard/1.0"

        def log_message(self, fmt, *args):  # noqa: A003
            if verbose:
                super().log_message(fmt, *args)

        # ----------------------------- helpers ----------------------------- #
        def _send(self, status: int, payload):
            body = dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            if not raw:
                return {}
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InputError(f"请求体不是合法 JSON: {error}") from error
            if not isinstance(value, dict):
                raise InputError("请求体必须是 JSON 对象")
            return value

        def _case_or_404(self, name):
            coord = manager.get(name)
            if coord is None:
                self._send(404, {"accepted": False, "error": f"case {name!r} 不存在"})
            return coord

        # ------------------------------ GET -------------------------------- #
        def do_GET(self):  # noqa: N802
            manager._lock.acquire()
            try:
                parts = [unquote(p) for p in urlparse(self.path).path.split("/") if p]
                if parts == []:
                    return self._send(200, self._descriptor())
                if parts == ["health"]:
                    return self._send(200, {"ok": True, "service": "changeguard", "cases": manager.names()})
                if parts == ["api", "cases"]:
                    return self._send(200, {"cases": manager.names()})
                if len(parts) >= 3 and parts[0] == "api" and parts[1] == "cases":
                    return self._handle_case_get(parts[2], parts[3:])
                return self._send(404, {"accepted": False, "error": "未知路径"})
            except InputError as error:
                return self._send(400, {"accepted": False, "error": str(error)})
            except Exception as error:  # pragma: no cover - 防御性
                return self._send(500, {"accepted": False, "error": repr(error)})
            finally:
                manager._lock.release()

        def _handle_case_get(self, name, rest):
            coord = self._case_or_404(name)
            if coord is None:
                return
            if rest == ["snapshot"]:
                return self._send(200, coord.snapshot())
            if rest == ["topology"]:
                return self._send(200, coord.topology_view())
            if rest == ["windows"]:
                return self._send(200, coord.windows_view())
            if len(rest) == 2 and rest[0] == "windows":
                view = coord.window_view(rest[1])
                if view is None:
                    return self._send(404, {"accepted": False, "error": "window_not_found"})
                return self._send(200, view)
            if rest == ["fleets"]:
                return self._send(200, coord.fleets_view())
            if len(rest) == 2 and rest[0] == "fleets":
                view = coord.fleet_view(rest[1])
                if view is None:
                    return self._send(404, {"accepted": False, "error": "fleet_not_found"})
                return self._send(200, view)
            if len(rest) == 2 and rest[0] == "work":
                view = coord.work_view(rest[1])
                if view is None:
                    return self._send(404, {"accepted": False, "error": "work_not_found"})
                return self._send(200, view)
            return self._send(404, {"accepted": False, "error": "未知案例子路径"})

        # ------------------------------ POST ------------------------------- #
        def do_POST(self):  # noqa: N802
            manager._lock.acquire()
            try:
                parts = [unquote(p) for p in urlparse(self.path).path.split("/") if p]
                body = self._read_json()
                if parts == ["api", "cases"]:
                    name = body.get("name") or f"case-{len(manager.names()) + 1}"
                    coord = manager.create(name)
                    return self._send(201, {"name": name, "created": True, "seq": coord.state["seq"]})
                if len(parts) >= 4 and parts[:2] == ["api", "cases"]:
                    name = parts[2]
                    action = parts[3]
                    coord = manager.get(name)
                    if coord is None:
                        # 允许直接向尚不存在的 case 发事件：自动建档，便于仿真端。
                        if action == "events":
                            coord = manager.create(name)
                        else:
                            return self._send(404, {"accepted": False, "error": f"case {name!r} 不存在"})
                    if action == "events":
                        return self._post_event(coord, body)
                    if action == "restart":
                        restarted = manager.restart(name)
                        return self._send(200, {
                            "accepted": True,
                            "restarted": True,
                            "snapshot": restarted.snapshot(),
                        })
                    if action == "intake":
                        return self._post_intake(coord, body)
                return self._send(404, {"accepted": False, "error": "未知 POST 路径"})
            except InputError as error:
                return self._send(400, {"accepted": False, "error": str(error)})
            except Exception as error:  # pragma: no cover
                return self._send(500, {"accepted": False, "error": repr(error)})
            finally:
                manager._lock.release()

        def _post_event(self, coord: Coordinator, body: dict):
            # 既接受完整信封 {at,kind,record}，也接受 {kind,record,at?}。
            if "kind" in body:
                envelope = {
                    "at": body.get("at"),
                    "kind": body["kind"],
                    "record": body.get("record", {}),
                }
            else:
                raise InputError("事件需要 kind 字段")
            try:
                decision = coord.ingest(envelope)
            except InputError as error:
                return self._send(400, {"accepted": False, "code": "invalid_envelope", "error": str(error)})
            # 业务策略拒绝仍以 200 + accepted:false 返回，便于仿真端继续演练。
            return self._send(200, decision)

        def _post_intake(self, coord: Coordinator, body: dict):
            fleet = body.get("fleet")
            if not isinstance(fleet, str) or not fleet:
                raise InputError("intake 需要 fleet 字段")
            result = coord.intake_probe(
                fleet,
                body.get("numbering"),
                client_authorized_new=bool(body.get("client_authorized_new", False)),
                service=body.get("service"),
            )
            return self._send(200, result)

        def _descriptor(self):
            return {
                "service": "changeguard-coordinator",
                "version": "1.0",
                "docs": "见 README.md；消息 kind 与 examples/message_types.json 一致",
                "endpoints": [
                    "GET /health",
                    "GET /api/cases",
                    "POST /api/cases",
                    "POST /api/cases/<case>/events",
                    "POST /api/cases/<case>/restart",
                    "POST /api/cases/<case>/intake",
                    "GET  /api/cases/<case>/snapshot",
                    "GET  /api/cases/<case>/topology",
                    "GET  /api/cases/<case>/windows[/<wid>]",
                    "GET  /api/cases/<case>/fleets[/<fid>]",
                ],
                "cases": manager.names(),
            }

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.case_manager = manager  # type: ignore[attr-defined]
    return httpd


def main():
    parser = argparse.ArgumentParser(description="车队更账窗口协调 HTTP 服务")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")))
    parser.add_argument("--data-dir", type=Path,
                        default=Path(os.environ.get("CHANGEGUARD_DATA", str(DEFAULT_DATA_DIR))))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    httpd = build_server(args.host, args.port, args.data_dir, verbose=args.verbose)
    print(f"ChangeGuard 协调服务监听 http://{args.host}:{args.port}  数据目录 {args.data_dir}")
    print("按 Ctrl+C 停止")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n停止中…")
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
