"""本地仿真端：用真实形状的 member / 交接 / 回执消息打 HTTP 协调服务。

两种用法：

1) 一键自检（自动在后台线程起一个临时数据目录的服务）：
       python3 simulator.py --local

2) 对接已启动的服务：
       python3 server.py --port 8765
       python3 simulator.py --base-url http://127.0.0.1:8765

脚本会回放 examples 下两个演练案例、在关键时点探测数据面准入，
然后继续受理一个全新车队的更账，并再次扩展/查询，最后打印核对结果。
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scenario_reader import load_events  # noqa: E402

ROOT = _ROOT


class Client:
    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")

    def _request(self, method: str, path: str, payload=None):
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {error.code} {path}: {body}") from error

    def health(self):
        return self._request("GET", "/health")

    def wait_up(self, timeout: float = 5.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                return self.health()
            except Exception as error:  # noqa: BLE001
                last = error
                time.sleep(0.1)
        raise RuntimeError(f"服务未就绪: {last}")

    def create_case(self, name: str):
        return self._request("POST", "/api/cases", {"name": name})

    def event(self, case: str, envelope: dict):
        return self._request("POST", f"/api/cases/{case}/events", envelope)

    def intake(self, case: str, **payload):
        return self._request("POST", f"/api/cases/{case}/intake", payload)

    def get_window(self, case: str, wid: str):
        return self._request("GET", f"/api/cases/{case}/windows/{wid}")

    def get_fleet(self, case: str, fleet: str):
        return self._request("GET", f"/api/cases/{case}/fleets/{fleet}")

    def restart_case(self, case: str):
        return self._request("POST", f"/api/cases/{case}/restart", {})


def run_simulation(base_url: str, examples_dir: Path, verbose: bool = True):
    cli = Client(base_url)
    cli.wait_up()
    kinds = set(json.loads((examples_dir / "message_types.json").read_text(encoding="utf-8")))

    checks: list[tuple[str, bool, str]] = []

    def check(label, ok, detail=""):
        checks.append((label, bool(ok), detail))
        if verbose:
            print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail and not ok else ""))

    def replay_case(filename: str, case: str):
        path = examples_dir / filename
        rows = list(load_events(path, kinds))
        decisions = []
        for row in rows:
            d = cli.event(case, row)
            decisions.append(d)
        return rows, decisions

    if verbose:
        print("\n── 案例一：handoff-does-not-drain（交接不等于排空）──")
    handoff_rows = list(load_events(examples_dir / "handoff-does-not-drain.jsonl", kinds))
    d1 = []
    probe_old = probe_new = probe_unknown = None
    for idx, row in enumerate(handoff_rows):
        d = cli.event("handoff", row)
        d1.append(d)
        if idx == 10:  # 第 11 行第二次 begin 成功，正处于 changing
            probe_old = cli.intake("handoff", fleet="a", numbering="v1")
            probe_new = cli.intake("handoff", fleet="a", client_authorized_new=True)
        if idx == 13:  # 第 14 行 inspect：restart 之后仍为 unknown
            probe_unknown = cli.intake("handoff", fleet="a", numbering="v1")

    fb = d1[7]
    check("首次 begin 被拒", not fb["accepted"] and fb["code"] == "begin_denied")
    check("阻塞项含新代次未暂停 + 工作未结束",
          {"pause_missing", "work_outstanding"} <= {b["code"] for b in fb["blockers"]})

    check("更账期间旧编号请求被拒/提示重试", probe_old and probe_old["decision"] == "reject_old_number",
          probe_old and probe_old["decision"])
    check("有权提交新编号的客户端也要等结果", probe_new and probe_new["decision"] == "wait_for_result",
          probe_new and probe_new["decision"])

    inspect_unknown = d1[13]["view"]
    check("重启后 inspect 仍为 unknown 且有实际阻塞项",
          inspect_unknown["state"] == "unknown"
          and any(b["code"] == "result_unresolved" for b in inspect_unknown["live_blockers"]))
    check("结果未知时继续冻结、提示稍后重试",
          probe_unknown and probe_unknown["decision"] == "retry_later",
          probe_unknown and probe_unknown["decision"])

    check("committed v2 被接受", d1[14]["accepted"])
    check("v1 resume 被判定陈旧并拒绝",
          (not d1[15]["accepted"]) and d1[15]["code"] == "stale_numbering_resume")
    check("v2 resume 被接受", d1[16]["accepted"])
    yard = cli.intake("handoff", fleet="a", numbering="v2", service="yard")
    desk = cli.intake("handoff", fleet="a", numbering="v2", service="desk")
    check("yard#2 已按 v2 放行", yard["decision"] == "allow", yard["decision"])
    check("desk#1 未恢复，仍在恢复中（不会提前接单）", desk["decision"] == "retry_restoring", desk["decision"])

    # 再打一次“真实对象重建”的重启端点，证明磁盘日志可恢复。
    reloaded = cli.restart_case("handoff")
    w = cli.get_window("handoff", "w1")
    check("HTTP /restart 后窗口仍为 committed/v2",
          reloaded["accepted"] and w["state"] == "committed" and w["determined_numbering"] == "v2")

    if verbose:
        print("\n── 案例二：scope-expansion-and-overlap（扩张与登记簿重叠）──")
    _rows2, d2 = replay_case("scope-expansion-and-overlap.jsonl", "expansion")
    check("open a 要求扩张到 b", d2[2]["resolution"]["expansion_required"] == ["b"])
    check("等待期间无关车队 c 正常作业", d2[4]["view"]["ordinary_requests"] == "allow")
    check("begin a 成功", d2[7]["accepted"] and bool(d2[7].get("commitment")))
    bb = d2[8]
    check("begin b 被共用登记簿 R 互斥拦截",
          (not bb["accepted"]) and "register_conflict" in {b["code"] for b in bb["blockers"]})
    check("未开始的 b 可 cancel", d2[9]["accepted"])
    check("aborted 后 v1 resume 被接受", d2[11]["accepted"])
    fleet_b = cli.get_fleet("expansion", "b")
    check("结束后车队 b 恢复正常", fleet_b["ordinary_requests"] == "allow")

    if verbose:
        print("\n── 续演：服务不重启，继续受理全新车队 d ──")
    cli.create_case("onboarding")
    cli.event("onboarding", {"at": 100, "kind": "member", "record": {
        "service": "nova", "boot": 1, "fleets": ["d"], "registers": ["S"],
        "pause_groups": [["d"]], "new_work": True,
        "downstreams": [{"service": "gateway", "boot": 3}],
    }})
    check("新车队 d 拓扑可见且可作业", cli.get_fleet("onboarding", "d")["ordinary_requests"] == "allow")
    cli.event("onboarding", {"at": 101, "kind": "open", "record": {"window": "wd", "fleets": ["d"]}})
    cli.event("onboarding", {"at": 102, "kind": "paused",
                             "record": {"window": "wd", "service": "nova", "boot": 1, "pending": []}})
    begun = cli.event("onboarding", {"at": 103, "kind": "begin",
                                     "record": {"window": "wd", "actor": "fleet-lead-li"}})
    check("车队负责人在屏障成立后持承诺开始", begun["accepted"] and begun["commitment"]["id"])
    check("更账中旧编号被拒", cli.intake("onboarding", fleet="d", numbering="v1")["decision"] == "reject_old_number")
    check("新编号授权客户端等待结果",
          cli.intake("onboarding", fleet="d", client_authorized_new=True)["decision"] == "wait_for_result")
    cli.event("onboarding", {"at": 104, "kind": "result",
                             "record": {"window": "wd", "state": "committed", "new_numbering": "v9"}})
    check("v9 已提交但服务未恢复前重试",
          cli.intake("onboarding", fleet="d", numbering="v9")["decision"] == "retry_restoring")
    stale = cli.event("onboarding", {"at": 105, "kind": "resume",
                                     "record": {"window": "wd", "service": "nova", "boot": 1, "numbering": "v1"}})
    check("旧版本 resume 再被拒", (not stale["accepted"]) and stale["code"] == "stale_numbering_resume")
    cli.event("onboarding", {"at": 106, "kind": "resume",
                             "record": {"window": "wd", "service": "nova", "boot": 1, "numbering": "v9"}})
    check("v9 resume 后新车队 d 按新版本放行",
          cli.intake("onboarding", fleet="d", numbering="v9")["decision"] == "allow")

    if verbose:
        print("\n── 再扩展：运行中接入另一个全新车队 e，并受理其更账范围/查询 ──")
    cli.event("onboarding", {"at": 200, "kind": "member", "record": {
        "service": "orion", "boot": 1, "fleets": ["e"], "registers": ["T2"],
        "pause_groups": [["e"]], "new_work": True}})
    check("第二支新车队 e 可查询且可作业", cli.get_fleet("onboarding", "e")["ordinary_requests"] == "allow")
    opened_e = cli.event("onboarding", {"at": 201, "kind": "open",
                                        "record": {"window": "we", "fleets": ["e"], "expand_accepted": False}})
    check("车队 e 的新范围已受理，begin 前如实列出 pause_missing",
          opened_e["accepted"] and "pause_missing" in {b["code"] for b in opened_e["blockers"]})
    status_e = cli.get_window("onboarding", "we")
    check("状态查询可持续返回 we", status_e["id"] == "we" and status_e["state"] == "waiting")

    failed = [c for c in checks if not c[1]]
    if verbose:
        print(f"\n仿真核对：{len(checks) - len(failed)}/{len(checks)} 通过")
        if failed:
            print("失败项：")
            for label, _ok, detail in failed:
                print(" -", label, detail)
    return len(failed) == 0, checks


def run_local(examples_dir: Path, port: int = 0):
    from server import build_server

    tmp = tempfile.TemporaryDirectory(prefix="changeguard-sim-")
    httpd = build_server("127.0.0.1", port, tmp.name, verbose=False)
    actual_port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{actual_port}"
    try:
        print(f"[local] 临时协调服务已在 {base_url} 启动（数据目录 {tmp.name}）")
        ok, _checks = run_simulation(base_url, examples_dir)
        return ok
    finally:
        httpd.shutdown()
        httpd.server_close()
        tmp.cleanup()


def main():
    parser = argparse.ArgumentParser(description="ChangeGuard 本地仿真端")
    parser.add_argument("--base-url", default="http://127.0.0.1:8765")
    parser.add_argument("--examples", type=Path, default=ROOT / "examples")
    parser.add_argument("--local", action="store_true", help="在进程内启动临时服务并一键演练")
    parser.add_argument("--port", type=int, default=0, help="--local 时使用的端口（0=随机）")
    args = parser.parse_args()

    if args.local:
        ok = run_local(args.examples, args.port)
    else:
        ok, _ = run_simulation(args.base_url, args.examples)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
