"""把演练 JSONL 逐行喂给协调引擎，打印决定并核对关键语义。

用法：
    python3 replay.py examples
    python3 replay.py examples/handoff-does-not-drain.jsonl --state-dir /tmp/cg
    python3 replay.py examples --json
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

# 兼容 ``python -I /abs/path/replay.py``：隔离模式不会把脚本目录放上 sys.path。
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coordinator import Coordinator, dumps  # noqa: E402
from scenario_reader import load_events  # noqa: E402

ROOT = _ROOT


def _codes(decision):
    return [b["code"] for b in decision.get("blockers", [])]


def verify_scenario(name: str, coordinator: Coordinator, decisions: list[dict]) -> list[tuple[str, bool, str]]:
    """对两个内置演练资料给出机器可核对的期望。"""
    checks: list[tuple[str, bool, str]] = []

    def check(label, ok, detail=""):
        checks.append((label, bool(ok), detail))

    if name == "handoff-does-not-drain.jsonl":
        first_begin = decisions[7]
        check(
            "第8行 begin 被拒：新代次 yard#2 未暂停",
            not first_begin["accepted"]
            and any(
                b.get("code") == "pause_missing" and (b.get("service"), b.get("boot")) == ("yard", 2)
                for b in first_begin.get("blockers", [])
            ),
        )
        check(
            "第8行 begin 被拒：job-3 仍在 yard#1 保管、未结束",
            "work_outstanding" in _codes(first_begin),
        )
        check("第11行静止屏障成立并发出承诺", decisions[10]["accepted"] and decisions[10].get("commitment"))
        check("第12行 result unknown 被接受", decisions[11]["accepted"])
        inspect = decisions[13]["view"]
        check(
            "重启后 inspect 仍为 unknown 且有 result_unresolved 阻塞项",
            inspect["state"] == "unknown"
            and "result_unresolved" in [b["code"] for b in inspect["live_blockers"]],
        )
        check("第15行 committed v2 被接受", decisions[14]["accepted"])
        check("第16行 v1 resume 因版本陈旧被拒", (not decisions[15]["accepted"]) and decisions[15]["code"] == "stale_numbering_resume")
        check("第17行 v2 resume 被接受", decisions[16]["accepted"])
        view = coordinator.window_view("w1")
        check("desk#1 尚未恢复，故 restoration_complete=False", view["restoration_complete"] is False)

    elif name == "scope-expansion-and-overlap.jsonl":
        check("open a 求出扩张到 b", decisions[2]["resolution"]["expansion_required"] == ["b"])
        check("无关车队 c 普通请求可进", decisions[4]["view"]["ordinary_requests"] == "allow")
        check("值班人 approve_expansion 被接受", decisions[5]["accepted"])
        check("第8行 begin a 成功并覆盖 a,b/R", decisions[7]["accepted"] and decisions[7]["commitment"]["fleets"] == ["a", "b"])
        bb = decisions[8]
        check(
            "第9行 begin b 因共用登记簿 R 冲突被拒",
            not bb["accepted"] and "register_conflict" in _codes(bb),
        )
        check("第10行 未开始的 b 可 cancel", decisions[9]["accepted"])
        check("第11行 result aborted 被接受", decisions[10]["accepted"])
        check("第12行 按 v1 resume 被接受", decisions[11]["accepted"])
        check("第13行 车队 b 恢复正常作业", decisions[12]["view"]["ordinary_requests"] == "allow")

    elif name == "shared-residual-work.jsonl":
        check(
            "双方都改称 pending 为空后，首次 begin wx 仍因共享残余工作被拒",
            (not decisions[8]["accepted"]) and "work_outstanding" in _codes(decisions[8]),
        )
        check("台账保管代次 desk#1 的 finished 被接受", decisions[9]["accepted"])
        check("工作终结后 begin wx 成功", decisions[10]["accepted"] and bool(decisions[10].get("commitment")))
        check("重叠窗口 wy 的 begin 被 R 互斥拦截",
              (not decisions[11]["accepted"]) and "register_conflict" in _codes(decisions[11]))
        check("wy 尚未开始，cancel 成功", decisions[12]["accepted"])
        check("wx aborted 被接受", decisions[13]["accepted"])
        check("desk#1 按 v1 resume", decisions[14]["accepted"])
        check("yard#1 再按 v1 resume 后恢复完成",
              decisions[15]["accepted"] and decisions[15].get("restoration_complete") is True)

    return checks


def replay_path(path: Path, state_dir: Path | None = None, as_json: bool = False):
    kinds = set(json.loads((path.parent / "message_types.json").read_text(encoding="utf-8")))
    own_tmp = None
    if state_dir is None:
        own_tmp = tempfile.TemporaryDirectory(prefix="changeguard-replay-")
        durable: str | Path = own_tmp.name
    else:
        state_dir.mkdir(parents=True, exist_ok=True)
        durable = state_dir / path.stem
    coordinator = Coordinator(durable)
    decisions = []
    lines = []
    for event in load_events(path, kinds):
        d = coordinator.ingest(event)
        decisions.append(d)
        lines.append(d)

    checks = verify_scenario(path.name, coordinator, decisions)
    if as_json:
        return {"file": path.name, "decisions": decisions, "checks": [
            {"label": l, "ok": ok, "detail": det} for l, ok, det in checks
        ]}

    print(f"\n=== {path.name}（{len(decisions)} 条）===")
    for d in decisions:
        mark = "✓" if d["accepted"] else "✗"
        line = f"  L{d['seq']:<2} {mark} {d['kind']:<18} {d.get('summary', '')}"
        print(line)
        for b in d.get("blockers", []):
            print(f"        ! {b.get('code')}: {b.get('message', '')}")
    if checks:
        print("  -- 核对 --")
        allok = True
        for label, ok, _detail in checks:
            allok &= ok
            print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        print(f"  => {'全部符合预期' if allok else '存在不符合项'}")
    if own_tmp is not None:
        own_tmp.cleanup()
    return all(check[1] for check in checks) if checks else True


def main():
    parser = argparse.ArgumentParser(description="回放并核对更账窗口演练资料")
    parser.add_argument("path", type=Path, help="examples 目录或某个 .jsonl 文件")
    parser.add_argument("--state-dir", type=Path, default=None, help="持久日志根目录（默认临时目录）")
    parser.add_argument("--json", action="store_true", help="输出 JSON 而非人类可读文本")
    args = parser.parse_args()

    if args.path.is_dir():
        kinds_file = args.path / "message_types.json"
        files = sorted(args.path.glob("*.jsonl"))
        base = args.path
    else:
        files = [args.path]
        base = args.path.parent
        kinds_file = base / "message_types.json"

    if args.json:
        report = []
        ok_all = True
        for f in files:
            state_dir = args.state_dir
            report.append(replay_path(f, state_dir, as_json=True))
        print(dumps(report))
        return

    ok_all = True
    for f in files:
        state_dir = args.state_dir
        ok_all &= replay_path(f, state_dir)
    print("\n总体：", "全部通过 ✅" if ok_all else "有失败 ❌")
    raise SystemExit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
