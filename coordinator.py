"""车队资料更账窗口的分布式静止屏障 —— 核心协调引擎。

只使用 Python 3.11 标准库。

设计要点（与仓库内演练资料逐条对应）：

* 真实写入拓扑：member 公布 service/boot、fleets、registers 以及不可拆分的
  pause_groups。窗口范围在 open 接受时求一次闭包，在每次 begin 前再求一次。
* 当前生产代次必须自己确认 paused；旧代次的确认不能当作新代次清净的证明。
  退任代次仍可对它承接过的 work 回报 finished。
* work 全局台账：paused.pending 只是某一时点的声明，transfer 移动保管责任，
  只有 finished（且来自当前保管代次）才真正终结。任何一方口头说“我这边没有了”
  都不会清空未完成工作，跨窗口共享的残余工作因此无法被分别抵赖掉。
* 共享服务若只能把 [a,b] 作为最小静止单元，则范围闭包会拉入 b；在值班人
  approve_expansion 或服务公布可独立停止的能力前，调度不会冻结 b。
* 同一共用 register 同时只允许一个窗口持有更账承诺（登记簿互斥）。
* 所有输入先写 append-only 日志再折叠成状态；restart 通过从日志重建来模拟，
  真实进程重启只要指向同一目录也会恢复全部冻结与未知结果。
* resume 只接受已经确定的编号版本：committed v2 后 v1 的恢复被拒绝；
  aborted 后沿用旧版本 v1；unknown 时版本未定，一律不得恢复接单。

引擎不依赖 HTTP，可直接 ``Coordinator().ingest(envelope)`` 驱动，也可被
server.py 包装成 JSON 服务。
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Iterable

__all__ = [
    "Coordinator",
    "CoordinatorError",
    "InputError",
    "KINDS",
    "BASE_NUMBERING",
    "replay_file",
]

KINDS = [
    "approve_expansion",
    "begin",
    "cancel",
    "finished",
    "inspect",
    "member",
    "open",
    "paused",
    "restart",
    "result",
    "resume",
    "transfer",
]

BASE_NUMBERING = "v1"

# 成功 begin 之后、完全恢复之前，登记簿锁都仍然持有。
LOCKED_PHASES = {"changing", "unknown", "committed", "aborted"}
ACTIVE_OTHER_PHASES = {"waiting", "changing", "unknown", "committed", "aborted"}


class CoordinatorError(Exception):
    """协调服务基础异常。"""


class InputError(CoordinatorError):
    """record 形状或必填字段不合法。"""


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #

def _sorted_set(value: Iterable[str] | None) -> list[str]:
    return sorted(set(value or []))


def _as_str_list(record: dict[str, Any], key: str, *, required: bool = False) -> list[str]:
    if key not in record:
        if required:
            raise InputError(f"record 缺少必填字段 {key!r}")
        return []
    value = record[key]
    if not isinstance(value, list) or not all(isinstance(x, str) and x for x in value):
        raise InputError(f"字段 {key!r} 必须是非空字符串组成的数组")
    return list(value)


def _boot_key(service: str, boot: int) -> str:
    return f"{service}#{boot}"


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=_json_default)


# --------------------------------------------------------------------------- #
# 状态构造
# --------------------------------------------------------------------------- #

def _fresh_state() -> dict[str, Any]:
    return {
        "seq": 0,
        "restarts": 0,
        # service -> {"current_boot": int|None, "boots": {boot_int: boot_dict}}
        "services": {},
        # window id -> window dict
        "windows": {},
        # work id -> work dict
        "work": {},
    }


def _new_boot(service: str, boot: int, seq: int) -> dict[str, Any]:
    return {
        "service": service,
        "boot": boot,
        "fleets": set(),
        "registers": set(),
        # list[frozenset[str]]：该服务声明的不可拆分静止组
        "pause_groups": [],
        "new_work": True,
        "downstreams": [],
        "first_seq": seq,
        "last_seq": seq,
    }


def _new_window(wid: str, seq: int, at: int | None) -> dict[str, Any]:
    return {
        "id": wid,
        "state": "waiting",  # waiting|changing|unknown|committed|aborted|restored|canceled
        "owner": "fleet-owner",
        "requested_fleets": set(),
        "requested_registers": set(),
        "expand_accepted_at_open": False,
        "expansion_approved": set(),
        "acceptance_resolution": None,
        "current_resolution": None,
        "topology_revisions": [],
        # "service#boot" -> {"seq","at","pending":[...]}
        "confirmations": {},
        "work_ids": set(),
        "commitment": None,
        "outcome": None,  # committed|aborted|canceled|None
        "base_numbering": BASE_NUMBERING,
        "new_numbering": None,
        "determined_numbering": None,
        # "service#boot" -> {"seq","at","numbering"}
        "resumes": {},
        "restoration_complete": False,
        "result_history": [],
        "begin_attempts": 0,
        "denials": [],
        "open_seq": seq,
        "open_at": at,
        "cancel_seq": None,
    }


def _new_work(work_id: str, seq: int) -> dict[str, Any]:
    return {
        "id": work_id,
        "status": "held",  # held|finished
        "holder": None,  # [service, boot]
        "finished_by": None,
        "finish_seq": None,
        "windows": set(),
        "history": [],
        "issues": [],
        "created_seq": seq,
    }


# --------------------------------------------------------------------------- #
# 协调器
# --------------------------------------------------------------------------- #

class Coordinator:
    """事件溯源的更账窗口协调器。

    参数
    ----
    directory:
        给定目录时，所有输入会追加到 ``<directory>/journal.jsonl``，
        构造时自动重放，因此进程重启后状态（含 unknown 冻结）不丢。
        为 None 时仅保存在内存中（仍可响应仿真 restart）。
    """

    def __init__(self, directory: str | Path | None = None) -> None:
        self.directory = Path(directory) if directory is not None else None
        self.journal_path: Path | None = None
        self._lock = threading.RLock()
        self._log: list[dict[str, Any]] = []
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.journal_path = self.directory / "journal.jsonl"
            if self.journal_path.exists():
                for line in self.journal_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        self._log.append(json.loads(line))
        self.state = _fresh_state()
        self._replay_log()

    # ----------------------------- 公共入口 ----------------------------- #

    def submit(
        self,
        kind: str,
        record: dict[str, Any] | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        """构造并提交一条消息；at 缺省取当前秒。"""
        if at is None:
            at = int(time.time())
        return self.ingest({"at": at, "kind": kind, "record": record or {}})

    def ingest(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """处理一条 ``{"at", "kind", "record"}`` 信封并返回决定。"""
        with self._lock:
            kind, record, at = self._validate_envelope(envelope)
            env = {"at": at, "kind": kind, "record": record}
            self._append(env)
            seq = len(self._log)
            self.state["seq"] = seq

            if kind == "restart":
                # 真正从持久日志重建内存状态，模拟协调端重启。
                if self.journal_path is not None:
                    self._log = [
                        json.loads(line)
                        for line in self.journal_path.read_text(
                            encoding="utf-8"
                        ).splitlines()
                        if line.strip()
                    ]
                self._replay_log()
                decision: dict[str, Any] = {
                    "accepted": True,
                    "summary": "协调端已从持久日志恢复，冻结与窗口承诺仍然有效",
                    "recovered": {
                        "windows": len(self.state["windows"]),
                        "services": len(self.state["services"]),
                        "work_items": len(self.state["work"]),
                        "restarts": self.state["restarts"],
                        "frozen_windows": [
                            w["id"]
                            for w in self.state["windows"].values()
                            if w["state"] in LOCKED_PHASES
                        ],
                    },
                }
            else:
                try:
                    decision = self._dispatch(kind, record, seq, at)
                except InputError as error:
                    decision = {
                        "accepted": False,
                        "code": "invalid_record",
                        "summary": str(error),
                        "blockers": [{"code": "invalid_record", "message": str(error)}],
                    }
            decision.setdefault("accepted", True)
            out = {"seq": seq, "at": at, "kind": kind, **decision}
            return out

    # ----------------------------- 持久化 ----------------------------- #

    def _append(self, env: dict[str, Any]) -> None:
        self._log.append(env)
        if self.journal_path is not None:
            with self.journal_path.open("a", encoding="utf-8") as stream:
                stream.write(dumps(env) + "\n")
                stream.flush()
                os.fsync(stream.fileno())

    def _replay_log(self) -> None:
        state = _fresh_state()
        self.state = state
        for seq, env in enumerate(self._log, 1):
            state["seq"] = seq
            kind = env["kind"]
            record = env.get("record", {})
            at = env.get("at")
            if kind == "restart":
                state["restarts"] += 1
                continue
            try:
                self._dispatch(kind, record, seq, at)
            except InputError:
                # 非法输入在首次提交时已被拒绝且不产生部分变更
                # （各 handler 均先校验后写入），重放时安全跳过。
                continue

    # ----------------------------- 校验/分发 ----------------------------- #

    @staticmethod
    def _validate_envelope(envelope: Any) -> tuple[str, dict[str, Any], int | None]:
        if not isinstance(envelope, dict):
            raise InputError("信封必须是对象")
        kind = envelope.get("kind")
        if kind not in KINDS:
            raise InputError(f"未登记的消息名称: {kind!r}")
        record = envelope.get("record", {})
        if not isinstance(record, dict):
            raise InputError("record 必须为对象")
        at = envelope.get("at")
        if at is not None and (type(at) is not int or at < 0):
            raise InputError("at 必须是非负整数或省略")
        return kind, record, at

    def _dispatch(
        self, kind: str, r: dict[str, Any], seq: int, at: int | None
    ) -> dict[str, Any]:
        handler = getattr(self, f"_h_{kind}")
        return handler(r, seq, at)

    # ------------------------------------------------------------------ #
    # member：发布拓扑
    # ------------------------------------------------------------------ #

    def _h_member(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        service = r.get("service")
        if not isinstance(service, str) or not service:
            raise InputError("member.service 必须是非空字符串")
        boot = r.get("boot")
        if type(boot) is not int or boot <= 0:
            raise InputError("member.boot 必须是正整数生产代次")
        fleets = set(_as_str_list(r, "fleets"))
        registers = set(_as_str_list(r, "registers"))

        groups_raw = r.get("pause_groups", [])
        if not isinstance(groups_raw, list):
            raise InputError("pause_groups 必须是数组的数组")
        pause_groups: list[frozenset[str]] = []
        for group in groups_raw:
            if not isinstance(group, list) or not all(
                isinstance(x, str) and x for x in group
            ):
                raise InputError("pause_groups 的每一项必须是非空字符串数组")
            pause_groups.append(frozenset(group))

        new_work = r.get("new_work", True)
        if not isinstance(new_work, bool):
            raise InputError("new_work 必须是布尔值")
        downstreams = r.get("downstreams", [])
        if not isinstance(downstreams, list):
            raise InputError("downstreams 必须是数组")

        svc = self.state["services"].setdefault(
            service, {"current_boot": None, "boots": {}}
        )
        boot_info = svc["boots"].get(boot)
        if boot_info is None:
            boot_info = _new_boot(service, boot, seq)
            svc["boots"][boot] = boot_info
        # 允许同一代次重新公布最新拓扑/静止能力。
        boot_info["fleets"] = fleets
        boot_info["registers"] = registers
        boot_info["pause_groups"] = pause_groups
        boot_info["new_work"] = new_work
        boot_info["downstreams"] = downstreams
        boot_info["last_seq"] = seq

        if svc["current_boot"] is None or boot >= svc["current_boot"]:
            svc["current_boot"] = boot

        return {
            "accepted": True,
            "summary": (
                f"服务 {service} 代次 #{boot} 公布：车队 {_sorted_set(fleets)}、"
                f"登记簿 {_sorted_set(registers)}、最小静止组 "
                f"{[sorted(g) for g in pause_groups] or '未声明(按单车队)'}、"
                f"new_work={new_work}"
            ),
            "topology": self.topology_view(),
        }

    # ------------------------------------------------------------------ #
    # open / approve_expansion / cancel
    # ------------------------------------------------------------------ #

    def _h_open(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        if not isinstance(wid, str) and not wid:
            raise InputError("open.window 必须是非空字符串")
        if not isinstance(wid, str) or not wid:
            raise InputError("open.window 必须是非空字符串")
        if wid in self.state["windows"]:
            return {
                "accepted": False,
                "code": "duplicate_window",
                "summary": f"窗口 {wid} 已存在，不能重复 open",
                "blockers": [{"code": "duplicate_window", "window": wid}],
            }
        fleets = set(_as_str_list(r, "fleets"))
        registers = set(_as_str_list(r, "registers"))
        if not fleets and not registers:
            raise InputError("open 至少要引用一个 fleet 或 register")
        expand_accepted = r.get("expand_accepted", False)
        if not isinstance(expand_accepted, bool):
            raise InputError("expand_accepted 必须是布尔值")

        w = _new_window(wid, seq, at)
        w["requested_fleets"] = fleets
        w["requested_registers"] = registers
        w["owner"] = r.get("owner") or r.get("actor") or "fleet-owner"
        w["expand_accepted_at_open"] = expand_accepted

        resolution = self.resolve_topology(fleets, registers)
        w["acceptance_resolution"] = resolution
        w["current_resolution"] = resolution
        w["topology_revisions"].append({"seq": seq, "at": at, "resolution": resolution})

        # open 时 expand_accepted=true 表示值班负责人已接受当时求出的扩大范围。
        if expand_accepted:
            w["expansion_approved"] |= set(resolution["expansion_required"])
        extra_fleets = _as_str_list(r, "approve_fleets")
        if extra_fleets:
            w["expansion_approved"] |= set(extra_fleets)

        self.state["windows"][wid] = w
        gate = self.evaluate_begin(wid)
        return {
            "accepted": True,
            "summary": (
                f"窗口 {wid} 已受理，请求车队 {_sorted_set(fleets)}、"
                f"登记簿 {_sorted_set(registers)}；真实拓扑闭包车队 "
                f"{resolution['fleets']}"
                + (
                    f"，需值班接受扩大到 {resolution['expansion_required']}"
                    if resolution["expansion_required"]
                    and not self._expansion_satisfied(w, resolution)
                    else ""
                )
            ),
            "resolution": resolution,
            "can_begin": gate["can_begin"],
            "blockers": gate["blockers"],
            "window": self.window_view(wid),
        }

    def _h_approve_expansion(
        self, r: dict[str, Any], seq: int, at: int | None
    ) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w  # type: ignore[return-value]
        assert isinstance(w, dict)
        fleets = set(_as_str_list(r, "fleets", required=True))
        if w["state"] != "waiting":
            return {
                "accepted": False,
                "code": "already_begun",
                "summary": f"窗口 {wid} 已 {w['state']}，开始后不能再扩大范围",
                "blockers": [{"code": "already_begun", "state": w["state"]}],
            }
        w["expansion_approved"] |= fleets
        gate = self.evaluate_begin(wid)
        return {
            "accepted": True,
            "summary": f"值班人确认窗口 {wid} 可覆盖附加车队 {_sorted_set(fleets)}",
            "approved_fleets": _sorted_set(w["expansion_approved"]),
            "can_begin": gate["can_begin"],
            "blockers": gate["blockers"],
        }

    def _h_cancel(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w
        assert isinstance(w, dict)
        if w["state"] == "canceled":
            return {"accepted": True, "summary": f"窗口 {wid} 此前已撤回（幂等）"}
        if w["state"] != "waiting":
            return {
                "accepted": False,
                "code": "cannot_cancel_after_begin",
                "summary": (
                    f"窗口 {wid} 已成功 begin（状态 {w['state']}）；"
                    "开始后只能提交 committed/aborted/unknown 结果，不能撤回"
                ),
                "blockers": [
                    {"code": "cannot_cancel_after_begin", "state": w["state"]}
                ],
            }
        w["state"] = "canceled"
        w["outcome"] = "canceled"
        # 撤回后旧编号仍然有效；已暂停的参与方可用 v1 的 resume 恢复。
        w["determined_numbering"] = w["base_numbering"]
        w["cancel_seq"] = seq
        return {
            "accepted": True,
            "summary": f"等待中的窗口 {wid} 已撤回；此前编号 {w['base_numbering']} 仍有效",
        }

    # ------------------------------------------------------------------ #
    # paused / transfer / finished：静止与排空
    # ------------------------------------------------------------------ #

    def _h_paused(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w
        assert isinstance(w, dict)
        service, boot = self._require_service_boot(r)
        pending = r.get("pending", [])
        if not isinstance(pending, list) or not all(
            isinstance(x, str) and x for x in pending
        ):
            raise InputError("paused.pending 必须是 work 编号字符串数组")

        key = _boot_key(service, boot)
        w["confirmations"][key] = {"seq": seq, "at": at, "pending": list(pending)}

        svc = self.state["services"].get(service)
        current_boot = svc["current_boot"] if svc else None
        stale = current_boot is not None and boot != current_boot

        # pending 只用于“登记/ attest ”工作，绝不据此删除未完成工作。
        for work_id in pending:
            item = self.state["work"].get(work_id)
            if item is None:
                item = _new_work(work_id, seq)
                self.state["work"][work_id] = item
            item["windows"].add(wid)
            w["work_ids"].add(work_id)
            if item["holder"] is None:
                item["holder"] = [service, boot]
            elif tuple(item["holder"]) != (service, boot):
                item["issues"].append(
                    {
                        "code": "custody_claim_conflict",
                        "seq": seq,
                        "claimed_by": [service, boot],
                        "ledger_holder": list(item["holder"]),
                    }
                )
            item["history"].append(
                {
                    "type": "pending_claim",
                    "seq": seq,
                    "at": at,
                    "by": [service, boot],
                    "window": wid,
                }
            )

        gate = self.evaluate_begin(wid)
        return {
            "accepted": True,
            "summary": (
                f"{key} 确认已停止为窗口 {wid} 的目标范围接旧编号新单，"
                f"自报待处理 {list(pending)}"
                + ("；注意这不是当前生产代次，不能证明新代次已静止" if stale else "")
            ),
            "stale_generation": stale,
            "current_boot": current_boot,
            "can_begin": gate["can_begin"],
            "blockers": gate["blockers"],
        }

    def _h_transfer(
        self, r: dict[str, Any], seq: int, at: int | None
    ) -> dict[str, Any]:
        work_id = r.get("work")
        if not isinstance(work_id, str) or not work_id:
            raise InputError("transfer.work 必须是非空字符串")
        from_s = r.get("from")
        to_s = r.get("to")
        from_b = r.get("from_boot")
        to_b = r.get("to_boot")
        if not all(isinstance(x, str) and x for x in (from_s, to_s)):
            raise InputError("transfer.from/to 必须是非空服务名")
        if type(from_b) is not int or type(to_b) is not int:
            raise InputError("transfer.from_boot/to_boot 必须是整数代次")

        item = self.state["work"].get(work_id)
        if item is None:
            item = _new_work(work_id, seq)
            self.state["work"][work_id] = item

        # 保管责任移动到接收方；工作本身仍未结束，范围因此尚未静止。
        item["holder"] = [to_s, to_b]
        item["history"].append(
            {
                "type": "transfer",
                "seq": seq,
                "at": at,
                "work": work_id,
                "from": [from_s, from_b],
                "to": [to_s, to_b],
            }
        )
        return {
            "accepted": True,
            "summary": (
                f"工作 {work_id} 由 {from_s}#{from_b} 交接给 {to_s}#{to_b}；"
                "转出方已确认，但工作在接收方真正 finished 之前仍属未排空"
            ),
            "work": self.work_view(work_id),
        }

    def _h_finished(
        self, r: dict[str, Any], seq: int, at: int | None
    ) -> dict[str, Any]:
        work_id = r.get("work")
        if not isinstance(work_id, str) or not work_id:
            raise InputError("finished.work 必须是非空字符串")
        service, boot = self._require_service_boot(r)

        item = self.state["work"].get(work_id)
        if item is None:
            # 没有任何 pending/transfer 与之矛盾：接受回执并立账终结。
            item = _new_work(work_id, seq)
            item["holder"] = [service, boot]
            self.state["work"][work_id] = item

        receipt = {
            "type": "finished_receipt",
            "seq": seq,
            "at": at,
            "by": [service, boot],
        }
        item["history"].append(receipt)

        if item["status"] == "finished":
            return {
                "accepted": True,
                "summary": f"工作 {work_id} 此前已终结；{service}#{boot} 的回执作为重复/补充记录",
                "work": self.work_view(work_id),
            }

        credible = item["holder"] is None or tuple(item["holder"]) == (service, boot)
        if not credible:
            item["issues"].append(
                {
                    "code": "finish_not_custodian",
                    "seq": seq,
                    "by": [service, boot],
                    "holder": list(item["holder"]),
                }
            )
            return {
                "accepted": False,
                "code": "finish_rejected_not_holder",
                "summary": (
                    f"{service}#{boot} 回报 {work_id} 完成，但台账当前保管代次是 "
                    f"{item['holder'][0]}#{item['holder'][1]}；退出登记或非承接代次"
                    "的完成说法不能清空该工作"
                ),
                "blockers": [
                    {
                        "code": "finish_rejected_not_holder",
                        "work": work_id,
                        "by": [service, boot],
                        "holder": list(item["holder"]),
                    }
                ],
                "work": self.work_view(work_id),
            }

        item["status"] = "finished"
        item["finished_by"] = [service, boot]
        item["finish_seq"] = seq
        return {
            "accepted": True,
            "summary": (
                f"工作 {work_id} 由原承接/当前保管代次 {service}#{boot} 回报真正结束"
            ),
            "work": self.work_view(work_id),
        }

    # ------------------------------------------------------------------ #
    # begin / result / resume
    # ------------------------------------------------------------------ #

    def _h_begin(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w
        assert isinstance(w, dict)
        actor = r.get("actor", "operator")
        w["begin_attempts"] += 1

        if w["state"] == "canceled":
            return self._deny_begin(w, seq, "window_canceled", "窗口已撤回，不能 begin")
        if w["state"] in {"changing", "unknown", "committed", "aborted", "restored"}:
            return self._deny_begin(
                w,
                seq,
                "begin_already_held",
                f"窗口已持有有效承诺/处于 {w['state']}，不能重复 begin",
                include_commitment=True,
            )

        gate = self.evaluate_begin(wid, commit_resolution=True, seq=seq, at=at)
        if not gate["can_begin"]:
            w["denials"].append({"seq": seq, "at": at, "blockers": gate["blockers"]})
            return {
                "accepted": False,
                "code": "begin_denied",
                "summary": (
                    f"窗口 {wid} 尚无权开始更账："
                    + "；".join(b["code"] for b in gate["blockers"])
                ),
                "blockers": gate["blockers"],
                "can_begin": False,
                "window": self.window_view(wid),
            }

        resolution = gate["resolution"]
        commitment = {
            "id": f"commit-{wid}-{w['begin_attempts']}",
            "window": wid,
            "actor": actor,
            "issued_seq": seq,
            "issued_at": at,
            "fleets": list(resolution["fleets"]),
            "registers": list(resolution["registers"]),
            "generations": dict(resolution["current_generations"]),
            "base_numbering": w["base_numbering"],
        }
        w["commitment"] = commitment
        w["state"] = "changing"
        return {
            "accepted": True,
            "summary": (
                f"窗口 {wid} 静止屏障成立，{actor} 持有效窗口承诺 "
                f"{commitment['id']} 开始更账；范围 {commitment['fleets']} / "
                f"{commitment['registers']}"
            ),
            "commitment": commitment,
            "window": self.window_view(wid),
        }

    def _deny_begin(
        self,
        w: dict[str, Any],
        seq: int,
        code: str,
        message: str,
        *,
        include_commitment: bool = False,
    ) -> dict[str, Any]:
        blockers = [{"code": code, "message": message}]
        w["denials"].append({"seq": seq, "blockers": blockers})
        out: dict[str, Any] = {
            "accepted": False,
            "code": code,
            "summary": message,
            "blockers": blockers,
            "can_begin": False,
        }
        if include_commitment and w.get("commitment"):
            out["commitment"] = w["commitment"]
        return out

    def _h_result(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w
        assert isinstance(w, dict)
        state = r.get("state")
        if state not in {"committed", "aborted", "unknown"}:
            raise InputError("result.state 必须是 committed|aborted|unknown")

        if w["state"] == "waiting":
            return {
                "accepted": False,
                "code": "result_without_begin",
                "summary": f"窗口 {wid} 尚未成功 begin，不能记录更账结果",
                "blockers": [{"code": "result_without_begin"}],
            }
        # 同一明确结果的重复回执始终幂等（即使窗口已恢复完成）。
        if w.get("outcome") in {"committed", "aborted"} and state == w["outcome"]:
            return {"accepted": True, "summary": f"窗口 {wid} {state} 结果重复送达，幂等处理"}
        if w["state"] == "canceled":
            return {
                "accepted": False,
                "code": "window_canceled",
                "summary": f"窗口 {wid} 已撤回，不能再记录 result",
                "blockers": [{"code": "window_canceled"}],
            }
        if w["state"] == "restored":
            return {
                "accepted": False,
                "code": "window_closed",
                "summary": f"窗口 {wid} 已按 {w['determined_numbering']} 恢复完成，不接受不同结果",
                "blockers": [{"code": "window_closed", "state": w["state"]}],
            }
        if w["state"] in {"committed", "aborted"}:
            if state == "unknown":
                return {
                    "accepted": False,
                    "code": "outcome_already_determined",
                    "summary": (
                        f"窗口 {wid} 已有明确结果 {w['outcome']}"
                        f"（版本 {w['determined_numbering']}），不能回退为未知而使版本再次未定"
                    ),
                    "blockers": [{"code": "outcome_already_determined"}],
                }
            return {
                "accepted": False,
                "code": "conflicting_result",
                "summary": f"窗口 {wid} 已 {w['outcome']}（{w['determined_numbering']}），不能改为 {state}",
                "blockers": [{"code": "conflicting_result"}],
            }

        record_entry = {"seq": seq, "at": at, "state": state}
        if state == "unknown":
            w["state"] = "unknown"
            w["outcome"] = "unknown"
            w["determined_numbering"] = None
            w["result_history"].append(record_entry)
            return {
                "accepted": True,
                "summary": (
                    f"窗口 {wid} 结果未知：继续冻结必要范围，版本未定，"
                    "值班人可 inspect 查看实际阻塞项；断开连接/重启都不会默认完成"
                ),
                "window": self.window_view(wid),
            }

        if state == "committed":
            new_numbering = r.get("new_numbering")
            if not isinstance(new_numbering, str) or not new_numbering:
                raise InputError("committed result 必须携带非空 new_numbering")
            w["state"] = "committed"
            w["outcome"] = "committed"
            w["new_numbering"] = new_numbering
            w["determined_numbering"] = new_numbering
            record_entry["new_numbering"] = new_numbering
            w["result_history"].append(record_entry)
            return {
                "accepted": True,
                "summary": f"窗口 {wid} 已提交，新编号版本 {new_numbering}；只接受按该版本的 resume",
                "determined_numbering": new_numbering,
            }

        # aborted：此前编号仍有效。
        w["state"] = "aborted"
        w["outcome"] = "aborted"
        w["determined_numbering"] = w["base_numbering"]
        w["result_history"].append(record_entry)
        return {
            "accepted": True,
            "summary": f"窗口 {wid} 已终止，原编号 {w['base_numbering']} 仍有效；按该版本 resume 后恢复",
            "determined_numbering": w["base_numbering"],
        }

    def _h_resume(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        w = self._require_window(wid)
        if isinstance(w, dict) and w.get("accepted") is False:
            return w
        assert isinstance(w, dict)
        service, boot = self._require_service_boot(r)
        numbering = r.get("numbering")
        if not isinstance(numbering, str) or not numbering:
            raise InputError("resume.numbering 必须是非空字符串")
        key = _boot_key(service, boot)

        determined = w["determined_numbering"]
        if determined is None:
            return {
                "accepted": False,
                "code": "numbering_not_determined",
                "summary": (
                    f"窗口 {wid} 尚未确定编号版本（状态 {w['state']}）；"
                    "结果未知/更账期间必须继续冻结，不能提前恢复接单"
                ),
                "blockers": [
                    {"code": "numbering_not_determined", "state": w["state"]}
                ],
            }
        if numbering != determined:
            return {
                "accepted": False,
                "code": "stale_numbering_resume",
                "summary": (
                    f"{key} 申请按 {numbering} 恢复，但窗口 {wid} 已确定版本为 "
                    f"{determined}；拒绝本次恢复，避免旧编号重新接单"
                ),
                "blockers": [
                    {
                        "code": "stale_numbering_resume",
                        "got": numbering,
                        "expected": determined,
                    }
                ],
            }

        w["resumes"][key] = {"seq": seq, "at": at, "numbering": numbering}
        # 恢复判定前再按真实拓扑刷新一次，确保 begin 之后新接管的代次也被计入。
        self.evaluate_begin(wid)
        complete, required = self._restoration_status(w)
        w["restoration_complete"] = complete
        if complete and w["state"] in {"committed", "aborted"}:
            w["state"] = "restored"
        return {
            "accepted": True,
            "summary": f"{key} 已按确定版本 {numbering} 恢复接单",
            "restoration_complete": complete,
            "required_participants": required,
        }

    # ------------------------------------------------------------------ #
    # inspect
    # ------------------------------------------------------------------ #

    def _h_inspect(self, r: dict[str, Any], seq: int, at: int | None) -> dict[str, Any]:
        wid = r.get("window")
        fleet = r.get("fleet")
        if wid is not None:
            view = self.window_view(wid)
            if view is None:
                return {
                    "accepted": False,
                    "code": "window_not_found",
                    "summary": f"未知窗口 {wid!r}",
                    "blockers": [{"code": "window_not_found"}],
                }
            return {"accepted": True, "view": view, "summary": f"窗口 {wid} 状态"}
        if fleet is not None:
            view = self.fleet_view(fleet)
            if view is None:
                return {
                    "accepted": False,
                    "code": "fleet_not_found",
                    "summary": f"从未在 member 拓扑中见到车队 {fleet!r}",
                    "blockers": [{"code": "fleet_not_found"}],
                }
            return {"accepted": True, "view": view, "summary": f"车队 {fleet} 状态"}
        raise InputError("inspect 需要 window 或 fleet 字段")

    # ------------------------------------------------------------------ #
    # 校验辅助
    # ------------------------------------------------------------------ #

    def _require_window(self, wid: Any) -> dict[str, Any]:
        if not isinstance(wid, str) or not wid:
            raise InputError("record.window 必须是非空字符串")
        w = self.state["windows"].get(wid)
        if w is None:
            return {
                "accepted": False,
                "code": "window_not_found",
                "summary": f"未知窗口 {wid!r}；请先 open",
                "blockers": [{"code": "window_not_found", "window": wid}],
            }
        return w

    @staticmethod
    def _require_service_boot(r: dict[str, Any]) -> tuple[str, int]:
        service = r.get("service")
        boot = r.get("boot")
        if not isinstance(service, str) or not service:
            raise InputError("record.service 必须是非空字符串")
        if type(boot) is not int or boot <= 0:
            raise InputError("record.boot 必须是正整数代次")
        return service, boot

    # ------------------------------------------------------------------ #
    # 拓扑闭包
    # ------------------------------------------------------------------ #

    def _current_boots(self) -> list[tuple[str, int, dict[str, Any]]]:
        result = []
        for service, svc in self.state["services"].items():
            cb = svc["current_boot"]
            if cb is None:
                continue
            result.append((service, cb, svc["boots"][cb]))
        return result

    def _register_fleet_index(self) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        reg_fleets: dict[str, set[str]] = {}
        fleet_regs: dict[str, set[str]] = {}
        for _s, _b, boot in self._current_boots():
            for reg in boot["registers"]:
                reg_fleets.setdefault(reg, set()).update(boot["fleets"])
            for fleet in boot["fleets"]:
                fleet_regs.setdefault(fleet, set()).update(boot["registers"])
        return reg_fleets, fleet_regs

    def resolve_topology(
        self,
        requested_fleets: set[str] | list[str],
        requested_registers: set[str] | list[str],
    ) -> dict[str, Any]:
        """按当前真实写入拓扑求静止范围闭包。"""
        req_f = set(requested_fleets)
        req_r = set(requested_registers)
        reg_fleets, fleet_regs = self._register_fleet_index()

        fleets: set[str] = set(req_f)
        for reg in req_r:
            fleets |= reg_fleets.get(reg, set())
        registers: set[str] = set(req_r)
        for fleet in req_f:
            registers |= fleet_regs.get(fleet, set())

        warnings: list[dict[str, Any]] = []
        pause_units: list[dict[str, Any]] = []

        # 不动点：共享服务的不可拆分静止组会拉入额外车队，
        # 新拉入的车队可能又由别的服务写出，继续闭包。
        for _ in range(10_000):
            before_f = set(fleets)
            before_r = set(registers)
            pause_units = []
            for service, boot_no, boot in self._current_boots():
                bf: set[str] = boot["fleets"]
                br: set[str] = boot["registers"]
                if not (bf & fleets) and not (br & registers):
                    continue

                registers |= br
                for fleet in bf:
                    registers |= fleet_regs.get(fleet, set())

                relevant = bf & fleets
                activated_groups: list[list[str]] = []
                groups: list[frozenset[str]] = boot["pause_groups"]
                for fleet in sorted(relevant):
                    containing = [g for g in groups if fleet in g]
                    if containing:
                        # 保守起见，所有包含该 fleet 的不可拆分组都要静止。
                        union_group: set[str] = set()
                        for group in containing:
                            union_group |= set(group)
                            activated_groups.append(sorted(group))
                        fleets |= union_group
                    elif groups:
                        # 声明了组但该 fleet 不在任何组里：无法证明可单独静止，
                        # 保守地把该服务整个公布范围作为最小静止单元。
                        fleets |= bf
                        warnings.append(
                            {
                                "code": "fleet_not_in_pause_group",
                                "service": service,
                                "boot": boot_no,
                                "fleet": fleet,
                                "minimum_scope": sorted(bf),
                            }
                        )
                    else:
                        # 未声明 pause_groups：默认具备按单车队独立静止的能力。
                        activated_groups.append([fleet])

                if activated_groups:
                    pause_units.append(
                        {
                            "service": service,
                            "boot": boot_no,
                            "groups": activated_groups,
                        }
                    )

            for fleet in list(fleets):
                registers |= fleet_regs.get(fleet, set())

            if fleets == before_f and registers == before_r:
                break

        selected: dict[str, int] = {}
        for service, boot_no, boot in self._current_boots():
            if (boot["fleets"] & fleets) or (boot["registers"] & registers):
                selected[service] = boot_no

        implicit_accepted: set[str] = set(req_f)
        for reg in req_r:
            implicit_accepted |= reg_fleets.get(reg, set())
        expansion_required = fleets - implicit_accepted

        return {
            "fleets": _sorted_set(fleets),
            "registers": _sorted_set(registers),
            "requested_fleets": _sorted_set(req_f),
            "requested_registers": _sorted_set(req_r),
            "implicit_accepted_fleets": _sorted_set(implicit_accepted),
            "expansion_required": _sorted_set(expansion_required),
            "services": _sorted_set(selected),
            "current_generations": dict(selected),
            "pause_units": pause_units,
            "warnings": warnings,
        }

    def _expansion_satisfied(self, w: dict[str, Any], resolution: dict[str, Any]) -> bool:
        required = set(resolution["expansion_required"])
        return required <= set(w["expansion_approved"])

    # ------------------------------------------------------------------ #
    # 恢复状态 / 登记簿锁
    # ------------------------------------------------------------------ #

    def _restoration_status(self, w: dict[str, Any]) -> tuple[bool, list[str]]:
        required: set[str] = set()
        if w.get("commitment"):
            for service, boot in w["commitment"]["generations"].items():
                required.add(_boot_key(service, boot))
        # begin 之后拓扑又换了当前代次，则新代次也必须按确定版本恢复。
        for service, svc in self.state["services"].items():
            cb = svc["current_boot"]
            if cb is not None:
                resolution = w.get("current_resolution") or w.get("acceptance_resolution")
                if resolution and service in resolution.get("services", []):
                    required.add(_boot_key(service, cb))
        if not required:
            return False, []
        determined = w["determined_numbering"]
        complete = True
        for key in required:
            resume = w["resumes"].get(key)
            if determined is None or not resume or resume["numbering"] != determined:
                complete = False
        return complete, sorted(required)

    def _valid_resume(self, w: dict[str, Any], service: str, boot: int) -> dict[str, Any] | None:
        determined = w.get("determined_numbering")
        if determined is None:
            return None
        resume = w["resumes"].get(_boot_key(service, boot))
        if resume and resume["numbering"] == determined:
            return resume
        return None

    def _active_pause_claim(
        self, service: str, boot: int
    ) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        """返回该代次尚未被“正确版本 resume”解除的 pause 确认及所属窗口。"""
        active = []
        key = _boot_key(service, boot)
        for w in self.state["windows"].values():
            conf = w["confirmations"].get(key)
            if not conf:
                continue
            if self._valid_resume(w, service, boot):
                continue
            active.append((w, conf))
        return active

    def _register_lock_owner(self, register: str, exclude_wid: str) -> dict[str, Any] | None:
        for wid, w in self.state["windows"].items():
            if wid == exclude_wid:
                continue
            if w["state"] not in LOCKED_PHASES:
                continue
            if w.get("restoration_complete"):
                continue
            regs = set()
            if w.get("commitment"):
                regs |= set(w["commitment"]["registers"])
            res = w.get("current_resolution") or w.get("acceptance_resolution")
            if res:
                regs |= set(res["registers"])
            if register in regs:
                return {"window": wid, "state": w["state"], "commitment": w.get("commitment")}
        return None

    # ------------------------------------------------------------------ #
    # begin 闸门（也用于 inspect 的 can_begin 实时计算）
    # ------------------------------------------------------------------ #

    def evaluate_begin(
        self, wid: str, *, commit_resolution: bool = False, seq: int | None = None, at: int | None = None
    ) -> dict[str, Any]:
        w = self.state["windows"].get(wid)
        if w is None:
            return {"can_begin": False, "blockers": [{"code": "window_not_found"}], "resolution": None}

        resolution = self.resolve_topology(
            w["requested_fleets"], w["requested_registers"]
        )
        blockers: list[dict[str, Any]] = []

        if commit_resolution:
            w["current_resolution"] = resolution
            w["topology_revisions"].append({"seq": seq, "at": at, "resolution": resolution})
        else:
            # 视图实时反映拓扑，但不污染已提交状态；展示用 resolution 直接返回。
            w["current_resolution"] = resolution

        # 1) 扩大范围必须经值班人接受。
        required_expansion = set(resolution["expansion_required"])
        unapproved = required_expansion - set(w["expansion_approved"])
        if unapproved:
            blockers.append(
                {
                    "code": "expansion_unapproved",
                    "message": (
                        "共享服务的最小静止组会拉入额外车队，调度不得擅自冻结；"
                        "等待值班人 approve_expansion 或可独立停止的新能力"
                    ),
                    "fleets": _sorted_set(unapproved),
                }
            )

        # 2) 同一共用登记簿的互斥。
        for register in resolution["registers"]:
            owner = self._register_lock_owner(register, wid)
            if owner is not None:
                blockers.append(
                    {
                        "code": "register_conflict",
                        "message": (
                            f"共用登记簿 {register} 已由窗口 {owner['window']}"
                            f"（{owner['state']}）持有更账承诺，不能在互不知情时一起更账"
                        ),
                        "register": register,
                        "other_window": owner["window"],
                        "other_state": owner["state"],
                    }
                )

        # 3) 每个当前生产代次都必须亲自确认停止接旧单。
        for service in sorted(resolution["current_generations"]):
            boot_no = resolution["current_generations"][service]
            key = _boot_key(service, boot_no)
            own = w["confirmations"].get(key)
            via = None
            if own is None:
                via = self._physical_stop_via(w, service, boot_no, resolution)
            if own is None and via is None:
                svc = self.state["services"][service]
                boot_info = svc["boots"][boot_no]
                blockers.append(
                    {
                        "code": "pause_missing",
                        "message": (
                            f"当前生产代次 {key} 尚未确认停止目标范围的旧编号新单"
                            + ("（其最近 member 仍声明 new_work=true）" if boot_info["new_work"] else "")
                        ),
                        "service": service,
                        "boot": boot_no,
                        "last_new_work": boot_info["new_work"],
                    }
                )

        # 4) 工作台账必须全部终结，且交接链可信。
        for work_id in sorted(w["work_ids"]):
            item = self.state["work"].get(work_id)
            if item is None:
                continue
            if item["status"] != "finished":
                blockers.append(
                    {
                        "code": "work_outstanding",
                        "message": (
                            f"旧工作 {work_id} 尚未真正结束"
                            + (f"，当前保管 {item['holder'][0]}#{item['holder'][1]}" if item["holder"] else "")
                            + "；本地 pending 为空不代表排空"
                        ),
                        "work": work_id,
                        "holder": list(item["holder"]) if item["holder"] else None,
                    }
                )
                continue
            bad = self._bad_handoff_for_window(item, wid)
            if bad:
                blockers.append(
                    {
                        "code": "ineligible_transfer",
                        "message": f"工作 {work_id} 曾交接给未确认/无能力的参与方，交接链不成立",
                        "work": work_id,
                        "issues": bad,
                    }
                )

        return {"can_begin": not blockers, "blockers": blockers, "resolution": resolution}

    def _physical_stop_via(
        self,
        w: dict[str, Any],
        service: str,
        boot_no: int,
        resolution: dict[str, Any],
    ) -> str | None:
        """该代次是否已因另一个有效窗口物理静止，且其范围覆盖当前需求。"""
        key = _boot_key(service, boot_no)
        boot = self.state["services"][service]["boots"][boot_no]
        need_fleets = boot["fleets"] & set(resolution["fleets"])
        need_regs = boot["registers"] & set(resolution["registers"])
        for other in self.state["windows"].values():
            if other["id"] == w["id"]:
                continue
            if other["state"] not in ACTIVE_OTHER_PHASES or other["state"] == "canceled":
                continue
            conf = other["confirmations"].get(key)
            if not conf or self._valid_resume(other, service, boot_no):
                continue
            ores = other.get("current_resolution") or other.get("acceptance_resolution")
            if not ores:
                continue
            of = set(ores["fleets"])
            or_ = set(ores["registers"])
            if need_fleets and need_fleets <= of:
                return other["id"]
            if need_regs and need_regs <= or_:
                return other["id"]
        return None

    def _bad_handoff_for_window(self, item: dict[str, Any], wid: str) -> list[dict[str, Any]]:
        """检查交接链中接收方是否在交接前已为该窗口确认 paused。"""
        bad: list[dict[str, Any]] = []
        for event in item["history"]:
            if event.get("type") != "transfer":
                continue
            to_s, to_b = event["to"]
            tx_seq = event["seq"]
            w = self.state["windows"].get(wid)
            if w is None:
                continue
            conf = w["confirmations"].get(_boot_key(to_s, to_b))
            if conf is None or conf["seq"] > tx_seq:
                bad.append(
                    {
                        "code": "recipient_not_confirmed_before_transfer",
                        "seq": tx_seq,
                        "to": [to_s, to_b],
                    }
                )
        return bad

    # ------------------------------------------------------------------ #
    # 数据面准入探测
    # ------------------------------------------------------------------ #

    def _boots_covering_fleet(
        self, fleet: str
    ) -> list[tuple[str, int, dict[str, Any]]]:
        reg_fleets, _ = self._register_fleet_index()
        covering = []
        for service, svc in self.state["services"].items():
            cb = svc["current_boot"]
            if cb is None:
                continue
            boot = svc["boots"][cb]
            in_register = any(
                fleet in reg_fleets.get(reg, set()) for reg in boot["registers"]
            )
            if fleet in boot["fleets"] or in_register:
                covering.append((service, cb, boot))
        return covering

    def _fleet_is_referenced(self, fleet: str) -> bool:
        for w in self.state["windows"].values():
            if fleet in w["requested_fleets"]:
                return True
            res = w.get("current_resolution") or w.get("acceptance_resolution")
            if res and fleet in set(res.get("fleets", [])):
                return True
        return False

    def fleet_view(self, fleet: str) -> dict[str, Any] | None:
        covering = self._boots_covering_fleet(fleet)
        if not covering and not self._fleet_is_referenced(fleet):
            return None

        services_view = []
        affecting: list[dict[str, Any]] = []
        for service, cb, boot in covering:
            holds = self._boot_holds_view(service, cb)
            accepted_versions = self._boot_accepted_versions(service, cb)
            services_view.append(
                {
                    "service": service,
                    "boot": cb,
                    "new_work_announced": boot["new_work"],
                    "accepted_numbering": accepted_versions,
                    "holds": holds,
                }
            )
            for hold in holds:
                affecting.append(
                    {
                        "window": hold["window"],
                        "state": hold["window_state"],
                        "determined_numbering": hold["determined_numbering"],
                    }
                )

        # 扩大范围咨询：窗口尚未批准拉入该 fleet 时，它仍可作业但能看到请求。
        advisories: list[dict[str, Any]] = []
        for w in self.state["windows"].values():
            res = w.get("current_resolution") or w.get("acceptance_resolution")
            if not res:
                continue
            if (
                fleet in res["expansion_required"]
                and fleet not in w["expansion_approved"]
                and w["state"] == "waiting"
            ):
                advisories.append(
                    {
                        "code": "expansion_requested",
                        "window": w["id"],
                        "message": "有窗口因共享服务最小静止组申请把本车队纳入范围，等待值班人接受；在此之前不冻结",
                    }
                )

        probe = self.intake_probe(fleet)
        ordinary = probe["decision"]
        return {
            "fleet": fleet,
            "serving_services": services_view,
            "affecting_windows": affecting,
            "advisories": advisories,
            "ordinary_requests": ordinary,
            "default_probe": probe,
        }

    def _boot_accepted_versions(self, service: str, boot: int) -> list[str]:
        versions = {BASE_NUMBERING}
        key = _boot_key(service, boot)
        for w in self.state["windows"].values():
            resume = w["resumes"].get(key)
            if resume and w.get("determined_numbering") == resume["numbering"]:
                versions = {resume["numbering"]}
        return _sorted_set(versions)

    def _boot_holds_view(self, service: str, boot: int) -> list[dict[str, Any]]:
        holds = []
        for w, conf in self._active_pause_claim(service, boot):
            resume = self._valid_resume(w, service, boot)
            holds.append(
                {
                    "window": w["id"],
                    "window_state": w["state"],
                    "determined_numbering": w["determined_numbering"],
                    "paused_at_seq": conf["seq"],
                    "valid_resume": resume,
                }
            )
        return holds

    def intake_probe(
        self,
        fleet: str,
        numbering: str | None = None,
        *,
        client_authorized_new: bool = False,
        service: str | None = None,
    ) -> dict[str, Any]:
        """数据面准入判断。

        decision 取值：
        allow / reject_old_number / reject_obsolete / wait_for_result /
        retry_later / retry_restoring / reject_new_not_committed /
        unknown_fleet / service_not_serving
        """
        covering = []
        for s, cb, _boot in self._boots_covering_fleet(fleet):
            if service is None or s == service:
                covering.append((s, cb))

        if service is not None and not any(s == service for s, _ in covering):
            return {
                "decision": "service_not_serving",
                "fleet": fleet,
                "service": service,
                "http_hint": 404,
                "reason": f"服务 {service} 不负责车队 {fleet}",
            }
        if not covering:
            return {
                "decision": "unknown_fleet",
                "fleet": fleet,
                "http_hint": 404,
                "reason": "拓扑中没有负责该车队的当前服务代次",
            }

        per_service = []
        for s, cb in covering:
            per_service.append(
                self._probe_one_service(fleet, s, cb, numbering, client_authorized_new)
            )

        if service is not None:
            return {"fleet": fleet, "service": service, **per_service[0], "per_service": per_service}

        # 聚合：写入拓扑上所有当前代次都放行才 allow。
        decisions = [p["decision"] for p in per_service]
        if all(d == "allow" for d in decisions):
            overall = "allow"
        elif any(d.startswith("reject") for d in decisions) and not any(
            d in {"retry_later", "retry_restoring", "wait_for_result"} for d in decisions
        ):
            overall = next(d for d in decisions if d.startswith("reject"))
        elif "wait_for_result" in decisions:
            overall = "wait_for_result"
        elif "retry_later" in decisions:
            overall = "retry_later"
        elif "retry_restoring" in decisions:
            overall = "retry_restoring"
        else:
            overall = decisions[0]
        return {
            "fleet": fleet,
            "numbering": numbering
            or next((p.get("numbering") for p in per_service if p.get("numbering")), BASE_NUMBERING),
            "client_authorized_new": client_authorized_new,
            "decision": overall,
            "http_hint": self._http_hint(overall),
            "per_service": per_service,
        }

    @staticmethod
    def _http_hint(decision: str) -> int:
        if decision == "allow":
            return 200
        if decision.startswith("reject"):
            return 409
        if decision == "unknown_fleet":
            return 404
        return 503

    def _probe_one_service(
        self,
        fleet: str,
        service: str,
        boot: int,
        numbering: str | None,
        client_authorized_new: bool,
    ) -> dict[str, Any]:
        key = _boot_key(service, boot)
        active = self._active_pause_claim(service, boot)

        # 没有任何“未正确恢复”的静止：按该代次已经生效的编号版本接单，
        # 但仍要拒绝已被 committed 新版本取代的旧编号。
        if not active:
            accepted = set(self._boot_accepted_versions(service, boot))
            current_version = sorted(accepted)[-1] if accepted else BASE_NUMBERING
            if numbering is None or numbering in accepted:
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "allow",
                    "numbering": numbering or current_version,
                }
            if numbering == BASE_NUMBERING and BASE_NUMBERING not in accepted:
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "reject_obsolete",
                    "reason": f"原编号 {BASE_NUMBERING} 已被 {current_version} 取代",
                }
            return {
                "service": service,
                "boot": boot,
                "decision": "reject_new_not_committed",
                "reason": f"当前生效版本为 {sorted(accepted)}，{numbering} 尚未经 committed 结果确定",
            }

        # 选择最严格的 hold 作为数据面当前状态。
        holds = [(w, self._valid_resume(w, service, boot)) for w, _conf in active]
        phase_order = {"unknown": 5, "changing": 4, "committed": 3, "aborted": 3, "waiting": 2, "canceled": 1}
        w, valid_resume = max(holds, key=lambda pair: phase_order.get(pair[0]["state"], 0))
        phase = w["state"]
        determined = w["determined_numbering"]
        requested = numbering or BASE_NUMBERING

        if phase == "unknown":
            view = self.window_view(w["id"])
            return {
                "service": service,
                "boot": boot,
                "decision": "retry_later",
                "retry_after": 30,
                "window": w["id"],
                "reason": "更账结果未知，冻结必要范围直至值班人录入明确结果",
                "blockers": view.get("live_blockers", []) if view else [],
            }
        if phase == "changing":
            if client_authorized_new:
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "wait_for_result",
                    "window": w["id"],
                    "reason": "已授权提交新编号的客户端也必须等待本次更账结果",
                }
            return {
                "service": service,
                "boot": boot,
                "decision": "reject_old_number",
                "http_hint": 409,
                "retry_after": 15,
                "window": w["id"],
                "reason": "更账期间旧编号请求被拒绝或请稍后重试",
            }
        if phase == "waiting":
            return {
                "service": service,
                "boot": boot,
                "decision": "retry_later",
                "retry_after": 20,
                "window": w["id"],
                "reason": "参与方已提前停止接旧单，窗口尚在等待静止屏障成立",
            }
        if phase == "canceled":
            return {
                "service": service,
                "boot": boot,
                "decision": "retry_restoring",
                "window": w["id"],
                "expected_numbering": BASE_NUMBERING,
                "reason": "窗口已撤回，等待参与方按 v1 resume 后恢复",
            }
        if phase == "committed":
            new_version = w["new_numbering"]
            if valid_resume and valid_resume["numbering"] == new_version:
                if requested == new_version:
                    return {"service": service, "boot": boot, "decision": "allow", "numbering": new_version}
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "reject_obsolete",
                    "window": w["id"],
                    "reason": f"旧编号已被 {new_version} 取代",
                }
            # 尚未按 v2 恢复本代次。
            if requested == new_version:
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "retry_restoring",
                    "retry_after": 10,
                    "window": w["id"],
                    "expected_numbering": new_version,
                    "reason": f"更账已提交为 {new_version}，本服务正在逐步恢复",
                }
            return {
                "service": service,
                "boot": boot,
                "decision": "reject_obsolete",
                "window": w["id"],
                "reason": f"旧编号已失效，应改用 {new_version}，并等待服务恢复",
            }
        if phase == "aborted":
            if valid_resume and valid_resume["numbering"] == BASE_NUMBERING:
                if requested == BASE_NUMBERING:
                    return {"service": service, "boot": boot, "decision": "allow", "numbering": BASE_NUMBERING}
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "reject_new_not_committed",
                    "window": w["id"],
                    "reason": "更账已终止，新编号未生效，继续使用 v1",
                }
            if requested == BASE_NUMBERING:
                return {
                    "service": service,
                    "boot": boot,
                    "decision": "retry_restoring",
                    "retry_after": 10,
                    "window": w["id"],
                    "expected_numbering": BASE_NUMBERING,
                    "reason": "更账已终止，等待本服务按 v1 恢复",
                }
            return {
                "service": service,
                "boot": boot,
                "decision": "reject_new_not_committed",
                "window": w["id"],
                "reason": "更账已终止，提议的新编号未获提交",
            }
        return {"service": service, "boot": boot, "decision": "retry_later", "window": w["id"]}

    # ------------------------------------------------------------------ #
    # 视图
    # ------------------------------------------------------------------ #

    def topology_view(self) -> dict[str, Any]:
        reg_fleets, fleet_regs = self._register_fleet_index()
        services = {}
        for service, svc in self.state["services"].items():
            services[service] = {
                "current_boot": svc["current_boot"],
                "boots": {
                    str(boot_no): {
                        "fleets": _sorted_set(b["fleets"]),
                        "registers": _sorted_set(b["registers"]),
                        "pause_groups": [sorted(g) for g in b["pause_groups"]],
                        "new_work": b["new_work"],
                        "downstreams": b["downstreams"],
                    }
                    for boot_no, b in sorted(svc["boots"].items())
                },
            }
        return {
            "services": services,
            "register_fleets": {r: _sorted_set(fs) for r, fs in sorted(reg_fleets.items())},
            "fleet_registers": {f: _sorted_set(rs) for f, rs in sorted(fleet_regs.items())},
        }

    def work_view(self, work_id: str) -> dict[str, Any] | None:
        item = self.state["work"].get(work_id)
        if item is None:
            return None
        return {
            "id": item["id"],
            "status": item["status"],
            "holder": list(item["holder"]) if item["holder"] else None,
            "finished_by": list(item["finished_by"]) if item["finished_by"] else None,
            "windows": _sorted_set(item["windows"]),
            "issues": item["issues"],
            "history": item["history"],
        }

    def window_view(self, wid: str) -> dict[str, Any] | None:
        w = self.state["windows"].get(wid)
        if w is None:
            return None
        gate = self.evaluate_begin(wid)
        can_begin = gate["can_begin"] if w["state"] == "waiting" else False
        blockers = gate["blockers"] if w["state"] == "waiting" else []

        # unknown 窗口的“实际阻塞项”：结果未决 + 尚未恢复的参与方。
        live_blockers: list[dict[str, Any]] = list(blockers)
        if w["state"] == "unknown":
            live_blockers.append(
                {
                    "code": "result_unresolved",
                    "message": "更账结果未知，必须保留冻结直到录入 committed 或 aborted",
                }
            )
        required = self._restoration_status(w)[1]
        missing_resume = []
        for key in required:
            resume = w["resumes"].get(key)
            if not resume or (
                w["determined_numbering"] is not None
                and resume["numbering"] != w["determined_numbering"]
            ):
                missing_resume.append(key)

        participants = []
        res = gate["resolution"]
        for service in sorted(res["current_generations"]):
            boot_no = res["current_generations"][service]
            key = _boot_key(service, boot_no)
            conf = w["confirmations"].get(key)
            via = None if conf else self._physical_stop_via(w, service, boot_no, res)
            resume = w["resumes"].get(key)
            participants.append(
                {
                    "service": service,
                    "boot": boot_no,
                    "confirmed_for_window": conf is not None,
                    "confirmation_seq": conf["seq"] if conf else None,
                    "physically_stopped_via": via,
                    "resume": resume,
                }
            )

        work = [self.work_view(x) for x in sorted(w["work_ids"])]
        begin_authorization = (
            {
                "authorized": True,
                "who": w["owner"],
                "action": "begin",
                "commitment_preview": {
                    "window": w["id"],
                    "fleets": res["fleets"],
                    "registers": res["registers"],
                    "generations": res["current_generations"],
                    "base_numbering": w["base_numbering"],
                },
            }
            if w["state"] == "waiting" and can_begin
            else {
                "authorized": False,
                "who": w["owner"],
                "state": w["state"],
                "blockers": blockers if w["state"] == "waiting" else [],
            }
        )
        return {
            "id": w["id"],
            "state": w["state"],
            "owner": w["owner"],
            "begin_authorization": begin_authorization,
            "requested_fleets": _sorted_set(w["requested_fleets"]),
            "requested_registers": _sorted_set(w["requested_registers"]),
            "scope": res,
            "expansion_approved": _sorted_set(w["expansion_approved"]),
            "can_begin": can_begin,
            "begin_blockers": blockers,
            "live_blockers": live_blockers,
            "participants": participants,
            "work": work,
            "commitment": w["commitment"],
            "outcome": w["outcome"],
            "base_numbering": w["base_numbering"],
            "new_numbering": w["new_numbering"],
            "determined_numbering": w["determined_numbering"],
            "resumes": w["resumes"],
            "required_participants": required,
            "missing_resume": missing_resume,
            "restoration_complete": w["restoration_complete"],
            "register_locks": [
                r
                for r in res["registers"]
                if (owner := self._register_lock_owner(r, wid)) is not None
            ],
            "begin_attempts": w["begin_attempts"],
            "result_history": w["result_history"],
        }

    def windows_view(self) -> dict[str, Any]:
        return {
            "windows": [self.window_view(wid) for wid in sorted(self.state["windows"])],
        }

    def fleets_view(self) -> dict[str, Any]:
        fleets: set[str] = set()
        for _s, _b, boot in self._current_boots():
            fleets |= boot["fleets"]
        return {"fleets": [self.fleet_view(f) for f in sorted(fleets)]}

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "seq": self.state["seq"],
                "restarts": self.state["restarts"],
                "topology": self.topology_view(),
                "windows": self.windows_view()["windows"],
                "fleets": self.fleets_view()["fleets"],
                "work": {wid: self.work_view(wid) for wid in sorted(self.state["work"])},
            }


# --------------------------------------------------------------------------- #
# 演练文件回放辅助
# --------------------------------------------------------------------------- #

def replay_file(path: str | Path, directory: str | Path | None = None) -> tuple[Coordinator, list[dict[str, Any]]]:
    """回放一个 JSONL 演练文件；directory 给定时走持久日志。"""
    import sys

    path = Path(path)
    sys.path.insert(0, str(Path(__file__).parent))
    from scenario_reader import load_events  # 标准读取器，复用契约校验

    kinds = set(json.loads((path.parent / "message_types.json").read_text(encoding="utf-8")))
    coordinator = Coordinator(directory)
    decisions = []
    for event in load_events(path, kinds):
        decisions.append(coordinator.ingest(event))
    return coordinator, decisions
