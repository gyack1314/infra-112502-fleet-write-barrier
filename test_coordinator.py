"""协调引擎规则测试：两个演练场景的关键决定 +  prose 中的边界规则。"""

import json
import tempfile
import unittest
from pathlib import Path

from coordinator import Coordinator
from scenario_reader import load_events

ROOT = Path(__file__).parent
EXAMPLES = ROOT / "examples"
KINDS = set(json.loads((EXAMPLES / "message_types.json").read_text(encoding="utf-8")))


def replay(name: str, directory: str | Path | None = None):
    c = Coordinator(directory)
    path = EXAMPLES / name
    decisions = [c.ingest(e) for e in load_events(path, KINDS)]
    return c, decisions


def codes(decision):
    return [b["code"] for b in decision.get("blockers", [])]


class HandoffDoesNotDrainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.c, self.d = replay("handoff-does-not-drain.jsonl", self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_begin_blocked_by_new_generation_and_open_work(self):
        first_begin = self.d[7]  # 第 8 行
        self.assertEqual(first_begin["kind"], "begin")
        self.assertFalse(first_begin["accepted"])
        self.assertIn("pause_missing", codes(first_begin))
        self.assertIn("work_outstanding", codes(first_begin))
        # pause_missing 指向 yard 的第 2 代次，而不是旧的 #1。
        pm = next(b for b in first_begin["blockers"] if b["code"] == "pause_missing")
        self.assertEqual((pm["service"], pm["boot"]), ("yard", 2))
        wo = next(b for b in first_begin["blockers"] if b["code"] == "work_outstanding")
        self.assertEqual(wo["work"], "job-3")
        self.assertEqual(wo["holder"], ["yard", 1])

    def test_second_begin_grants_commitment(self):
        second_begin = self.d[10]  # 第 11 行
        self.assertTrue(second_begin["accepted"], second_begin)
        self.assertIn("commitment", second_begin)
        self.assertEqual(second_begin["commitment"]["generations"]["yard"], 2)
        self.assertEqual(second_begin["commitment"]["generations"]["desk"], 1)
        # 在该事件之后、result 之前，窗口确实处于 changing。
        c = Coordinator()
        for e in list(load_events(EXAMPLES / "handoff-does-not-drain.jsonl", KINDS))[:11]:
            c.ingest(e)
        self.assertEqual(c.window_view("w1")["state"], "changing")

    def test_original_accepting_generation_finishes_after_supersession(self):
        finished = self.d[9]  # 第 10 行：yard#1 finish
        self.assertTrue(finished["accepted"])
        self.assertEqual(self.c.state["work"]["job-3"]["status"], "finished")

    def test_unknown_result_and_restart_keep_freeze_and_show_blocker(self):
        unknown = self.d[11]
        self.assertTrue(unknown["accepted"])
        restart = self.d[12]
        self.assertTrue(restart["accepted"])
        inspect = self.d[13]["view"]  # 第 14 行 inspect
        self.assertEqual(inspect["state"], "unknown")
        self.assertIsNone(inspect["determined_numbering"])
        self.assertTrue(inspect["commitment"])
        self.assertIn("result_unresolved", [b["code"] for b in inspect["live_blockers"]])
        # unknown 期间新旧编号都进不去（在第 14 行那个时点探测，而非最终状态）。
        c = Coordinator()
        for e in list(load_events(EXAMPLES / "handoff-does-not-drain.jsonl", KINDS))[:14]:
            c.ingest(e)
        self.assertEqual(c.window_view("w1")["state"], "unknown")
        self.assertEqual(c.intake_probe("a", "v1")["decision"], "retry_later")
        self.assertEqual(c.intake_probe("a", client_authorized_new=True)["decision"], "retry_later")

    def test_restart_really_reloads_from_disk(self):
        # 模拟进程退出后用同一数据目录重新启动。
        c2 = Coordinator(self.tmp.name)
        v = c2.window_view("w1")
        self.assertEqual(v["state"], "committed")  # 日志最后已 committed v2
        self.assertEqual(v["determined_numbering"], "v2")
        self.assertIn("yard#2", v["resumes"])
        # 旧版本恢复在任何新实例上依旧是被拒绝的。
        again = c2.submit("resume", {"window": "w1", "service": "desk", "boot": 1, "numbering": "v1"})
        self.assertFalse(again["accepted"])
        self.assertEqual(again["code"], "stale_numbering_resume")

    def test_stale_v1_resume_rejected_after_v2_commit(self):
        stale = self.d[15]  # 第 16 行
        good = self.d[16]  # 第 17 行
        self.assertFalse(stale["accepted"])
        self.assertEqual(stale["code"], "stale_numbering_resume")
        self.assertTrue(good["accepted"])
        # yard#2 已按 v2 放行；desk#1 尚未恢复，故整个车队仍是恢复中。
        self.assertEqual(self.c.intake_probe("a", "v2", service="yard")["decision"], "allow")
        self.assertEqual(
            self.c.intake_probe("a", "v2", service="desk")["decision"],
            "retry_restoring",
        )
        self.assertEqual(self.c.intake_probe("a", "v2")["decision"], "retry_restoring")
        self.assertEqual(self.c.intake_probe("a", "v1")["decision"], "reject_obsolete")

    def test_journal_file_exists(self):
        self.assertTrue((Path(self.tmp.name) / "journal.jsonl").exists())


class ScopeExpansionAndOverlapTest(unittest.TestCase):
    def setUp(self):
        self.c, self.d = replay("scope-expansion-and-overlap.jsonl")

    def test_open_fleet_a_requires_expansion_to_b(self):
        opened_a = self.d[2]
        self.assertEqual(opened_a["resolution"]["expansion_required"], ["b"])
        self.assertIn("expansion_unapproved", codes(opened_a))

    def test_unrelated_fleet_c_keeps_running_while_window_waits(self):
        inspect_c = self.d[4]["view"]  # 第 5 行
        self.assertEqual(inspect_c["fleet"], "c")
        self.assertEqual(inspect_c["ordinary_requests"], "allow")
        self.assertEqual(self.c.intake_probe("c", "v1")["decision"], "allow")

    def test_fleet_b_sees_advisory_but_is_not_frozen_before_approval(self):
        # 重放到 approve 之前（第 4 行 open b 之后）。
        c = Coordinator()
        for e in list(load_events(EXAMPLES / "scope-expansion-and-overlap.jsonl", KINDS))[:4]:
            c.ingest(e)
        view_b = c.fleet_view("b")
        advisory_codes = [a["code"] for a in view_b["advisories"]]
        self.assertIn("expansion_requested", advisory_codes)
        # joined 尚未 paused，因此 b 的普通请求仍然可进，没有冻结 hold。
        self.assertEqual(view_b["ordinary_requests"], "allow")

    def test_begin_a_succeeds_after_approval_and_pause(self):
        begin_a = self.d[7]  # 第 8 行
        self.assertTrue(begin_a["accepted"], begin_a)
        self.assertEqual(begin_a["commitment"]["fleets"], ["a", "b"])
        self.assertIn("R", begin_a["commitment"]["registers"])

    def test_overlapping_window_b_blocked_by_shared_register(self):
        begin_b = self.d[8]  # 第 9 行
        self.assertFalse(begin_b["accepted"])
        self.assertIn("register_conflict", codes(begin_b))
        conflict = next(b for b in begin_b["blockers"] if b["code"] == "register_conflict")
        self.assertEqual(conflict["register"], "R")
        self.assertEqual(conflict["other_window"], "a")

    def test_waiting_b_can_cancel_after_denied_begin(self):
        cancel = self.d[9]
        self.assertTrue(cancel["accepted"])
        self.assertEqual(self.c.window_view("b")["state"], "canceled")

    def test_abort_keeps_v1_and_resume_reopens_fleet_b(self):
        aborted = self.d[10]
        self.assertTrue(aborted["accepted"])
        resume = self.d[11]
        self.assertTrue(resume["accepted"])
        inspect_b = self.d[12]["view"]
        self.assertEqual(inspect_b["ordinary_requests"], "allow")
        self.assertEqual(self.c.intake_probe("b", "v1")["decision"], "allow")
        # abort 后提议的新编号未生效，按新编号提交要被拒。
        self.assertEqual(self.c.intake_probe("b", "v2")["decision"], "reject_new_not_committed")
        self.assertEqual(self.c.window_view("a")["state"], "restored")


class TopologyAndGenerationRulesTest(unittest.TestCase):
    def test_takeover_before_open_old_confirmation_does_not_count(self):
        c = Coordinator()
        c.submit("member", {"service": "reg", "boot": 1, "fleets": ["a"], "registers": ["R"], "pause_groups": [["a"]]})
        # 窗口形成之前新代次已经接管。
        c.submit("member", {"service": "reg", "boot": 2, "fleets": ["a"], "registers": ["R"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "w", "fleets": ["a"]})
        old_pause = c.submit("paused", {"window": "w", "service": "reg", "boot": 1, "pending": []})
        self.assertTrue(old_pause["stale_generation"])
        gate = c.evaluate_begin("w")
        self.assertFalse(gate["can_begin"])
        pm = next(b for b in gate["blockers"] if b["code"] == "pause_missing")
        self.assertEqual(pm["boot"], 2)
        c.submit("paused", {"window": "w", "service": "reg", "boot": 2, "pending": []})
        self.assertTrue(c.evaluate_begin("w")["can_begin"])

    def test_independent_stop_capability_removes_expansion(self):
        c = Coordinator()
        c.submit("member", {"service": "shared", "boot": 1, "fleets": ["a", "b"], "pause_groups": [["a", "b"]]})
        c.submit("open", {"window": "w", "fleets": ["a"], "expand_accepted": False})
        self.assertEqual(c.evaluate_begin("w")["resolution"]["expansion_required"], ["b"])
        # 服务升级后公布可独立停止 a / b 的新能力。
        c.submit("member", {"service": "shared", "boot": 2, "fleets": ["a", "b"], "pause_groups": [["a"], ["b"]]})
        res = c.evaluate_begin("w")["resolution"]
        self.assertEqual(res["expansion_required"], [])
        c.submit("paused", {"window": "w", "service": "shared", "boot": 2, "pending": []})
        begin = c.submit("begin", {"window": "w", "actor": "lead"})
        self.assertTrue(begin["accepted"], begin)
        self.assertEqual(begin["commitment"]["fleets"], ["a"])

    def test_cannot_cancel_after_successful_begin(self):
        c = Coordinator()
        c.submit("member", {"service": "s", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "w", "fleets": ["a"]})
        c.submit("paused", {"window": "w", "service": "s", "boot": 1, "pending": []})
        self.assertTrue(c.submit("begin", {"window": "w"})["accepted"])
        cancel = c.submit("cancel", {"window": "w"})
        self.assertFalse(cancel["accepted"])
        self.assertEqual(cancel["code"], "cannot_cancel_after_begin")

    def test_cross_window_shared_residual_work_needs_global_finish(self):
        c = Coordinator()
        c.submit("member", {"service": "desk", "boot": 1, "fleets": ["a"], "registers": ["R"], "pause_groups": [["a"]]})
        c.submit("member", {"service": "yard", "boot": 1, "fleets": ["a"], "registers": ["R"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "wx", "fleets": ["a"]})
        c.submit("open", {"window": "wy", "fleets": ["a"]})
        # 同一残余工作同时挂在两个窗口。
        c.submit("paused", {"window": "wx", "service": "desk", "boot": 1, "pending": ["job-shared"]})
        c.submit("paused", {"window": "wy", "service": "yard", "boot": 1, "pending": ["job-shared"]})
        # 双方随后都声称自己 pending 为空 —— 但没有 finished。
        c.submit("paused", {"window": "wx", "service": "desk", "boot": 1, "pending": []})
        c.submit("paused", {"window": "wy", "service": "yard", "boot": 1, "pending": []})
        for wid in ("wx", "wy"):
            self.assertIn("work_outstanding", codes(c.evaluate_begin(wid)))
        work = c.work_view("job-shared")
        self.assertEqual(work["status"], "held")
        self.assertEqual(set(work["windows"]), {"wx", "wy"})
        # 台账保管方是 desk（首次登记），yard 的异主声明只产生 conflict，不清工作。
        finished = c.submit("finished", {"service": "desk", "boot": 1, "work": "job-shared"})
        self.assertTrue(finished["accepted"])
        self.assertNotIn("work_outstanding", codes(c.evaluate_begin("wx")))

    def test_transfer_to_unconfirmed_party_makes_handoff_ineligible(self):
        c = Coordinator()
        c.submit("member", {"service": "src", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("member", {"service": "dst", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "w", "fleets": ["a"]})
        c.submit("paused", {"window": "w", "service": "src", "boot": 1, "pending": ["j"]})
        # 接收方尚未确认就交接。
        c.submit("transfer", {"work": "j", "from": "src", "from_boot": 1, "to": "dst", "to_boot": 1})
        c.submit("paused", {"window": "w", "service": "dst", "boot": 1, "pending": ["j"]})
        c.submit("finished", {"service": "dst", "boot": 1, "work": "j"})
        gate = c.evaluate_begin("w")
        self.assertIn("ineligible_transfer", codes(gate))

    def test_finish_from_non_custodian_does_not_clear(self):
        c = Coordinator()
        c.submit("member", {"service": "s", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("member", {"service": "s", "boot": 2, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "w", "fleets": ["a"]})
        c.submit("paused", {"window": "w", "service": "s", "boot": 1, "pending": ["j"]})
        # 当前保管是 s#1；s#2 已接管但并非承接代次，它说完成不能销账。
        bad = c.submit("finished", {"service": "s", "boot": 2, "work": "j"})
        self.assertFalse(bad["accepted"])
        self.assertEqual(bad["code"], "finish_rejected_not_holder")
        self.assertEqual(c.work_view("j")["status"], "held")
        good = c.submit("finished", {"service": "s", "boot": 1, "work": "j"})
        self.assertTrue(good["accepted"])
        self.assertEqual(c.work_view("j")["status"], "finished")


class DurabilityAndOnboardingTest(unittest.TestCase):
    def test_unknown_freeze_survives_real_process_restart_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            c = Coordinator(tmp)
            c.submit("member", {"service": "s", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
            c.submit("open", {"window": "w", "fleets": ["a"]})
            c.submit("paused", {"window": "w", "service": "s", "boot": 1, "pending": []})
            c.submit("begin", {"window": "w", "actor": "lead"})
            c.submit("result", {"window": "w", "state": "unknown"})

            c2 = Coordinator(tmp)  # 全新实例 = 真实进程重启
            self.assertEqual(c2.window_view("w")["state"], "unknown")
            blocked = c2.submit("resume", {"window": "w", "service": "s", "boot": 1, "numbering": "v1"})
            self.assertFalse(blocked["accepted"])
            self.assertEqual(blocked["code"], "numbering_not_determined")
            c2.submit("result", {"window": "w", "state": "committed", "new_numbering": "v2"})
            ok = c2.submit("resume", {"window": "w", "service": "s", "boot": 1, "numbering": "v2"})
            self.assertTrue(ok["accepted"])
            self.assertEqual(c2.intake_probe("a", "v2")["decision"], "allow")

    def test_onboard_brand_new_fleet_and_run_full_window(self):
        c = Coordinator()
        c.submit("member", {
            "service": "nova", "boot": 1, "fleets": ["d"], "registers": ["S"],
            "pause_groups": [["d"]], "new_work": True,
            "downstreams": [{"service": "gateway", "boot": 3}],
        })
        self.assertEqual(c.fleet_view("d")["ordinary_requests"], "allow")
        c.submit("open", {"window": "wd", "fleets": ["d"]})
        c.submit("paused", {"window": "wd", "service": "nova", "boot": 1, "pending": []})
        begin = c.submit("begin", {"window": "wd", "actor": "fleet-lead-li"})
        self.assertTrue(begin["accepted"])

        old = c.intake_probe("d", "v1")
        self.assertEqual(old["decision"], "reject_old_number")
        new_client = c.intake_probe("d", client_authorized_new=True)
        self.assertEqual(new_client["decision"], "wait_for_result")

        c.submit("result", {"window": "wd", "state": "committed", "new_numbering": "v9"})
        self.assertEqual(c.intake_probe("d", "v9")["decision"], "retry_restoring")
        stale = c.submit("resume", {"window": "wd", "service": "nova", "boot": 1, "numbering": "v1"})
        self.assertEqual(stale["code"], "stale_numbering_resume")
        c.submit("resume", {"window": "wd", "service": "nova", "boot": 1, "numbering": "v9"})
        self.assertEqual(c.intake_probe("d", "v9")["decision"], "allow")
        # 完全按 v9 恢复后，旧 v1 即使在“无冻结 hold”分支也必须判 obsolete。
        self.assertEqual(c.intake_probe("d", "v1")["decision"], "reject_obsolete")
        self.assertEqual(c.fleet_view("d")["ordinary_requests"], "allow")

    def test_service_cannot_unilaterally_freeze_unrelated_shared_fleet(self):
        # 未 approve 前，调度侧看不到任何针对 b 的 hold。
        c = Coordinator()
        c.submit("member", {"service": "shared", "boot": 1, "fleets": ["a", "b"], "pause_groups": [["a", "b"]]})
        c.submit("member", {"service": "other", "boot": 1, "fleets": ["c"], "pause_groups": [["c"]]})
        c.submit("open", {"window": "w", "fleets": ["a"], "expand_accepted": False})
        for fleet in ("b", "c"):
            view = c.fleet_view(fleet)
            self.assertEqual(view["ordinary_requests"], "allow", fleet)
            self.assertEqual(view["serving_services"][0]["holds"], [])


class ResultAndAuthorizationTest(unittest.TestCase):
    def _ready(self):
        c = Coordinator()
        c.submit("member", {"service": "s", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]})
        c.submit("open", {"window": "w", "fleets": ["a"], "owner": "lead-z"})
        c.submit("paused", {"window": "w", "service": "s", "boot": 1, "pending": []})
        return c

    def test_begin_authorization_names_owner_and_flips_after_begin(self):
        c = self._ready()
        view = c.window_view("w")
        auth = view["begin_authorization"]
        self.assertTrue(auth["authorized"])
        self.assertEqual(auth["who"], "lead-z")
        self.assertEqual(auth["commitment_preview"]["generations"], {"s": 1})
        begun = c.submit("begin", {"window": "w", "actor": "lead-z"})
        self.assertTrue(begun["accepted"])
        self.assertFalse(c.window_view("w")["begin_authorization"]["authorized"])

    def test_committed_cannot_regress_to_unknown_and_duplicate_is_idempotent(self):
        c = self._ready()
        c.submit("begin", {"window": "w"})
        c.submit("result", {"window": "w", "state": "committed", "new_numbering": "v2"})
        regress = c.submit("result", {"window": "w", "state": "unknown"})
        self.assertFalse(regress["accepted"])
        self.assertEqual(regress["code"], "outcome_already_determined")
        self.assertEqual(c.window_view("w")["determined_numbering"], "v2")
        dup = c.submit("result", {"window": "w", "state": "committed", "new_numbering": "v2"})
        self.assertTrue(dup["accepted"])

    def test_result_before_begin_rejected(self):
        c = Coordinator()
        c.submit("open", {"window": "w", "fleets": ["a"]})
        r = c.submit("result", {"window": "w", "state": "committed", "new_numbering": "v9"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "result_without_begin")


if __name__ == "__main__":
    unittest.main()
