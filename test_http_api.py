"""HTTP 端到端测试，以及把 replay.py 的场景核对纳入 unittest。"""

import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from urllib.request import urlopen

from server import build_server
from simulator import Client
from coordinator import replay_file
from replay import verify_scenario

ROOT = Path(__file__).parent
EXAMPLES = ROOT / "examples"


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.httpd = build_server("127.0.0.1", 0, self.tmp.name)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"
        self.client = Client(self.base)
        self.client.wait_up()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.tmp.cleanup()

    def test_health_and_descriptor(self):
        health = self.client.health()
        self.assertTrue(health["ok"])
        with urlopen(self.base + "/health", timeout=5) as resp:
            self.assertEqual(resp.status, 200)

    def test_full_window_over_http_and_autocreate_case(self):
        # 直接向不存在的 case 发事件应自动建档。
        d = self.client.event("auto", {"at": 1, "kind": "member", "record": {
            "service": "s", "boot": 1, "fleets": ["a"], "pause_groups": [["a"]]}})
        self.assertTrue(d["accepted"])
        self.client.event("auto", {"at": 2, "kind": "open", "record": {"window": "w", "fleets": ["a"]}})
        self.client.event("auto", {"at": 3, "kind": "paused",
                                    "record": {"window": "w", "service": "s", "boot": 1, "pending": []}})
        begun = self.client.event("auto", {"at": 4, "kind": "begin", "record": {"window": "w"}})
        self.assertTrue(begun["accepted"], begun)

        old = self.client.intake("auto", fleet="a", numbering="v1")
        self.assertEqual(old["decision"], "reject_old_number")
        wait = self.client.intake("auto", fleet="a", client_authorized_new=True)
        self.assertEqual(wait["decision"], "wait_for_result")

        self.client.event("auto", {"at": 5, "kind": "result",
                                  "record": {"window": "w", "state": "committed", "new_numbering": "v2"}})
        self.client.event("auto", {"at": 6, "kind": "resume",
                                   "record": {"window": "w", "service": "s", "boot": 1, "numbering": "v2"}})
        self.assertEqual(self.client.intake("auto", fleet="a", numbering="v2")["decision"], "allow")
        fleet = self.client.get_fleet("auto", "a")
        self.assertEqual(fleet["ordinary_requests"], "allow")

        # 快照与案例列表可用。
        snap = self.client._request("GET", "/api/cases/auto/snapshot")
        self.assertEqual(snap["windows"][0]["state"], "restored")
        cases = self.client._request("GET", "/api/cases")["cases"]
        self.assertIn("auto", cases)

    def test_restart_endpoint_reloads_journal(self):
        self.client.create_case("persist")
        self.client.event("persist", {"kind": "member", "record": {
            "service": "s", "boot": 1, "fleets": ["f"], "pause_groups": [["f"]]}})
        self.client.event("persist", {"kind": "open", "record": {"window": "w", "fleets": ["f"]}})
        self.client.event("persist", {"kind": "paused",
                                      "record": {"window": "w", "service": "s", "boot": 1, "pending": []}})
        self.client.event("persist", {"kind": "begin", "record": {"window": "w"}})
        self.client.event("persist", {"kind": "result", "record": {"window": "w", "state": "unknown"}})
        reloaded = self.client.restart_case("persist")
        self.assertTrue(reloaded["restarted"])
        window = self.client.get_window("persist", "w")
        self.assertEqual(window["state"], "unknown")
        blocked = self.client.event("persist", {"kind": "resume",
                                                "record": {"window": "w", "service": "s", "boot": 1, "numbering": "v1"}})
        self.assertEqual(blocked["code"], "numbering_not_determined")

    def test_unknown_routes_and_bad_input(self):
        with self.assertRaises(RuntimeError):
            self.client.get_fleet("missing-case", "x")
        # 缺 fleet 的 intake -> 400
        self.client.create_case("x")
        req = urllib.request.Request(
            self.base + "/api/cases/x/intake",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(ctx.exception.code, 400)


class ReplayVerificationTest(unittest.TestCase):
    def test_every_example_matches_its_contract(self):
        examples = sorted(EXAMPLES.glob("*.jsonl"))
        self.assertGreaterEqual(len(examples), 3)
        for path in examples:
            with self.subTest(file=path.name):
                with tempfile.TemporaryDirectory() as tmp:
                    coordinator, decisions = replay_file(path, tmp)
                    checks = verify_scenario(path.name, coordinator, decisions)
                    self.assertTrue(checks, f"{path.name} 缺少核对规则")
                    failed = [label for label, ok, _ in checks if not ok]
                    self.assertEqual(failed, [], f"{path.name} 未通过核对: {failed}")


if __name__ == "__main__":
    unittest.main()
