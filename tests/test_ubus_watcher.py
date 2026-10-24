"""Regression test: EOF restarts a ubus subscription without looping forever."""
import importlib.util
from pathlib import Path
import sys
import threading
import types
import unittest
from unittest.mock import patch


def load_detector():
    paho = types.ModuleType("paho")
    paho.__path__ = []
    pkg = types.ModuleType("paho.mqtt")
    pkg.__path__ = []
    client = types.ModuleType("paho.mqtt.client")
    paho.mqtt = pkg
    pkg.client = client
    with patch.dict(sys.modules, {"paho": paho, "paho.mqtt": pkg, "paho.mqtt.client": client}):
        path = Path(__file__).resolve().parents[1] / "presence-detector.py"
        spec = importlib.util.spec_from_file_location("tracker_eof_test", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["tracker_eof_test"] = module
        spec.loader.exec_module(module)
    return module


class FakeProcess:
    def __init__(self, lines):
        self.lines = iter(lines)
        self.stdout = self
        self.terminated = False

    def readline(self):
        return next(self.lines, "")

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self):
        return 0


class WatcherEOFTest(unittest.TestCase):
    def test_subscription_restarts_after_eof(self):
        mod = load_detector()
        joined = []
        left = []
        watcher = mod.UbusWatcher("hostapd.wlan0", lambda i, m: joined.append((i, m)), lambda i, m: left.append((i, m)))
        restarted = threading.Event()
        processes = []

        def popen(args, **kwargs):
            self.assertEqual(args, ["ubus", "subscribe", "hostapd.wlan0"])
            if processes:
                watcher.stop()
                restarted.set()
                proc = FakeProcess([])
            else:
                proc = FakeProcess(['{"assoc":{"address":"AA:BB:CC:DD:EE:FF"}}\\n', '{"disassoc":{"address":"AA:BB:CC:DD:EE:FF"}}\\n'])
            processes.append(proc)
            return proc

        with patch.object(mod.subprocess, "Popen", side_effect=popen), patch.object(mod.time, "sleep", return_value=None):
            thread = threading.Thread(target=watcher.run, daemon=True)
            thread.start()
            try:
                self.assertTrue(restarted.wait(2), "EOF did not trigger another subscription")
            finally:
                watcher.stop()
                thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(processes[0].terminated)
        self.assertEqual(joined, [("hostapd.wlan0", "aa:bb:cc:dd:ee:ff")])
        self.assertEqual(left, [("hostapd.wlan0", "aa:bb:cc:dd:ee:ff")])


if __name__ == "__main__":
    unittest.main()
