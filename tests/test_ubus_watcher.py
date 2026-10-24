"""Regression test: EOF restarts a ubus subscription without looping forever."""
import importlib.util
from pathlib import Path
import sys
import threading
from queue import Queue
from types import SimpleNamespace
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
    def test_sync_reconciles_live_only_disassociation(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        mac = "aa:bb:cc:dd:ee:ff"
        detector._registered_clients = set()
        detector._last_seen_clients = set()
        detector._settings = SimpleNamespace(interfaces=["hostapd.test"], params={})
        detector._online_clients = {"hostapd.test": {mac}}
        detector._queue = Queue()
        detector._logger = SimpleNamespace(log=lambda *args, **kwargs: None)
        detector._get_all_online_devices = lambda: []
        detector._should_handle_device = lambda device: True
        detector._do_full_sync()
        event = detector._queue.get_nowait()
        self.assertEqual(event.action, mod.QueueItem.Action.DELETE)
        self.assertEqual(event.device, mac)
        self.assertEqual(detector._online_clients["hostapd.test"], set())

    def test_subscription_restarts_after_eof(self):
        mod = load_detector()
        joined = []
        left = []
        watcher = mod.UbusWatcher("hostapd.wlan0", lambda i, m: joined.append((i, m)), lambda i, m: left.append((i, m)))
        restarted = threading.Event()
        reconciled = []
        def on_reconnect(interface):
            reconciled.append(interface)
            watcher.stop()
        watcher._on_reconnect = on_reconnect
        processes = []

        def popen(args, **kwargs):
            self.assertEqual(args, ["ubus", "subscribe", "hostapd.wlan0"])
            if processes:
                proc = FakeProcess([])
                restarted.set()
            else:
                proc = FakeProcess(['{"assoc":{"address":"AA:BB:CC:DD:EE:FF"}}\n', '{"disassoc":{"address":"AA:BB:CC:DD:EE:FF"}}\n'])
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
        self.assertEqual(reconciled, ["hostapd.wlan0"])
        self.assertEqual(joined, [("hostapd.wlan0", "aa:bb:cc:dd:ee:ff")])
        self.assertEqual(left, [("hostapd.wlan0", "aa:bb:cc:dd:ee:ff")])


if __name__ == "__main__":
    unittest.main()
