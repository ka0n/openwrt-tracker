"""Regression test for retrying failed MQTT discovery publishes."""
import importlib.util
from pathlib import Path
import sys
import types
from types import SimpleNamespace
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
    path = Path(__file__).resolve().parents[1] / "presence-detector.py"
    with patch.dict(sys.modules, {"paho": paho, "paho.mqtt": pkg, "paho.mqtt.client": client, "tracker_discovery_test": module}):
        spec = importlib.util.spec_from_file_location("tracker_discovery_test", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["tracker_discovery_test"] = module
        spec.loader.exec_module(module)
    return module


class DiscoveryRetryTest(unittest.TestCase):
    def test_failed_discovery_not_cached(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._registered_clients = set()
        detector._settings = SimpleNamespace(ap_name="", params={}, location="home", away="not_home", source_type="router", mqtt_retain_state=True)
        calls = {"config": 0, "state": 0}

        def publish(topic, payload, retain=False):
            kind = "config" if topic.endswith("/config") else "state"
            calls[kind] += 1
            return kind != "config" or calls[kind] > 1

        detector._publish = publish
        mac = "aa:bb:cc:dd:ee:ff"
        slug = "aa_bb_cc_dd_ee_ff"
        self.assertFalse(detector._ha_seen(mac))
        self.assertNotIn(slug, detector._registered_clients)
        self.assertTrue(detector._ha_seen(mac))
        self.assertIn(slug, detector._registered_clients)
        self.assertTrue(detector._ha_seen(mac, seen=False))
        self.assertEqual(calls, {"config": 2, "state": 3})


if __name__ == "__main__":
    unittest.main()
