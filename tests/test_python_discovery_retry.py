"""Regression test for retrying failed MQTT discovery publishes."""
import importlib.util
from pathlib import Path
import sys
from threading import Lock
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
    with patch.dict(sys.modules, {"paho": paho, "paho.mqtt": pkg, "paho.mqtt.client": client}):
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
        detector._registration_lock = Lock()
        detector._registration_epoch = 0
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

    def test_cached_registration_invalidated_during_state_call(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._registered_clients = {"aa_bb_cc_dd_ee_ff"}
        detector._registration_lock = Lock()
        detector._registration_epoch = 0
        detector._settings = SimpleNamespace(ap_name="", params={}, location="home", away="not_home", source_type="router", mqtt_retain_state=True)
        publications = []

        class ResetOnContains(set):
            def __contains__(self, value):
                result = super().__contains__(value)
                detector._reset_registrations()
                return result

        detector._registered_clients = ResetOnContains(detector._registered_clients)
        detector._publish = lambda topic, payload, retain=False: publications.append(topic) or True
        self.assertFalse(detector._ha_seen("aa:bb:cc:dd:ee:ff"))
        self.assertEqual(publications, [])
        self.assertTrue(detector._ha_seen("aa:bb:cc:dd:ee:ff"))
        self.assertTrue(any(topic.endswith("/config") for topic in publications))

    def test_reset_during_publish_is_not_cached(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._registered_clients = set()
        detector._registration_lock = Lock()
        detector._registration_epoch = 0
        detector._settings = SimpleNamespace(ap_name="", params={}, location="home", away="not_home", source_type="router", mqtt_retain_state=True)
        publications = []

        def publish(topic, payload, retain=False):
            publications.append(topic)
            if topic.endswith("/config") and len(publications) == 1:
                detector._reset_registrations()
            return True

        detector._publish = publish
        mac = "aa:bb:cc:dd:ee:ff"
        slug = "aa_bb_cc_dd_ee_ff"
        self.assertFalse(detector._ha_seen(mac))
        self.assertNotIn(slug, detector._registered_clients)
        self.assertTrue(detector._ha_seen(mac))
        self.assertIn(slug, detector._registered_clients)
        self.assertEqual(sum(s.endswith("/config") for s in publications), 2)


if __name__ == "__main__":
    unittest.main()
