"""Regression test for retrying failed MQTT discovery publishes."""

import importlib.util
import json
from pathlib import Path
from queue import Queue
import sys
from threading import Event, Lock, RLock, Thread
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
    with patch.dict(
        sys.modules, {"paho": paho, "paho.mqtt": pkg, "paho.mqtt.client": client}
    ):
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
        detector._settings = SimpleNamespace(
            ap_name="",
            params={},
            location="home",
            away="not_home",
            source_type="router",
            mqtt_retain_state=True,
        )
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

    def test_cached_registration_invalidated_after_reset(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._registered_clients = {"aa_bb_cc_dd_ee_ff"}
        detector._registration_lock = Lock()
        detector._registration_epoch = 0
        detector._settings = SimpleNamespace(
            ap_name="",
            params={},
            location="home",
            away="not_home",
            source_type="router",
            mqtt_retain_state=True,
        )
        publications = []
        detector._publish = (
            lambda topic, payload, retain=False: publications.append(topic) or True
        )
        detector._reset_registrations()
        self.assertTrue(detector._ha_seen("aa:bb:cc:dd:ee:ff"))
        self.assertTrue(any(topic.endswith("/config") for topic in publications))

    def test_reset_during_publish_is_not_cached(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._registered_clients = set()
        detector._registration_lock = Lock()
        detector._registration_epoch = 0
        detector._settings = SimpleNamespace(
            ap_name="",
            params={},
            location="home",
            away="not_home",
            source_type="router",
            mqtt_retain_state=True,
        )
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

    def make_detector(self):
        mod = load_detector()
        with patch.object(mod, "Settings"), patch.object(
            mod.PresenceDetector, "_connect_to_mqtt"
        ):
            mod.Settings.return_value = SimpleNamespace(
                ap_name="",
                params={},
                location="home",
                away="not_home",
                source_type="router",
                mqtt_retain_state=True,
                debug=False,
                interfaces=["hostapd.test"],
                filter=[],
                filter_is_denylist=True,
                fallback_sync_interval=0,
            )
            return mod, mod.PresenceDetector("unused")

    def test_connect_and_live_online_coalesce_sync_requests(self):
        mod, detector = self.make_detector()
        detector._mqtt = SimpleNamespace(
            subscribe=lambda topic: subscriptions.append(topic)
        )
        subscriptions = []
        detector._on_mqtt_connect(None, None, None, 1)
        self.assertTrue(detector._queue.empty())
        detector._on_mqtt_connect(None, None, None, 0)
        detector._on_ha_status_message(
            None, None, SimpleNamespace(retain=False, payload=b"online")
        )
        self.assertEqual(subscriptions, ["homeassistant/status"])
        self.assertEqual(detector._queue.get_nowait().action, mod.QueueItem.Action.SYNC)
        self.assertTrue(detector._queue.empty())

    def test_offline_configured_device_reannounced_on_every_sync(self):
        mod, detector = self.make_detector()
        mac, denied = "aa:bb:cc:dd:ee:ff", "11:22:33:44:55:66"
        detector._settings.params = {mac: {}, denied: {}}
        detector._settings.filter_is_denylist = False
        detector._settings.filter = [mac]
        detector._get_all_online_devices = lambda: []
        publications = []
        detector._publish = (
            lambda topic, body, retain=False: publications.append(
                (topic, json.loads(body))
            )
            or True
        )

        for _ in range(2):
            detector._do_full_sync()
            item = detector._queue.get_nowait()
            self.assertEqual(
                (item.device, item.interface, item.action),
                (mac, "", mod.QueueItem.Action.DELETE),
            )
            self.assertTrue(detector._ha_seen(item.device, False, item))
            self.assertTrue(detector._queue.empty())
        self.assertEqual(
            [topic.rsplit("/", 1)[1] for topic, _ in publications],
            ["config", "state"] * 2,
        )
        self.assertEqual(
            [body["state"] for topic, body in publications if topic.endswith("/state")],
            ["not_home"] * 2,
        )

    def test_new_away_during_home_discovery_suppresses_home_state(self):
        mod, detector = self.make_detector()
        entered, release = Event(), Event()
        interface, mac = "hostapd.test", "aa:bb:cc:dd:ee:ff"
        states = []
        detector.start_watchers = lambda: None
        detector.set_device_home(interface, mac)

        def publish(topic, payload, retain=False):
            if topic.endswith("/config"):
                entered.set()
                return release.wait(5)
            states.append(json.loads(payload)["state"])
            if states[-1] == "not_home":
                detector._queue.put(mod.QueueItem("", "", mod.QueueItem.Action.QUIT))
            return True

        detector._publish = publish
        detector.start()
        try:
            self.assertTrue(entered.wait(5))
            detector.set_device_away(interface, mac)
            release.set()
            detector.join(5)
            self.assertFalse(detector.is_alive())
            self.assertEqual(states, ["not_home"])
        finally:
            release.set()
            detector._queue.put(mod.QueueItem("", "", mod.QueueItem.Action.QUIT))
            detector.join(5)

    def test_snapshot_preserves_watcher_event_received_during_scan(self):
        mod, detector = self.make_detector()
        interface, mac = "hostapd.test", "aa:bb:cc:dd:ee:ff"
        detector._settings.params = {mac: {}}

        def snapshot():
            detector.set_device_home(interface, mac)
            return []

        detector._get_all_online_devices = snapshot
        detector._do_full_sync()
        items = [detector._queue.get_nowait(), detector._queue.get_nowait()]
        self.assertEqual(
            [item.action for item in items], [mod.QueueItem.Action.ADD] * 2
        )
        self.assertEqual(detector._online_clients[interface], {mac})


if __name__ == "__main__":
    unittest.main()
