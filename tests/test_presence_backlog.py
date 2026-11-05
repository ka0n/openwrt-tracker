"""Verify MQTT retry backlog cannot republish superseded presence states."""
from queue import Queue
from threading import Lock
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_python_discovery_retry import load_detector


class PresenceBacklogTest(unittest.TestCase):
    def test_failed_home_retry_cannot_override_newer_away(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._queue = Queue()
        detector._killed = False
        detector._presence_revision = {}
        detector._presence_lock = Lock()
        detector._settings = SimpleNamespace(fallback_sync_interval=0)
        detector._logger = SimpleNamespace(log=lambda *args, **kwargs: None)
        detector.start_watchers = lambda: None

        mac = "aa:bb:cc:dd:ee:ff"
        interface = "hostapd.test"
        detector._queue_presence(mac, interface, mod.QueueItem.Action.ADD)
        attempted = []
        published = []

        def publish(device, seen, item):
            attempted.append(seen)
            if len(attempted) == 1:
                # During the failed HOME publish, the device leaves.
                detector._queue_presence(mac, interface, mod.QueueItem.Action.DELETE)
                return False
            published.append(seen)
            # The stale HOME retry is still queued; processing it should
            # not make another publication.
            detector._queue.put(mod.QueueItem("quit", "", mod.QueueItem.Action.QUIT))
            return True

        detector._ha_seen = publish
        with patch.object(mod.time, "sleep", return_value=None):
            detector.run()
        self.assertEqual(attempted, [True, False])
        self.assertEqual(published, [False])
        self.assertEqual(detector._presence_revision[mac], 2)

    def test_duplicate_same_state_retry_keeps_latest_revision(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._presence_revision = {}
        detector._presence_lock = Lock()
        detector._queue = Queue()
        mac = "aa:bb:cc:dd:ee:ff"
        detector._queue_presence(mac, "hostapd.test", mod.QueueItem.Action.ADD)
        old = detector._queue.get_nowait()
        detector._queue_presence(mac, "hostapd.test", mod.QueueItem.Action.ADD)
        new = detector._queue.get_nowait()
        self.assertFalse(detector._is_current_presence(old))
        self.assertTrue(detector._is_current_presence(new))


if __name__ == "__main__":
    unittest.main()
