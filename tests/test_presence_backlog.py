"""Reproduce MQTT backlog reordering against the actual Python run loop."""
from queue import Queue
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_python_discovery_retry import load_detector


class PresenceBacklogTest(unittest.TestCase):
    def test_failed_old_home_must_not_override_newer_away(self):
        mod = load_detector()
        detector = object.__new__(mod.PresenceDetector)
        detector._queue = Queue()
        detector._killed = False
        detector._settings = SimpleNamespace(fallback_sync_interval=0)
        detector._logger = SimpleNamespace(log=lambda *args, **kwargs: None)
        detector._do_full_sync = lambda: None
        detector.start_watchers = lambda: None

        mac = "aa:bb:cc:dd:ee:ff"
        detector._queue.put(mod.QueueItem(mac, "hostapd.test", mod.QueueItem.Action.ADD))
        detector._queue.put(mod.QueueItem(mac, "hostapd.test", mod.QueueItem.Action.DELETE))

        attempted = []
        published = []

        def publish(device, seen):
            attempted.append(seen)
            if len(attempted) == 1:
                # Broker is offline: the old HOME event gets requeued.
                return False
            published.append(seen)
            if len(attempted) == 3:
                detector._killed = True
            return True

        detector._ha_seen = publish
        with patch.object(mod.time, "sleep", return_value=None):
            detector.run()
        self.assertEqual(attempted, [True, False, True])
        # The real retained MQTT state must not be reverted to an older
        # HOME event when the broker is back online.
        self.assertEqual(published[-1], False, "stale HOME overwrote newer AWAY")


if __name__ == "__main__":
    unittest.main()
