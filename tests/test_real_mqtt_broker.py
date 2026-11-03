"""Integration test: actual MQTT broker restart and discovery retry (CI only)."""
import getpass
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest

from paho.mqtt import client as mqtt


def load_detector():
    path = Path(__file__).resolve().parents[1] / "presence-detector.py"
    spec = importlib.util.spec_from_file_location("tracker_mqtt_integration", path)
    mod = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def wait_for(predicate, timeout=12):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("Condition not reached within timeout")


class RealBrokerRecoveryTest(unittest.TestCase):
    def test_broker_outage_retries_discovery_and_state(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            broker_conf = root / "mosquitto.conf"
            broker_conf.write_text(
                f"user {getpass.getuser()}\nlistener {port} 127.0.0.1\n"
                "allow_anonymous true\npersistence false\n"
                "log_dest stderr\n",
                encoding="utf-8",
            )
            broker = None
            detector = None
            observer = None
            messages = []
            try:
                def launch_broker():
                    p = subprocess.Popen(
                        ["mosquitto", "-c", str(broker_conf)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )
                    def accepts_connections():
                        if p.poll() is not None:
                            raise AssertionError(
                                "mosquitto exited unexpectedly: "
                                + p.stderr.read().decode("utf-8", errors="replace")
                            )
                        with socket.socket() as sock:
                            return sock.connect_ex(("127.0.0.1", port)) == 0
                    wait_for(accepts_connections)
                    return p

                broker = launch_broker()
                settings = root / "config.json"
                settings.write_text(
                    json.dumps({
                        "mqtt_host": "127.0.0.1",
                        "mqtt_port": port,
                        "mqtt_username": "ha",
                        "mqtt_password": "",
                        "interfaces": ["hostapd.test"],
                        "params": {},
                        "fallback_sync_interval": 0,
                    }),
                    encoding="utf-8",
                )
                mod = load_detector()
                detector = mod.PresenceDetector(str(settings))
                wait_for(detector._mqtt.is_connected)

                topic_prefix = "homeassistant/device_tracker/aa_bb_cc_dd_ee_ff/"
                def subscribe_observer():
                    nonlocal observer, messages
                    messages = []
                    observer = mqtt.Client(
                        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                    )
                    observer.on_message = lambda _c, _u, msg: messages.append(
                        (msg.topic, msg.payload.decode("utf-8"))
                    )
                    observer.connect("127.0.0.1", port)
                    observer.subscribe(topic_prefix + "#", qos=1)
                    observer.loop_start()

                subscribe_observer()
                mac = "aa:bb:cc:dd:ee:ff"
                self.assertTrue(detector._ha_seen(mac, seen=True))
                wait_for(lambda: len(messages) >= 2)
                self.assertEqual(
                    {topic for topic, _payload in messages},
                    {topic_prefix + "config", topic_prefix + "state"},
                )
                self.assertIn("aa_bb_cc_dd_ee_ff", detector._registered_clients)

                observer.disconnect()
                observer.loop_stop()
                observer = None
                broker.terminate()
                broker.wait(timeout=5)
                broker = None
                wait_for(lambda: not detector._mqtt.is_connected())
                # This publish must fail while the actual TCP broker is down.
                self.assertFalse(detector._ha_seen(mac, seen=False))
                self.assertNotIn("aa_bb_cc_dd_ee_ff", detector._registered_clients)

                broker = launch_broker()
                wait_for(detector._mqtt.is_connected, timeout=20)
                subscribe_observer()
                self.assertTrue(detector._ha_seen(mac, seen=False))
                wait_for(lambda: len(messages) >= 2)
                received = dict(messages)
                self.assertIn(topic_prefix + "config", received)
                self.assertEqual(
                    json.loads(received[topic_prefix + "state"])["state"],
                    "not_home",
                )
                self.assertIn("aa_bb_cc_dd_ee_ff", detector._registered_clients)
            finally:
                if observer is not None:
                    observer.disconnect()
                    observer.loop_stop()
                if detector is not None:
                    detector._mqtt.disconnect()
                    detector._mqtt.loop_stop()
                if broker is not None:
                    broker.terminate()
                    try:
                        broker.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        broker.kill()
                        broker.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
