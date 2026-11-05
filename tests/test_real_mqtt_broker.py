"""Exercise automatic recovery against a localhost-only Mosquitto broker."""

from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
from threading import Event
import time
import unittest

from paho.mqtt import client as mqtt

ROOT = Path(__file__).resolve().parents[1]
MAC = "aa:bb:cc:dd:ee:ff"
INTERFACE = "hostapd.test"
TOPIC = "homeassistant/device_tracker/aa_bb_cc_dd_ee_ff/"


def load_detector():
    spec = importlib.util.spec_from_file_location(
        "tracker_mqtt_integration", ROOT / "presence-detector.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def wait_for(predicate, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("Condition not reached within timeout")


class RealBrokerRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with closing(socket.socket()) as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.broker = None
        self.observer = None
        self.messages = []
        self.addCleanup(self.stop_broker)
        self.addCleanup(self.stop_observer)
        self.start_broker()

    def start_broker(self):
        # Mosquitto 2.x without a config uses a localhost-only listener
        # and no on-disk persistence. Restarting loses all retained data.
        self.broker = subprocess.Popen(
            ["mosquitto", "-p", str(self.port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        def accepts_connections():
            if self.broker.poll() is not None:
                self.fail(self.broker.stderr.read().decode("utf-8", errors="replace"))
            with closing(socket.socket()) as sock:
                return sock.connect_ex(("127.0.0.1", self.port)) == 0

        wait_for(accepts_connections)

    def stop_broker(self):
        if self.broker is not None:
            self.broker.terminate()
            try:
                self.broker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.broker.kill()
                self.broker.wait(timeout=5)
            self.broker.stderr.close()
            self.broker = None

    def observe(self):
        self.messages = []
        subscribed = Event()
        self.observer = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.observer.on_message = lambda _c, _u, msg: self.messages.append(
            (msg.topic, json.loads(msg.payload))
        )
        self.observer.on_subscribe = lambda *_args: subscribed.set()
        self.observer.connect("127.0.0.1", self.port)
        self.observer.subscribe(TOPIC + "#", qos=1)
        self.observer.loop_start()
        self.assertTrue(subscribed.wait(5), "observer never subscribed")

    def stop_observer(self):
        if self.observer is not None:
            self.observer.disconnect()
            self.observer.loop_stop()
            self.observer = None

    def expect_state(self, expected):
        wait_for(
            lambda: {topic for topic, _ in self.messages}
            >= {TOPIC + "config", TOPIC + "state"}
        )
        states = [
            payload["state"]
            for topic, payload in self.messages
            if topic == TOPIC + "state"
        ]
        self.assertEqual(set(states), {expected})

    def start_python_detector(self):
        settings = self.root / "config.json"
        settings.write_text(
            json.dumps(
                {
                    "mqtt_host": "127.0.0.1",
                    "mqtt_port": self.port,
                    "mqtt_username": "",
                    "interfaces": [INTERFACE],
                    "fallback_sync_interval": 0,
                    # Observe live state publishes, not an older retained value.
                    "mqtt_retain_state": False,
                }
            ),
            encoding="utf-8",
        )
        self.snapshot = [(INTERFACE, MAC)]
        mod = load_detector()
        detector = mod.PresenceDetector(str(settings))
        detector._get_all_online_devices = lambda: self.snapshot
        detector.start_watchers = lambda: None
        detector.start()

        def stop_detector():
            detector.stop()
            detector.join(timeout=10)
            self.assertFalse(detector.is_alive())

        self.addCleanup(stop_detector)
        return detector

    def exercise_python_restart(self, events_during_outage):
        self.observe()
        detector = self.start_python_detector()
        self.expect_state("home")

        # Hold the next TCP connection until the observer has subscribed.
        # Production reconnect and the MQTT callback still run normally.
        reconnect = Event()
        detector._mqtt.on_pre_connect = lambda *_args: reconnect.wait(20)
        try:
            self.stop_observer()
            self.stop_broker()
            wait_for(lambda: not detector._mqtt.is_connected())
            self.snapshot = []
            if events_during_outage:
                detector.set_device_away(INTERFACE, MAC)
                detector.set_device_home(INTERFACE, MAC)
                detector.set_device_away(INTERFACE, MAC)

            self.start_broker()
            self.observe()
            reconnect.set()
            self.expect_state("not_home")
        finally:
            reconnect.set()

    def test_python_idle_broker_restart_recovers_discovery_and_state(self):
        # No WLAN events or HA status publications at any point.
        self.exercise_python_restart(events_during_outage=False)

    def test_python_broker_restart_converges_after_presence_changes(self):
        self.exercise_python_restart(events_during_outage=True)

    def test_shell_failed_publish_recovers_after_broker_restart(self):
        # Run the normal main loop with the real mosquitto_pub. Speed up
        # its retry clock and stub router I/O and global process cleanup.
        runtime = self.root / "run"
        snapshot = self.root / "clients"
        snapshot.write_text(json.dumps({MAC: {}}), encoding="utf-8")
        script = (ROOT / "presence-detector.sh").read_text(encoding="utf-8")
        script, _, _ = script.rpartition('main "$@"')
        test_script = self.root / "tracker.sh"
        test_script.write_text(
            script + r"""
RUNDIR="$1/run"
LAST_SEEN="$1/last_seen"
CONFIG="$1/config"
SNAPSHOT="$1/clients"
MQTT_PORT="$2"
load_settings() {
    INTERFACES=hostapd.test
    MQTT_HOST=127.0.0.1
    MQTT_USERNAME=''
    MQTT_RETAIN_STATE=true
    FILTER_IS_DENYLIST=true
    HAS_UCODE=0
    FALLBACK_SYNC_INTERVAL=0
    AWAY_DEBOUNCE_SECONDS=0
    LOCATION=home
    AWAY=not_home
    SOURCE_TYPE=router
}
ubus() { cat "$SNAPSHOT"; }
jsonfilter() {
    case "$*" in
        *'@.clients') printf '%s\n' "$2" ;;
        *'@.params') printf '{}\n' ;;
    esac
}
log() { :; }
watch_interface() { :; }
cleanup() { exit 0; }
sleep() {
    if [ "$1" = 2 ]; then command sleep 0.2; else command sleep "$@"; fi
}
main
""",
            encoding="utf-8",
        )
        self.observe()
        tracker = subprocess.Popen(
            ["sh", str(test_script), str(self.root), str(self.port)],
            start_new_session=True,
        )

        def stop_tracker():
            try:
                os.killpg(tracker.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            tracker.wait(timeout=5)

        self.addCleanup(stop_tracker)
        self.expect_state("home")
        wait_for(lambda: not list(runtime.glob("*.presence_retry")))
        self.stop_observer()
        self.stop_broker()
        snapshot.write_text("{}", encoding="utf-8")
        (runtime / f"{MAC}.presence").touch()
        wait_for(
            lambda: (runtime / f"{MAC}.presence_retry").exists()
            and not (runtime / "aa_bb_cc_dd_ee_ff.reg").exists()
        )
        self.start_broker()
        self.observe()
        self.expect_state("not_home")


if __name__ == "__main__":
    unittest.main()
