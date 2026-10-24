#!/bin/sh
# Exercise the actual ha_seen() function with mocked MQTT publication.
set -e
SOURCE="${1:-presence-detector.sh}"
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT HUP INT TERM
sed -n '/^ha_seen() {/,/^}/p' "$SOURCE" > "$DIR/function.sh"
[ -s "$DIR/function.sh" ] || exit 1
. "$DIR/function.sh"
RUNDIR="$DIR"
CONFIG="$DIR/config"
printf '{}\n' > "$CONFIG"
HAS_UCODE=0
AP_NAME=''
LOCATION=home
AWAY=not_home
SOURCE_TYPE=router
MQTT_RETAIN_STATE=true
config_attempts=0
state_attempts=0
device_slug() { printf '%s\n' "$1" | tr ':' '_'; }
is_registered() { [ -f "$RUNDIR/$1.reg" ]; }
mark_registered() { touch "$RUNDIR/$1.reg"; }
jsonfilter() { return 0; }
mqtt_pub() {
    case "$1" in
        */config)
            config_attempts=$((config_attempts + 1))
            [ "$config_attempts" -gt 1 ]
            return $?
            ;;
        */state) state_attempts=$((state_attempts + 1)); return 0 ;;
        *) return 2 ;;
    esac
}
mac='aa:bb:cc:dd:ee:ff'
slug='aa_bb_cc_dd_ee_ff'
if ha_seen "$mac" 1; then echo 'First config publish should fail' >&2; exit 1; fi
if is_registered "$slug"; then echo 'Failed discovery cached' >&2; exit 1; fi
ha_seen "$mac" 1
[ "$config_attempts" -eq 2 ] || exit 1
is_registered "$slug" || exit 1
ha_seen "$mac" 0
[ "$config_attempts" -eq 2 ] || exit 1
[ "$state_attempts" -eq 3 ] || exit 1
echo 'PASS: shell discovery retry'
