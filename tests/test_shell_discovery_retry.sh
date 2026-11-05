#!/bin/sh
# Run the production publisher and queue with stubbed hostapd and MQTT.
set -eu
SOURCE="${1:-presence-detector.sh}"
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT
trap 'exit 1' HUP INT TERM
sed '/^main "$@"$/d' "$SOURCE" > "$DIR/functions.sh"
. "$DIR/functions.sh"

mac='aa:bb:cc:dd:ee:ff'
slug='test_aa_bb_cc_dd_ee_ff'

log() { :; }
jsonfilter() {
    case "$*" in
        *'@.clients') printf '%s\n' "$2" ;;
        *'@.params') printf '{}\n' ;;
    esac
}
ubus() {
    case "$2" in
        hostapd.first) cat "$RUNDIR/first" ;;
        hostapd.second) cat "$RUNDIR/second" ;;
        *) return 4 ;;
    esac
}
mqtt_pub() {
    case "$1" in
        */config) kind=config ;;
        */state) kind=state ;;
        *) return 2 ;;
    esac
    if [ "${block:-}" = "$kind" ]; then
        printf 'publishing\n' > "$RUNDIR/entered"
        read -r _ < "$RUNDIR/release"
    fi
    printf '%s %s %s\n' "$kind" "$3" "$2" >> "$RUNDIR/published"
    [ ! -f "$RUNDIR/offline" ]
}
setup() {
    RUNDIR="$DIR/$1"
    mkdir "$RUNDIR"
    LAST_SEEN="$RUNDIR/last_seen"
    CONFIG="$RUNDIR/config"
    printf '{}\n' > "$CONFIG"
    printf '{"%s":{}}\n' "$mac" > "$RUNDIR/first"
    printf '{}\n' > "$RUNDIR/second"
    HAS_UCODE=0
    AP_NAME=test
    LOCATION=home
    AWAY=not_home
    SOURCE_TYPE=router
    MQTT_RETAIN_STATE="${2:-true}"
    FILTER_IS_DENYLIST=true
    FILTER=''
    INTERFACES='hostapd.first hostapd.second'
    block=''
}
fail() { echo "FAIL: $*" >&2; exit 1; }

for retain in true false; do
    setup "retry-$retain" "$retain"
    touch "$RUNDIR/offline"
    queue_presence "$mac"
    process_presence 0 0
    [ -f "$RUNDIR/$mac.presence_retry" ] || fail 'missing retry marker'
    ! is_registered "$slug" || fail 'failed config cached'
    [ "$(wc -l < "$RUNDIR/published" | tr -d ' ')" -eq 2 ] || fail 'expected both publish attempts'
    # Failed retries stay bounded and leave no temporary files behind.
    for _ in 1 2 3; do process_presence 0 1; done
    set -- "$RUNDIR"/*.presence*
    [ "$#" -eq 1 ] && [ "$1" = "$RUNDIR/$mac.presence_retry" ] || fail 'leaked work files'
    # No event arrives while the device leaves and the broker comes back.
    printf '{}\n' > "$RUNDIR/first"
    rm "$RUNDIR/offline"
    : > "$RUNDIR/published"
    process_presence 0 1
    flag=0
    [ "$retain" = false ] || flag=1
    [ "$(awk '{print $1, $2}' "$RUNDIR/published")" = "$(printf 'config 1\nstate %s' "$flag")" ] ||
        fail "discovery followed by state with retain=$retain"
    grep -q '^state .*"state":"not_home"' "$RUNDIR/published" || fail 'retry replayed old HOME'
    is_registered "$slug" || fail 'missing registration'
    [ ! -f "$RUNDIR/$mac.presence_retry" ] || fail 'completed work kept'

    # A failure after discovery was already cached must still retry both
    # messages. A broker restart can have discarded the retained config.
    touch "$RUNDIR/offline"
    queue_presence "$mac"
    process_presence 0 0
    ! is_registered "$slug" || fail 'cached discovery survived state failure'
    rm "$RUNDIR/offline"
    : > "$RUNDIR/published"
    process_presence 0 1
    grep -q '^config ' "$RUNDIR/published" || fail 'state retry did not restore discovery'
    grep -q '^state .*"state":"not_home"' "$RUNDIR/published" || fail 'state retry did not publish'
done

# A watcher can record a disassoc while the old HOME is in discovery or
# state publication. Only the main loop publishes, using the newer snapshot
# on the next pass. FIFOs order the two processes without sleeps.
for phase in config state; do
    setup "concurrent-$phase"
    mkfifo "$RUNDIR/entered" "$RUNDIR/release"
    queue_presence "$mac"
    (
        read -r _ < "$RUNDIR/entered"
        printf '{}\n' > "$RUNDIR/first"
        queue_presence "$mac"
        printf 'continue\n' > "$RUNDIR/release"
    ) &
    watcher=$!
    block=$phase
    process_presence 0 0
    block=''
    wait "$watcher"
    process_presence 0 0
    states=$(sed -n 's/^state .*"state":"\([^"]*\)".*/\1/p' "$RUNDIR/published")
    expected=not_home
    if [ "$phase" = state ]; then expected=$(printf 'home\nnot_home'); fi
    [ "$states" = "$expected" ] || fail "out-of-order state with $phase blocked"
done

setup concurrent-reset
mkfifo "$RUNDIR/entered" "$RUNDIR/release"
queue_presence "$mac"
(
    read -r _ < "$RUNDIR/entered"
    touch "$RUNDIR/.reset_registrations" "$RUNDIR/.full_sync"
    printf 'continue\n' > "$RUNDIR/release"
) &
watcher=$!
block=config
process_presence 0 0
block=''
wait "$watcher"
! is_registered "$slug" || fail 'in-flight config undid reset'
! grep -q '^state ' "$RUNDIR/published" || fail 'published state from invalidated batch'
process_sync_requests
[ "$(grep -c '^config ' "$RUNDIR/published")" -eq 2 ] || fail 'sync after reset did not restore discovery'
is_registered "$slug" || fail 'new discovery not cached'

# The actual startup path restores both older discovery-only markers and
# in-flight work. The stopped test instance never launches real watchers.
for format in legacy current; do
    setup "restart-$format"
    if [ "$format" = legacy ]; then
        printf '%s 1\n' "$mac" > "$RUNDIR/$slug.discovery_retry"
        touch "$RUNDIR/$slug.discovery_retry.interrupted"
    else
        touch "$RUNDIR/$mac.presence_retry"
    fi
    printf '{}\n' > "$RUNDIR/first"
    (
        load_settings() { FALLBACK_SYNC_INTERVAL=0; AWAY_DEBOUNCE_SECONDS=0; }
        watch_ha_status() { :; }
        watch_interface() { :; }
        cleanup() { exit 0; }
        sleep() { touch "$RUNDIR/.stop"; }
        main
    )
    grep -q '^config ' "$RUNDIR/published" || fail 'lost discovery retry on restart'
    grep -q '^state .*"state":"not_home"' "$RUNDIR/published" || fail 'restart replayed old HOME'
    [ ! -f "$RUNDIR/$mac.presence_retry" ] || fail 'completed restart work kept'
    [ ! -f "$RUNDIR/$slug.discovery_retry.interrupted" ] || fail 'old temporary file kept'
done

setup roaming
# hostapd.first emits disassoc, but the device is already on hostapd.second.
printf '{}\n' > "$RUNDIR/first"
printf '{"%s":{}}\n' "$mac" > "$RUNDIR/second"
queue_presence "$mac"
process_presence 0 0
grep -q '^state .*"state":"home"' "$RUNDIR/published" || fail 'roaming published AWAY'

setup incomplete-snapshot
rm "$RUNDIR/first"
queue_presence "$mac"
process_presence 0 0
[ ! -f "$RUNDIR/published" ] || fail 'published AWAY when hostapd query failed'
[ -f "$RUNDIR/$mac.presence_retry" ] || fail 'lost work when hostapd query failed'
echo 'PASS: shell discovery, presence retry, reset and ordering'
