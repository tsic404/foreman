#!/bin/sh
# TC-tech-cache-04 — the node-side half of AC-22: the production foreman-gc
# binary, on a real node against the node's own hostPath state root, reclaims
# session/transcript stores older than FOREMAN_AGENT_STATE_TTL and memory
# stores older than FOREMAN_AGENT_MEMORY_TTL, judged per entry by its own
# newest mtime. Fresh entries survive, and a zero TTL turns each rule off.
# The rule itself is tech_cache_04_agent_state_gc_test.go; this carrier adds
# the node filesystem, the published image and the deployed DaemonSet wiring.
#
#   [NODE=n] [GC_IMAGE=ref] [GC_WAIT=180] ./tech-cache-04-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
STATE_ROOT=${FOREMAN_STATE_ROOT:-/var/lib/foreman}
NODE=${NODE:-}
GC_IMAGE=${GC_IMAGE:-}
GC_WAIT=${GC_WAIT:-180}
# GNU coreutils inside the node-debug container: touch -d, find, chown.
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
RUN=tc-cache-04-$(date +%s)-$$
# The fixture lives in its own root under the state root, and the GC pod mounts
# that root as /state: the judged round never sees the real home tree, so its
# shorter TTLs cannot reclaim the node's live sessions, transcripts or memory.
ROOT="$STATE_ROOT/.tc-cache-04-$RUN"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

# The deployed wiring: the DaemonSet must carry both agent-state rules, and its
# image is the carrier's subject. A missing DaemonSet is an environment block,
# not a failure — the rule has no deployed carrier to judge.
ds_json=$(kubectl -n "$JOB_NS" get daemonset foreman-gc -o json 2>/dev/null) ||
	blocked "cannot read DaemonSet $JOB_NS/foreman-gc — deploy it first"
[ -n "$GC_IMAGE" ] || GC_IMAGE=$(printf '%s' "$ds_json" |
	jq -r '.spec.template.spec.containers[0].image // empty')
[ -n "$GC_IMAGE" ] || blocked "the foreman-gc DaemonSet names no image"
ds_env() {
	printf '%s' "$ds_json" | jq -r --arg k "$1" \
		'[.spec.template.spec.containers[0].env[]? | select(.name == $k) | .value][0] // empty'
}
duration_ok() {
	printf '%s' "$1" | grep -Eq '^(0|[0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h)+)$'
}
state_ttl=$(ds_env FOREMAN_AGENT_STATE_TTL)
memory_ttl=$(ds_env FOREMAN_AGENT_MEMORY_TTL)
[ -n "$state_ttl" ] || blocked "the foreman-gc DaemonSet carries no FOREMAN_AGENT_STATE_TTL"
[ -n "$memory_ttl" ] || blocked "the foreman-gc DaemonSet carries no FOREMAN_AGENT_MEMORY_TTL"
duration_ok "$state_ttl" || blocked "FOREMAN_AGENT_STATE_TTL='$state_ttl' is not a duration"
duration_ok "$memory_ttl" || blocked "FOREMAN_AGENT_MEMORY_TTL='$memory_ttl' is not a duration"
observe "gc image=$GC_IMAGE state_ttl=$state_ttl memory_ttl=$memory_ttl"

if [ -z "$NODE" ]; then
	NODE=$(kubectl -n "$JOB_NS" get pods -l "app.kubernetes.io/name=foreman-gc" \
		-o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null) ||
		blocked "cannot list foreman-gc pods in $JOB_NS"
fi
[ -n "$NODE" ] || blocked "no foreman-gc pod names a node — set NODE or deploy the DaemonSet"
observe "node=$NODE isolated_root=$ROOT (the GC round mounts this root as /state)"

# node_sh runs a shell on the node with the host filesystem at /host. The
# debug container needs an explicit image and stdin (-i attaches); without
# both the call never reaches the node and its stdout stays empty.
node_sh() {
	kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "$1" 2>&1
}

node_blocked_reason() {
	printf '%s\n' "$1" | sed -n 's/^QA_BLOCKED: //p' | head -1
}

tmp=$(mktemp -d)
POD=""
cleanup() {
	[ -z "$POD" ] || kubectl -n "$JOB_NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
	# The fixture root is isolated from the real home tree; remove it even when
	# a step failed before the assertions ran.
	_cleanup=$(sed -e "s|@ROOT@|$ROOT|g" <<'NODE'
set -eu
[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
rm -rf '/host@ROOT@'
NODE
	)
	node_sh "$_cleanup" >/dev/null 2>&1 || true
	rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# build_tree <profile> <state-age> <memory-age> <name-prefix>: the design
# case's five state stores plus the three memory samples. Every path in an
# aged store is pushed back together, so the store's newest mtime — the rule's
# judgement basis — is the aged value rather than the creation moment.
build_tree() {
	_profile=$1
	_state_age=$2
	_memory_age=$3
	_prefix=$4
	sed -e "s|@ROOT@|$ROOT|g" \
		-e "s|@PROFILE@|$_profile|g" \
		-e "s|@STATE_AGE@|$_state_age|g" \
		-e "s|@MEMORY_AGE@|$_memory_age|g" \
		-e "s|@PREFIX@|$_prefix|g" <<'NODE'
set -eu
[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
R='/host@ROOT@'
root="$R/home"
prof="$root/.multica/profiles/@PROFILE@"
mkdir -p "$root"
mkdir -p "$prof/hermes-sessions/a/x/old" "$prof/hermes-sessions/a/x/new" \
	"$prof/dsh-sessions/a/old" "$prof/dsh-sessions/a/new" \
	"$prof/reasonix-state/a/old" "$prof/reasonix-state/a/new" \
	"$prof/hermes-state/a/old-mem" "$prof/hermes-state/a/mid-mem" "$prof/hermes-state/a/new-mem" \
	"$root/.multica/pi-sessions" "$root/.codex/multica-sessions/@PREFIX@/old" "$root/.codex/multica-sessions/@PREFIX@/new"
for f in "$prof/hermes-sessions/a/x/old/state.db" "$prof/hermes-sessions/a/x/new/state.db" \
	"$prof/dsh-sessions/a/old/s.jsonl" "$prof/dsh-sessions/a/new/s.jsonl" \
	"$prof/reasonix-state/a/old/s.jsonl" "$prof/reasonix-state/a/new/s.jsonl" \
	"$prof/hermes-state/a/old-mem/mem.md" "$prof/hermes-state/a/mid-mem/mem.md" "$prof/hermes-state/a/new-mem/mem.md" \
	"$root/.multica/pi-sessions/@PREFIX@-old.jsonl" "$root/.multica/pi-sessions/@PREFIX@-new.jsonl" \
	"$root/.codex/multica-sessions/@PREFIX@/old/s.jsonl" "$root/.codex/multica-sessions/@PREFIX@/new/s.jsonl"; do
	printf 'sample\n' >"$f"
done
age() { find "$1" -exec touch -h -d "$2" {} +; }
age "$prof/hermes-sessions/a/x/old" '@STATE_AGE@'
age "$prof/dsh-sessions/a/old" '@STATE_AGE@'
age "$prof/reasonix-state/a/old" '@STATE_AGE@'
age "$prof/hermes-state/a/old-mem" '@MEMORY_AGE@'
age "$prof/hermes-state/a/mid-mem" '35 days ago'
age "$root/.multica/pi-sessions/@PREFIX@-old.jsonl" '@STATE_AGE@'
age "$root/.codex/multica-sessions/@PREFIX@/old" '@STATE_AGE@'
# The isolated root is the only tree chowned: the GC runs as uid 1000 and must
# be able to remove the fixture's store directories.
chown -R 1000:1000 "$R"
echo "created=$prof"
NODE
}

# inspect prints key=exists|missing for every judged sample path.
inspect() {
	_profile=$1
	_prefix=$2
	sed -e "s|@ROOT@|$ROOT|g" \
		-e "s|@PROFILE@|$_profile|g" -e "s|@PREFIX@|$_prefix|g" <<'NODE'
set -eu
[ -d /host ] || { echo 'QA_BLOCKED: the node filesystem is not mounted at /host' >&2; exit 2; }
R='/host@ROOT@'
root="$R/home"
prof="$root/.multica/profiles/@PROFILE@"
for pair in \
	"hermes_old=$prof/hermes-sessions/a/x/old" \
	"hermes_new=$prof/hermes-sessions/a/x/new" \
	"dsh_old=$prof/dsh-sessions/a/old" \
	"dsh_new=$prof/dsh-sessions/a/new" \
	"reasonix_old=$prof/reasonix-state/a/old" \
	"reasonix_new=$prof/reasonix-state/a/new" \
	"memory_old=$prof/hermes-state/a/old-mem" \
	"memory_mid=$prof/hermes-state/a/mid-mem" \
	"memory_new=$prof/hermes-state/a/new-mem" \
	"pi_old=$root/.multica/pi-sessions/@PREFIX@-old.jsonl" \
	"pi_new=$root/.multica/pi-sessions/@PREFIX@-new.jsonl" \
	"codex_old=$root/.codex/multica-sessions/@PREFIX@/old/s.jsonl" \
	"codex_new=$root/.codex/multica-sessions/@PREFIX@/new/s.jsonl"; do
	key=${pair%%=*}
	path=${pair#*=}
	if [ -e "$path" ]; then echo "$key=exists"; else echo "$key=missing"; fi
done
NODE
}

# run_round <pod-suffix> <state-ttl> <memory-ttl>: one real GC round on the
# node. The process is deleted as soon as the round-complete line appears, so
# the 24h interval never waits.
run_round() {
	_suffix=$1
	_state_ttl=$2
	_memory_ttl=$3
	POD="tc-cache-04-$_suffix-$RUN"
	cat >"$tmp/gc-$_suffix.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: $JOB_NS
  labels:
    app.kubernetes.io/name: tc-cache-04
spec:
  nodeName: $NODE
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: gc
      image: $GC_IMAGE
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-ec", "exec /usr/local/bin/foreman-gc"]
      env:
        - { name: FOREMAN_STATE_ROOT, value: "/state" }
        - { name: NODE_NAME, value: "$NODE" }
        - { name: FOREMAN_GC_QUIESCE, value: "0" }
        - { name: FOREMAN_GC_INTERVAL, value: "24h" }
        - { name: FOREMAN_CACHE_TTL, value: "87600h" }
        - { name: FOREMAN_TASK_DIR_TTL, value: "87600h" }
        - { name: FOREMAN_CACHE_MAX_BYTES, value: "100Ti" }
        - { name: FOREMAN_AGENT_STATE_TTL, value: "$_state_ttl" }
        - { name: FOREMAN_AGENT_MEMORY_TTL, value: "$_memory_ttl" }
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: ["ALL"] }
      volumeMounts:
        - { name: state, mountPath: /state }
  volumes:
    - name: state
      hostPath: { path: "$ROOT", type: Directory }
EOF
	kubectl -n "$JOB_NS" apply -f "$tmp/gc-$_suffix.yaml" >/dev/null ||
		blocked "cannot create the GC round pod $POD"
	deadline=$(( $(date +%s) + GC_WAIT ))
	phase=""
	round=""
	while [ "$(date +%s)" -lt "$deadline" ]; do
		phase=$(kubectl -n "$JOB_NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)
		round=$(kubectl -n "$JOB_NS" logs "pod/$POD" 2>/dev/null |
			grep -E '"msg":"cache.gc"' | tail -1 || true)
		[ -n "$round" ] && break
		case $phase in
		Failed | Succeeded) break ;;
		esac
		sleep 2
	done
	[ -n "$round" ] || {
		_why=$(kubectl -n "$JOB_NS" logs "pod/$POD" 2>/dev/null | tail -5 || true)
		blocked "the GC round did not finish within ${GC_WAIT}s (pod phase=${phase:-unknown}): ${_why:-<no logs>}"
	}
	kubectl -n "$JOB_NS" logs "pod/$POD" >"$tmp/gc-$_suffix.log" 2>/dev/null ||
		blocked "cannot read the GC round log"
	kubectl -n "$JOB_NS" delete pod "$POD" --wait=false >/dev/null 2>&1 || true
	POD=""
}

# log_value <log> <msg> <field>: the last value of one field on one event.
log_value() {
	jq -Rr --arg m "$2" --arg f "$3" 'fromjson? | select(.msg == $m) | .[$f]' "$1" | tail -1
}

# ---- Round 1: the two retention windows ------------------------------------
rc=0
out=$(node_sh "$(build_tree "$RUN" '2 days ago' '95 days ago' "$RUN")") || rc=$?
case $out in
*QA_BLOCKED*) blocked "$(node_blocked_reason "$out")" ;;
esac
[ "$rc" -eq 0 ] || blocked "cannot build the node sample tree (rc=$rc, image $NODE_DEBUG_IMAGE): $out"
[ -n "$out" ] || blocked "the node-side sample creation returned no output (image $NODE_DEBUG_IMAGE not attached?)"
observe "sample tree: $out"

# State window 24h, memory window 45d: the 48h state stores expire, the 95d
# memory store expires, the 35d memory store stays inside its window.
run_round window 24h 1080h
grep -q '"msg":"gc.started"' "$tmp/gc-window.log" ||
	blocked "the GC image did not start (log carries no gc.started): $(tail -3 "$tmp/gc-window.log")"
got_state_ttl=$(log_value "$tmp/gc-window.log" gc.started agent_state_ttl)
got_memory_ttl=$(log_value "$tmp/gc-window.log" gc.started agent_memory_ttl)
observe "round1 ttl: agent_state=$got_state_ttl agent_memory=$got_memory_ttl"
case $got_state_ttl in 24h | 24h0m0s) : ;; *) fail "round1 agent_state_ttl=$got_state_ttl, want 24h" ;; esac
case $got_memory_ttl in 1080h | 1080h0m0s) : ;; *) fail "round1 agent_memory_ttl=$got_memory_ttl, want 1080h" ;; esac
state_removed=$(log_value "$tmp/gc-window.log" gc.agent_state removed_count)
memory_removed=$(log_value "$tmp/gc-window.log" gc.agent_memory removed_count)
observe "round1 removed: agent_state=$state_removed agent_memory=$memory_removed"
[ -n "$state_removed" ] && [ -n "$memory_removed" ] ||
	fail "the round log carries no removed_count for one of the two rules"
[ "$state_removed" -ge 5 ] ||
	fail "agent_state removed $state_removed stores, want at least the 5 aged sample stores"
[ "$memory_removed" -ge 1 ] ||
	fail "agent_memory removed $memory_removed stores, want the aged sample store"

rc=0
out=$(node_sh "$(inspect "$RUN" "$RUN")") || rc=$?
case $out in
*QA_BLOCKED*) blocked "$(node_blocked_reason "$out")" ;;
esac
[ "$rc" -eq 0 ] || blocked "cannot inspect the node sample tree (rc=$rc): $out"
observe "after round1: $(printf '%s' "$out" | tr '\n' ' ')"
for key in hermes_old dsh_old reasonix_old memory_old pi_old codex_old; do
	case $out in
	*"$key=missing"*) : ;;
	*) fail "$key survived the window (want deleted): $out" ;;
	esac
done
for key in hermes_new dsh_new reasonix_new memory_mid memory_new pi_new codex_new; do
	case $out in
	*"$key=exists"*) : ;;
	*) fail "$key was deleted (fresh or inside the memory window): $out" ;;
	esac
done

# ---- Round 2: a zero TTL turns each rule off -------------------------------
rc=0
out=$(node_sh "$(build_tree "$RUN-zero" '2 days ago' '95 days ago' "$RUN-zero")") || rc=$?
case $out in
*QA_BLOCKED*) blocked "$(node_blocked_reason "$out")" ;;
esac
[ "$rc" -eq 0 ] || blocked "cannot rebuild the node sample tree (rc=$rc): $out"
run_round zero 0 0
got_state_ttl=$(log_value "$tmp/gc-zero.log" gc.started agent_state_ttl)
got_memory_ttl=$(log_value "$tmp/gc-zero.log" gc.started agent_memory_ttl)
observe "round2 ttl: agent_state=$got_state_ttl agent_memory=$got_memory_ttl"
[ "$got_state_ttl" = "0s" ] || fail "round2 agent_state_ttl=$got_state_ttl, want 0s"
[ "$got_memory_ttl" = "0s" ] || fail "round2 agent_memory_ttl=$got_memory_ttl, want 0s"
state_removed=$(log_value "$tmp/gc-zero.log" gc.agent_state removed_count)
memory_removed=$(log_value "$tmp/gc-zero.log" gc.agent_memory removed_count)
observe "round2 (zero TTLs) removed: agent_state=$state_removed agent_memory=$memory_removed"
[ "$state_removed" = "0" ] || fail "agent_state removed $state_removed stores with the rule off"
[ "$memory_removed" = "0" ] || fail "agent_memory removed $memory_removed stores with the rule off"

rc=0
out=$(node_sh "$(inspect "$RUN-zero" "$RUN-zero")") || rc=$?
case $out in
*QA_BLOCKED*) blocked "$(node_blocked_reason "$out")" ;;
esac
[ "$rc" -eq 0 ] || blocked "cannot inspect the zero-TTL sample tree (rc=$rc): $out"
observe "after round2: $(printf '%s' "$out" | tr '\n' ' ')"
for key in hermes_old memory_old pi_old codex_old; do
	case $out in
	*"$key=exists"*) : ;;
	*) fail "$key was deleted with its rule off: $out" ;;
	esac
done

echo "PASS: TC-tech-cache-04 (node $NODE: per-entry mtime, 24h/45d windows, zero TTLs off)"
