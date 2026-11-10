#!/bin/sh
# Pin the test agent's Jobs to one node — the same-node window tech-cache-02
# needs (README §Same-node window).
#
#   ./pin-node.sh <node>    # job policy: a hard hostname nodeSelector for the
#                           # test agent, loaded by a roll of the instance
#   ./pin-node.sh --off     # remove the policy object: back to the shipped
#                           # no-axis deployment
#
# The case judges two independent tasks that ran concurrently on one node. The
# Job template cannot build that window by itself: its preferred-node item is
# soft and exists only for a continuation (a learned chat session), and its
# spread constraint keeps independent pods apart. The product's designed
# placement knob is the job policy's nodeSelector axis (02-contracts.md §8.5,
# ADR-036): a hard selector on the agent row, which also suppresses the
# preferred-node item.
#
# The file is read once at startup, so the entry rolls the instance and checks
# the startup log carries the loaded file's sha256 before reporting success — a
# pin that is not loaded is a silent no-op. Run it after ./reset.sh (the policy
# key is resolved against the server's agents and refuses startup when absent)
# and remove it with --off when the window is done: while it is on, every task
# of the test agent is pinned, and the preferred-node item cache-05/06 judge is
# suppressed.
#
# The policy file is read once at startup, so a pin written while the instance
# is stopped cannot be verified here. That path is its own terminal state: the
# entry writes the expected startup line as a witness and exits 3, and
# ./wire.sh verifies the witness against the started instance's log (the same
# hash check this entry makes when the instance is already running). Running
# ./pin-node.sh --verify performs that check on demand.
#
# Exit: 0 = the pinned (or restored) state is loaded (or nothing is pending);
#       1 = it is not loaded; 3 = written, not yet verified (instance stopped);
#       wire.sh verifies the witness after start.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need jq
need sha256sum

pin=1
verify=0
node=
for arg in "$@"; do
	case $arg in
	-h | --help)
		sed -n '2,35p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	--off) pin=0 ;;
	--verify) verify=1 ;;
	-*) die "unknown argument: $arg" ;;
	*)
		[ -z "$node" ] || die "one node at a time (got '$node' and '$arg')"
		node=$arg
		;;
	esac
done
if [ "$verify" -eq 1 ]; then
	[ "$pin" -eq 1 ] && [ -z "$node" ] ||
		die "--verify takes no other argument"
else
	[ "$pin" -eq 1 ] || [ -z "$node" ] ||
		die "--off takes no node (got '$node')"
	[ "$pin" -eq 0 ] || [ -n "$node" ] || die "usage: ./pin-node.sh <node> | ./pin-node.sh --off | ./pin-node.sh --verify"
fi

# The policy key is the agent's name, and the startup name resolution refuses a
# key the server reports absent — that refusal is a CrashLoopBackOff, so the
# entry checks the row first and names ./reset.sh instead.
check_agent_exists() {
	wsapi GET /api/agents |
		jq -e --arg name "$AGENT_NAME" 'any(.name == $name and (.archived_at == null))' >/dev/null ||
		die "agent $AGENT_NAME does not exist on $SERVER_URL — run ./reset.sh first"
}

# instance_pod prints the newest pod of the instance under test. The rollout's
# previous pod may still be terminating beside it, so the deployment's own
# "first pod" is not a stable read.
instance_pod() {
	_selector=$(k -n "$INSTANCE_NS" get deploy "$INSTANCE" -o jsonpath='{.spec.selector.matchLabels}' |
		jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")') ||
		die "cannot read the selector of $INSTANCE_NS/$INSTANCE"
	[ -n "$_selector" ] ||
		die "Deployment $INSTANCE_NS/$INSTANCE has no selector labels"
	k -n "$INSTANCE_NS" get pod -l "$_selector" \
		--sort-by=.metadata.creationTimestamp -o name |
		sed -n '$p'
}

# restart_instance rolls the Deployment and waits for it. 0 = a running pod is
# there to verify; 1 = the instance is stopped, so the policy is read at its
# next start and nothing can be verified now.
restart_instance() {
	_replicas=$(k -n "$INSTANCE_NS" get deploy "$INSTANCE" -o jsonpath='{.spec.replicas}' 2>/dev/null) ||
		die "Deployment $INSTANCE_NS/$INSTANCE is not deployed — apply deploy/ first (deploy/README.md)"
	if [ "${_replicas:-0}" -eq 0 ]; then
		info "instance $INSTANCE_NS/$INSTANCE is stopped — the policy is read at its next start"
		return 1
	fi
	kubectl -n "$INSTANCE_NS" rollout restart deploy/"$INSTANCE" >/dev/null ||
		die "cannot restart $INSTANCE_NS/$INSTANCE"
	kubectl -n "$INSTANCE_NS" rollout status deploy/"$INSTANCE" \
		--timeout="${PIN_ROLLOUT_TIMEOUT:-300}s" >/dev/null ||
		die "$INSTANCE_NS/$INSTANCE did not become ready within ${PIN_ROLLOUT_TIMEOUT:-300}s — kubectl -n $INSTANCE_NS describe deploy/$INSTANCE logs the reason"
	return 0
}

# check_loaded <log fragment>: the new pod's startup log must carry it. The
# policy file is evidence of what the process loaded (AC-29 / AC-44), so a pin
# that did not reach the file is reported here, not discovered by the case.
check_loaded() {
	_pod=$(instance_pod)
	[ -n "$_pod" ] || die "no pod for $INSTANCE_NS/$INSTANCE after the rollout"
	_logs=$(kubectl -n "$INSTANCE_NS" logs "$_pod" --tail=400 2>/dev/null) ||
		die "cannot read the log of $_pod"
	printf '%s' "$_logs" | grep -qF "$1" ||
		die "$_pod did not report '$1' — the policy file was not loaded; check the job-policy volume of $INSTANCE_NS/$INSTANCE"
}

work=$(mktemp -d) || die "cannot create a temporary directory"
trap 'rm -rf "$work"' EXIT

# The witness a stopped-instance pin/restore leaves for ./wire.sh: the exact
# startup-log fragment the started instance must print. An absent witness means
# nothing is pending and --verify is a no-op success; a witness that exists but
# cannot be read is an error, not an absence.
POLICY_WITNESS=${PIN_NODE_WITNESS:-$HOME/.foreman-job-policy-witness}

verify_pending_witness() {
	if [ ! -e "$POLICY_WITNESS" ]; then
		info "no pending job-policy pin to verify"
		return 0
	fi
	[ -r "$POLICY_WITNESS" ] ||
		die "the pending job-policy witness $POLICY_WITNESS exists but is unreadable — fix its permissions or remove it; the startup check cannot be verified"
	_expect=$(sed -n 's/^expect=//p' "$POLICY_WITNESS" | head -1)
	[ -n "$_expect" ] ||
		die "the policy witness $POLICY_WITNESS carries no expect= line — remove it and re-run ./pin-node.sh"
	_pod=$(instance_pod)
	[ -n "$_pod" ] || die "no pod for $INSTANCE_NS/$INSTANCE to verify the pending policy pin against"
	_logs=$(kubectl -n "$INSTANCE_NS" logs "$_pod" --tail=400 2>/dev/null) ||
		die "cannot read the log of $_pod"
	printf '%s' "$_logs" | grep -qF "$_expect" ||
		die "$_pod did not report '$_expect' — the pending job-policy pin was not loaded; check the job-policy volume of $INSTANCE_NS/$INSTANCE (witness kept at $POLICY_WITNESS)"
	rm -f "$POLICY_WITNESS"
	info "pending job-policy pin verified against $_pod: $_expect"
}

if [ "$verify" -eq 1 ]; then
	verify_pending_witness
	exit 0
fi

if [ "$pin" -eq 1 ]; then
	[ -n "$node" ] || die "usage: ./pin-node.sh <node> | ./pin-node.sh --off"
	kubectl get node "$node" -o name >/dev/null 2>&1 ||
		die "node $node is not in the cluster — kubectl get nodes --show-labels"
	check_agent_exists
	{
		printf 'agents:\n'
		printf '  %s:\n' "$(yaml_scalar "$AGENT_NAME")"
		printf '    nodeSelectorPolicy: agent\n'
		printf '    nodeSelector:\n'
		printf '      kubernetes.io/hostname: %s\n' "$(yaml_scalar "$node")"
	} >"$work/policy.yaml"
	kubectl -n "$INSTANCE_NS" create configmap foreman-job-policy \
		--from-file=policy.yaml="$work/policy.yaml" --dry-run=client -o yaml |
		kubectl apply -f - >/dev/null ||
		die "cannot write ConfigMap $INSTANCE_NS/foreman-job-policy"
	sum=$(sha256sum "$work/policy.yaml" | cut -d' ' -f1) ||
		die "cannot hash the policy file $work/policy.yaml"
	case $sum in
	'' | *[!0-9a-f]*)
		die "the policy file's sha256 is not a 64-character lowercase hex digest (got '$sum') — the startup log check would degrade to a match on the label alone"
		;;
	esac
	[ "${#sum}" -eq 64 ] ||
		die "the policy file's sha256 is ${#sum} characters, not 64 (got '$sum')"
	info "pinned agent $AGENT_NAME to node $node (policy sha256 $sum)"
	if restart_instance; then
		check_loaded "sha256=$sum"
		rm -f "$POLICY_WITNESS"
		info "next: trigger two independent long tasks (both land on $node), then run tech-cache-02-cluster.sh with their ids right away — it waits for both and captures the failing Job's pod"
		exit 0
	fi
	# The file is read at startup: a pin written while the instance is stopped
	# is not loaded yet, and reporting success would be the silent no-op this
	# entry exists to prevent. Record what the started instance must print and
	# let wire.sh verify it.
	printf 'expect=sha256=%s\nnode=%s\nwritten_at=%s\n' "$sum" "$node" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$POLICY_WITNESS" ||
		die "cannot write the policy witness at $POLICY_WITNESS"
	info "policy written; the instance is stopped, so the pin is not verified yet — ./wire.sh will compare the startup log against $POLICY_WITNESS"
	info "next: ./wire.sh starts the instance with the pin loaded; then trigger two independent long tasks and run tech-cache-02-cluster.sh right away — it waits and captures the failing Job's pod"
	exit 3
fi

kubectl -n "$INSTANCE_NS" delete configmap foreman-job-policy --ignore-not-found >/dev/null ||
	die "cannot delete ConfigMap $INSTANCE_NS/foreman-job-policy"
info "removed the job policy ($INSTANCE_NS/foreman-job-policy): no axis configured"
if restart_instance; then
	check_loaded "job policy file missing"
	rm -f "$POLICY_WITNESS"
	info "next: the preferred-node item cache-05/06 judge is no longer suppressed"
	exit 0
fi
printf 'expect=job policy file missing\nnode=(off)\nwritten_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$POLICY_WITNESS" ||
	die "cannot write the policy witness at $POLICY_WITNESS"
info "policy removed; the instance is stopped, so the restore is not verified yet — ./wire.sh will compare the startup log against $POLICY_WITNESS"
exit 3
