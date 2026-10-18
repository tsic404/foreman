#!/bin/sh
# Shared settings and helpers for the test multica cluster scripts.
#
# Everything is overridable from the environment so one checkout can drive more
# than one test cluster; the defaults are the ones this repository's test
# cluster uses.

set -eu

TEST_MULTICA_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export TEST_MULTICA_DIR

# --- Environment identity ----------------------------------------------------
NS=${TEST_MULTICA_NS:-multica-test}
PROFILE=${TEST_MULTICA_PROFILE:-test-multica}
# Addresses of the two faces (the node ports in deploy.yaml). The node shares
# the test host's network namespace, so both are reachable on the host itself:
# loopback is the stable form, and the default. The node's InternalIP is the
# host's own address and changes underneath a running deployment (it is
# whatever the host was given when it booted) — deploy.sh bakes the entry
# origin into the server's config, so that address would go stale without
# anyone touching the cluster. TEST_MULTICA_HOST (or the two *_URL overrides)
# is for consumers that are not on this host.
HOST=${TEST_MULTICA_HOST:-127.0.0.1}
SERVER_URL=${TEST_MULTICA_SERVER_URL:-http://$HOST:30080}
ENTRY_URL=${TEST_MULTICA_ENTRY_URL:-http://$HOST:30300}
# The stable access point: the test server itself, nothing in front of it.
# It has one face per consumer — in-cluster consumers (the instance under test)
# resolve the Service name, host-side ones (the CLI profile, self-check ③) use
# SERVER_URL above. Both are that one server, and neither is the production one.
ACCESS_URL=${TEST_MULTICA_ACCESS_URL:-http://test-multica-server.$NS.svc:8080}
# The instance under test: the Deployment the wiring and self-check entries
# read and re-point. It comes from deploy/ (deploy/README.md); the
# test cluster only decides where it points.
INSTANCE_NS=${TEST_MULTICA_INSTANCE_NS:-foreman}
INSTANCE=${TEST_MULTICA_INSTANCE:-foreman}
INSTANCE_SECRET=${TEST_MULTICA_INSTANCE_SECRET:-foreman-secret}
# Names of the test workspace's objects. The reset entry rebuilds these.
WORKSPACE_NAME=${TEST_MULTICA_WORKSPACE_NAME:-Foreman Test}
WORKSPACE_SLUG=${TEST_MULTICA_WORKSPACE_SLUG:-foreman-test}
ACCOUNT_EMAIL=${TEST_MULTICA_EMAIL:-acceptance@test.local}
AGENT_NAME=${TEST_MULTICA_AGENT:-foreman-test-agent}
AUTOPILOT_TITLE=${TEST_MULTICA_AUTOPILOT:-foreman-test-autopilot}
CHAT_TITLE=${TEST_MULTICA_CHAT:-foreman-test-chat}
REPO_URL=${TEST_MULTICA_REPO:-https://github.com/tsic404/foreman}
# Development login code (MULTICA_DEV_VERIFICATION_CODE in the Secret).
VERIFICATION_CODE=${TEST_MULTICA_VERIFICATION_CODE:-888888}

PROFILE_CONFIG="$HOME/.multica/profiles/$PROFILE/config.json"
MULTICA_TEST_CLI="$TEST_MULTICA_DIR/multica-test"

# --- Helpers -----------------------------------------------------------------

die() {
	printf 'test-multica: %s\n' "$*" >&2
	exit 1
}

info() {
	printf 'test-multica: %s\n' "$*" >&2
}

need() {
	command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# The CLI refuses a non-task token when it sees a daemon task marker (it walks
# up from the working directory) or the platform's task environment. The
# wrapper clears both; scripts must go through it instead of calling multica
# directly.
mcli() {
	"$MULTICA_TEST_CLI" "$@"
}

# The retry line is a diagnostic, not a record: an argument that could carry a
# credential is masked whole before it reaches it (the value sits inside the
# argument, so masking the value alone would still leak the rest of the pair).
redact() {
	_redacted=
	for _arg in "$@"; do
		case $_arg in
		*token* | *TOKEN* | *Authorization* | *Bearer*) _arg=REDACTED ;;
		esac
		_redacted="$_redacted$_arg "
	done
	printf '%s' "${_redacted% }"
}

# kubectl with retries. The test cluster shares its node with the instance
# under test; a starved API server answers with a TLS handshake timeout instead
# of an error, which is not a reason to fail a reset.
#
# Do not wrap a command that reads stdin (`kubectl apply -f -`): a retry would
# find the stream already consumed. Render to a file first.
k() {
	attempt=0
	while :; do
		if kubectl "$@"; then
			return 0
		fi
		attempt=$((attempt + 1))
		if [ "$attempt" -ge "${TEST_MULTICA_KUBECTL_RETRIES:-5}" ]; then
			return 1
		fi
		info "kubectl $(redact "$@") failed — retrying ($attempt)"
		sleep $((attempt * 5))
	done
}

# The profile's personal access token: the acceptance side's credential
# (查询面凭据). Read from the profile config the CLI itself uses.
pat() {
	[ -r "$PROFILE_CONFIG" ] || die "no CLI profile at $PROFILE_CONFIG — run ./reset.sh first"
	jq -er .token "$PROFILE_CONFIG" || die "profile $PROFILE_CONFIG carries no token"
}

# api <method> <path> [json-body] — authenticated call against the test server.
api() {
	method=$1
	path=$2
	body=${3:-}
	if [ -n "$body" ]; then
		curl -fsS -m 60 -X "$method" "$SERVER_URL$path" \
			-H "Authorization: Bearer $(pat)" \
			-H 'Content-Type: application/json' \
			-d "$body"
	else
		curl -fsS -m 60 -X "$method" "$SERVER_URL$path" \
			-H "Authorization: Bearer $(pat)"
	fi
}

# Workspace-scoped variant of api().
wsapi() {
	method=$1
	path=$2
	body=${3:-}
	ws=$(workspace_id)
	if [ -n "$body" ]; then
		curl -fsS -m 60 -X "$method" "$SERVER_URL$path" \
			-H "Authorization: Bearer $(pat)" \
			-H "X-Workspace-ID: $ws" \
			-H 'Content-Type: application/json' \
			-d "$body"
	else
		curl -fsS -m 60 -X "$method" "$SERVER_URL$path" \
			-H "Authorization: Bearer $(pat)" \
			-H "X-Workspace-ID: $ws"
	fi
}

# The test workspace id from the profile (the acceptance side's
# MULTICA_WORKSPACE_ID).
workspace_id() {
	jq -er .workspace_id "$PROFILE_CONFIG" || die "profile $PROFILE_CONFIG carries no workspace_id"
}

# psql against the test server's own database, for the reset entry's row wipe.
# Runs inside the persistence pod; no credential leaves the cluster.
psql_ro() {
	k exec deploy/postgres -n "$NS" -- \
		psql -v ON_ERROR_STOP=1 -U multica -d multica -qtAX -c "$1"
}

# --- The instance under test -------------------------------------------------

# Is one object of the instance under test there? 0 = present, 1 = definitively
# absent, dies when the API cannot answer at all. The first read is unretried
# because NotFound is a definitive answer and a retry loop would spend its
# budget on it; anything else is a read failure — a starved API server answers
# with a TLS handshake timeout here — and goes through k() like every other
# read.
instance_object_present() {
	_kind=$1
	_name=$2
	if _out=$(kubectl -n "$INSTANCE_NS" get "$_kind" "$_name" -o name 2>&1); then
		return 0
	fi
	case $_out in
	*NotFound*) return 1 ;;
	esac
	k -n "$INSTANCE_NS" get "$_kind" "$_name" -o name >/dev/null 2>&1 ||
		die "cannot read $_kind/$_name in $INSTANCE_NS — $_out"
	return 0
}

# One env key of the instance under test's first container (`containers[0]`).
instance_env() {
	instance_object_present deploy "$INSTANCE" || return 1
	k -n "$INSTANCE_NS" get deploy "$INSTANCE" \
		-o jsonpath="{.spec.template.spec.containers[0].env[?(@.name==\"$1\")].value}"
}

# Scale the instance under test to 0 and wait for its pod to go away. Gate ②
# is a pre-start assertion — the runtime list must be empty before the instance
# starts — so both the reset entry (the wipe has to be observable) and the
# wiring entry (the assertion must not race its own rollout) bring it down
# first. An instance that is not deployed at all is left to the self-check,
# which reports the missing Deployment as the blocked environment it is.
stop_instance() {
	if ! instance_object_present deploy "$INSTANCE"; then
		info "instance $INSTANCE_NS/$INSTANCE is not deployed — nothing to stop"
		return 0
	fi
	replicas=$(k -n "$INSTANCE_NS" get deploy "$INSTANCE" -o jsonpath='{.spec.replicas}')
	if [ "${replicas:-0}" -eq 0 ]; then
		info "instance $INSTANCE_NS/$INSTANCE is already stopped"
		return 0
	fi
	k -n "$INSTANCE_NS" scale deploy "$INSTANCE" --replicas=0 >/dev/null
	# The wait runs only while pods are listed: `--for=delete` errors when the
	# selector matches nothing, which is the state wanted here. A pod that
	# outlives the wait is reported instead of swallowed — the gate's ② that
	# follows is what decides, and it needs the pod actually gone.
	selector=$(k -n "$INSTANCE_NS" get deploy "$INSTANCE" -o jsonpath='{.spec.selector.matchLabels}' |
		jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")') ||
		die "cannot read the selector of $INSTANCE_NS/$INSTANCE"
	[ -n "$selector" ] || die "Deployment $INSTANCE_NS/$INSTANCE has no selector labels"
	pods=$(k -n "$INSTANCE_NS" get pod -l "$selector" -o name) ||
		die "cannot list the pods of $INSTANCE_NS/$INSTANCE"
	if [ -n "$pods" ]; then
		kubectl -n "$INSTANCE_NS" wait --for=delete pod -l "$selector" --timeout=120s >/dev/null 2>&1 ||
			printf 'test-multica: error: pods of %s/%s are still terminating after 120s\n' \
				"$INSTANCE_NS" "$INSTANCE" >&2
	fi
	info "stopped $INSTANCE_NS/$INSTANCE"
}
