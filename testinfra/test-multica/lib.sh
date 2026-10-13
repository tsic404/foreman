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
# Names of the test workspace's objects (§3). The reset entry rebuilds these.
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
		info "kubectl $* failed — retrying ($attempt)"
		sleep $((attempt * 5))
	done
}

# The profile's personal access token: the acceptance side's credential
# (§2 查询面凭据). Read from the profile config the CLI itself uses.
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
# MULTICA_WORKSPACE_ID, §2 接线).
workspace_id() {
	jq -er .workspace_id "$PROFILE_CONFIG" || die "profile $PROFILE_CONFIG carries no workspace_id"
}

# psql against the test server's own database, for the reset entry's row wipe
# (§7 重置). Runs inside the persistence pod; no credential leaves the cluster.
psql_ro() {
	k exec deploy/postgres -n "$NS" -- \
		psql -v ON_ERROR_STOP=1 -U multica -d multica -qtAX -c "$1"
}
