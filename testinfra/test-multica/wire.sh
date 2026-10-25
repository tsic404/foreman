#!/bin/sh
# Wire the instance under test to the test multica cluster — the wiring step of
# the test environment (`README.md`), between ./reset.sh and
# ./bind-agent.sh.
#
#   ./wire.sh
#
# What it does, in order:
#
#   1. stop — scale Deployment foreman to 0: gate ② is a pre-start assertion
#      (the runtime list must be empty before the instance starts), and
#      patching a running instance would roll it before that assertion could
#      run;
#   2. wire — MULTICA_SERVER_URL := the access point (never the production
#      address), MULTICA_WORKSPACE_ID := the test workspace, and the
#      Deployment's MULTICA_TOKEN := the credential the test server minted for
#      that workspace (the profile's);
#   3. self-check — ./selfcheck.sh, the environment gate ①②③④, while the
#      instance is still stopped;
#   4. start — scale back to 1 and wait for the rollout.
#
# The Deployment object itself is not created here: it comes from
# `kubectl apply -f deploy/` (deploy/README.md), whose MULTICA_SERVER_URL is
# the production address — this entry is what points it at the test cluster.
# Idempotent: re-running it re-asserts the same wiring and restarts the
# instance.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need curl
need jq

for arg in "$@"; do
	case $arg in
	-h | --help)
		sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*) die "unknown argument: $arg" ;;
	esac
done

[ -r "$PROFILE_CONFIG" ] ||
	die "no CLI profile at $PROFILE_CONFIG — run ./reset.sh first (it mints the test server credential)"

ws=$(workspace_id)
token=$(pat)

# The credential travels through files, never arguments: process arguments are
# readable on this host (ps) and end up in retry diagnostics. mktemp -d is 0700,
# and it is removed on exit.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
printf 'header = "Authorization: Bearer %s"\n' "$token" >"$work/curlrc"
printf '%s' "$token" >"$work/token"

# The credential is the test server's, for the test workspace: check
# it before the instance is touched, so a stale profile (a rebuilt test cluster
# leaves the old one behind) fails here instead of as a crash loop.
status=$(curl -sS -m 30 -o /dev/null -w '%{http_code}' --config "$work/curlrc" "$SERVER_URL/api/me") ||
	die "cannot reach $SERVER_URL"
[ "$status" = 200 ] ||
	die "the profile token is rejected by $SERVER_URL (HTTP $status) — run ./reset.sh to re-issue it"

instance_object_present deploy "$INSTANCE" ||
	die "Deployment $INSTANCE_NS/$INSTANCE does not exist — deploy the instance under test first (deploy/README.md)"
container=$(k -n "$INSTANCE_NS" get deploy "$INSTANCE" -o jsonpath='{.spec.template.spec.containers[0].name}') ||
	die "cannot read Deployment $INSTANCE_NS/$INSTANCE"
[ -n "$container" ] || die "Deployment $INSTANCE_NS/$INSTANCE has no containers"

# --- 1. stop -----------------------------------------------------------------

stop_instance

# --- 2. wire -----------------------------------------------------------------

# A strategic merge: `env` merges by name, so these two keys are replaced and
# every other env key stays as deployed.
patch=$(jq -n --arg name "$container" --arg url "$ACCESS_URL" --arg ws "$ws" \
	'{spec:{template:{spec:{containers:[{name:$name,env:[
		{name:"MULTICA_SERVER_URL",value:$url},
		{name:"MULTICA_WORKSPACE_ID",value:$ws}]}]}}}}')
k -n "$INSTANCE_NS" patch deploy "$INSTANCE" --type=strategic -p "$patch" >/dev/null
info "wired $INSTANCE_NS/$INSTANCE → $ACCESS_URL (workspace $ws)"

# A merge patch, never `apply`: a three-way merge sends the keys the rendered
# manifest omits (FOREMAN_JOB_TOKEN_KEY) as deletions and takes the signing key
# with them; a merge patch replaces exactly the keys it names.
instance_object_present secret "$INSTANCE_SECRET" ||
	die "Secret $INSTANCE_NS/$INSTANCE_SECRET does not exist — deploy the instance under test first (deploy/README.md)"
jq -n --rawfile token "$work/token" '{data:{MULTICA_TOKEN:($token|@base64)}}' >"$work/patch.json"
k -n "$INSTANCE_NS" patch secret "$INSTANCE_SECRET" --type=merge --patch-file "$work/patch.json" >/dev/null
info "credential: MULTICA_TOKEN ← profile $PROFILE (minted by the test server)"

# --- 3. the environment gate, while the instance is stopped ------------------

"$TEST_MULTICA_DIR/selfcheck.sh"

# --- 4. start ----------------------------------------------------------------

k -n "$INSTANCE_NS" scale deploy "$INSTANCE" --replicas=1 >/dev/null
k -n "$INSTANCE_NS" rollout status deploy "$INSTANCE" --timeout=180s >/dev/null
info "started $INSTANCE_NS/$INSTANCE"

printf '\n'
printf 'access point: %s\n' "$ACCESS_URL"
printf 'workspace:    %s\n' "$ws"
printf 'token:        profile %s\n' "$PROFILE"
printf 'next:         ./bind-agent.sh   # binds the test agent to the runtime the instance registered\n'
