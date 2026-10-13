#!/bin/sh
# Bind the test agent to the runtime Foreman registers, and create the §3
# autopilot that hangs off it — the last step of the environment setup
# (test-cases/_infra/test-multica-cluster.md §3).
#
#   ./bind-agent.sh [--wait SECONDS]     # default: wait up to 180s
#
# Why a separate step: the reset entry leaves the agent unbound, because §1's
# self-check ③ requires an empty runtime list *before* Foreman starts, while §3
# requires the agent's runtime to be the one Foreman registers. The two are
# only both true if the binding happens after registration.
#
# The autopilot is created here for the same reason: the server refuses an
# autopilot whose assignee agent has no runtime, so it cannot be part of the
# reset entry's rebuild.
#
# Idempotent: re-running it rebinds the same agent to the current runtime and
# re-activates the autopilot.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need curl
need jq

wait_for=${TEST_MULTICA_BIND_WAIT:-180}
while [ $# -gt 0 ]; do
	case $1 in
	--wait)
		shift
		[ $# -gt 0 ] || die "--wait needs a value (seconds)"
		wait_for=$1
		;;
	-h | --help)
		sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*) die "unknown argument: $1" ;;
	esac
	shift
done
case $wait_for in
'' | *[!0-9]*) die "--wait needs a whole number of seconds (got '$wait_for')" ;;
esac

agent=$(wsapi GET /api/agents |
	jq -r --arg name "$AGENT_NAME" '[.[] | select(.name == $name and (.archived_at == null))][0].id // ""')
[ -n "$agent" ] || die "agent $AGENT_NAME does not exist — run ./reset.sh first"

deadline=$(( $(date +%s) + wait_for ))
while :; do
	runtimes=$(wsapi GET /api/runtimes)
	count=$(printf '%s' "$runtimes" | jq '[.[] | select(.provider == "omp")] | length')
	if [ "$count" -eq 1 ]; then
		break
	fi
	[ "$(date +%s)" -lt "$deadline" ] ||
		die "expected exactly 1 provider=omp runtime, found $count — is the Foreman deployment up?"
	info "waiting for the omp runtime ($count present)"
	sleep 5
done

runtime=$(printf '%s' "$runtimes" | jq -r '[.[] | select(.provider == "omp")][0].id')
name=$(printf '%s' "$runtimes" | jq -r '[.[] | select(.provider == "omp")][0].name')

wsapi PUT "/api/agents/$agent" "{\"runtime_id\":\"$runtime\"}" >/dev/null

bound=$(wsapi GET "/api/agents/$agent" | jq -r '.runtime_id // ""')
[ "$bound" = "$runtime" ] || die "binding did not take effect (agent runtime_id=$bound)"
info "bound agent $AGENT_NAME ($agent) to runtime $name ($runtime)"

# §3 可手动触发的 autopilot (`./multica-test autopilot trigger <id>`). Created
# now that the assignee has a runtime; an autopilot left over from an earlier
# round is re-pointed at the current agent and re-activated.
id=$(wsapi GET /api/autopilots |
	jq -r --arg title "$AUTOPILOT_TITLE" '[.autopilots[] | select(.title == $title)][0].id // ""')
if [ -z "$id" ]; then
	id=$(wsapi POST /api/autopilots \
		"{\"title\":\"$AUTOPILOT_TITLE\",\"description\":\"Reply with OK only.\",\"assignee_id\":\"$agent\",\"execution_mode\":\"run_only\"}" |
		jq -er .id) || die "autopilot creation failed"
	info "created autopilot $AUTOPILOT_TITLE ($id)"
else
	wsapi PATCH "/api/autopilots/$id" "{\"assignee_id\":\"$agent\",\"status\":\"active\"}" >/dev/null ||
		die "could not re-activate autopilot $id"
	info "autopilot $AUTOPILOT_TITLE ($id), active"
fi
