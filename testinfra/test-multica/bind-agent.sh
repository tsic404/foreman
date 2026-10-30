#!/bin/sh
# Bind the test agent and the failure agent to the runtime Foreman registers,
# and create the test autopilot that hangs off the test agent — the last step
# of the environment setup (`README.md`).
#
#   ./bind-agent.sh [--wait SECONDS]     # default: wait up to 180s
#
# Why a separate step: the reset entry leaves the agents unbound, because the
# environment gate's ② requires an empty runtime list *before* the instance
# starts, while the test objects require the agents' runtime to be the one
# Foreman registers. The two are only both true if the binding happens after
# registration.
#
# The autopilot is created here for the same reason: the server refuses an
# autopilot whose assignee agent has no runtime, so it cannot be part of the
# reset entry's rebuild.
#
# Idempotent: re-running it rebinds both agents to the current runtime and
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

# agent_id <name> -> the live row's id, empty when this round has no such agent.
agent_id() {
	wsapi GET /api/agents |
		jq -r --arg name "$1" '[.[] | select(.name == $name and (.archived_at == null))][0].id // ""'
}

agent=$(agent_id "$AGENT_NAME") || die "cannot read the agent list"
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

# The binding and the fixture's instructions travel in one update: an agent
# that survived an earlier round keeps whatever text it was created with, and
# that text is what the task process reads. A stale "reply with a short
# acknowledgement, do not modify any repository" contradicts the run prompt,
# and the passthrough leg then never gets its call — the reset entry only
# writes the instructions of an agent it creates.
#
# Both rows ride the same runtime: the failure agent exists only so a task
# dispatched to it dies on the agent's missing model, and that requires the Job
# daemon the instance under test starts.
bind_one() { # <agent-name>
	_id=$(agent_id "$1") || die "cannot read the agent list"
	[ -n "$_id" ] || die "agent $1 does not exist — run ./reset.sh first"
	_body=$(jq -nc --arg rt "$runtime" --arg ins "$AGENT_INSTRUCTIONS" \
		'{runtime_id:$rt, instructions:$ins}') ||
		die "cannot render the agent update"
	wsapi PUT "/api/agents/$_id" "$_body" >/dev/null ||
		die "cannot update agent $1"
	_written=$(wsapi GET "/api/agents/$_id") || die "cannot read agent $1 back"
	_bound=$(printf '%s' "$_written" | jq -r '.runtime_id // ""')
	[ "$_bound" = "$runtime" ] ||
		die "binding agent $1 did not take effect (agent runtime_id=$_bound)"
	_text=$(printf '%s' "$_written" | jq -r '.instructions // ""')
	[ "$_text" = "$AGENT_INSTRUCTIONS" ] ||
		die "agent instructions did not take effect on $1 (read back: $_text)"
	info "bound agent $1 ($_id) to runtime $name ($runtime), instructions converged"
}

bind_one "$AGENT_NAME"
bind_one "$FAULT_AGENT_NAME"

# The 可手动触发的 autopilots (`./multica-test autopilot trigger <id>`), one
# per task shape. Both are created now that the assignee has a runtime; one
# left over from an earlier round is re-pointed at the current agent,
# re-activated and given the current prompt — the prompt is the fixture's own
# text, so a surviving autopilot must not keep an older one.
ensure_autopilot() { # <title> <description>
	_title=$1
	_desc=$2
	# The list comes back as {"autopilots":[…]}; a bare array is accepted too so
	# the lookup does not depend on which shape the server answers with.
	_id=$(wsapi GET /api/autopilots |
		jq -r --arg title "$_title" '[(if type == "object" then .autopilots else . end)[] | select(.title == $title)][0].id // ""') ||
		die "cannot read the autopilot list"
	if [ -z "$_id" ]; then
		_body=$(jq -nc --arg title "$_title" --arg desc "$_desc" --arg agent "$agent" \
			'{title:$title, description:$desc, assignee_id:$agent, execution_mode:"run_only"}') ||
			die "could not encode the autopilot body"
		_id=$(wsapi POST /api/autopilots "$_body" | jq -er .id) || die "autopilot creation failed"
		info "created autopilot $_title ($_id)"
	else
		_body=$(jq -nc --arg desc "$_desc" --arg agent "$agent" \
			'{assignee_id:$agent, status:"active", description:$desc}') ||
			die "could not encode the autopilot patch"
		wsapi PATCH "/api/autopilots/$_id" "$_body" >/dev/null ||
			die "could not re-activate autopilot $_id"
		info "autopilot $_title ($_id), active"
	fi
}

ensure_autopilot "$AUTOPILOT_TITLE" "$TASK_PROMPT"
ensure_autopilot "$LONG_AUTOPILOT_TITLE" "$TASK_PROMPT_LONG"
