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
# What the test agent is told to do. A task makes one Multica API call of its
# own (the CLI resolves MULTICA_SERVER_URL / MULTICA_TOKEN from the task
# environment), because the agent-api passthrough counter only moves when the
# Job's tenant calls the server — G2's "透传" leg has no other observable face.
AGENT_INSTRUCTIONS=${TEST_MULTICA_AGENT_INSTRUCTIONS:-"Run this shell command exactly once, then reply with OK only: multica issue list --output json. Do not modify any repository."}
TASK_PROMPT=${TEST_MULTICA_TASK_PROMPT:-"Run this shell command exactly once, then reply with OK only: multica issue list --output json"}
# The long shape: a task whose agent stays alive until the command ends, so
# the window it occupies is the operator's (`sleep` length), not the model's.
# It is what the deadline, cancel and in-flight-fault cases need — a task that
# settles in seconds cannot be cancelled mid-flight or outlive a budget.
LONG_AUTOPILOT_TITLE=${TEST_MULTICA_LONG_AUTOPILOT:-foreman-test-autopilot-long}
TASK_PROMPT_LONG=${TEST_MULTICA_LONG_TASK_PROMPT:-"Run this shell command and wait for it to finish, then reply with DONE only: sleep ${TEST_MULTICA_LONG_TASK_SLEEP:-900}"}
REPO_URL=${TEST_MULTICA_REPO:-https://github.com/tsic404/foreman}
# Development login code (MULTICA_DEV_VERIFICATION_CODE in the Secret).
VERIFICATION_CODE=${TEST_MULTICA_VERIFICATION_CODE:-888888}
# The keyless local model backend (ollama) the test agent runs against: a test
# task must produce a real result and non-zero token usage, which a model-less
# agent cannot. No credential is involved — the engine is the test cluster's
# own, reached only on the in-cluster network. ./deploy.sh installs it only
# when no endpoint is configured (model-backend.yaml); with an endpoint there
# is no such Deployment, which is why local_model_backend treats NotFound as
# "this round has no local source".
MODEL_BACKEND_DEPLOY=${TEST_MULTICA_MODEL_DEPLOY:-test-multica-ollama}
# The Service is resolved from the Job pods (namespace multica-agents), so the
# name is namespace-qualified; `svc` rather than `svc.cluster.local` keeps it
# resolvable on a cluster whose domain is not the default one.
MODEL_BACKEND_URL="http://$MODEL_BACKEND_DEPLOY.$NS.svc:11434"
# The model the backend serves, rendered into model-backend.yaml by ./deploy.sh.
# The model writer reads it back from the Deployment instead of reusing this
# value, so the agent's selector always names what is actually served. It is a
# full tag: an untagged name gets the implicit `latest`, which is the row the
# backend's list reports and the gate compares.
MODEL_NAME=${TEST_MULTICA_MODEL:-qwen2.5:0.5b}
case $MODEL_NAME in
*:*) ;;
*) MODEL_NAME="$MODEL_NAME:latest" ;;
esac

# --- The model endpoint (a deployment parameter) -----------------------------
# The test agent needs a model source: the deployment-supplied endpoint when
# its triple is set (base URL, model id and credential), else the keyless local
# backend ./deploy.sh installs. The triple arrives from the environment — the
# credential may instead come from a file named by
# TEST_MULTICA_MODEL_API_KEY_FILE — and nothing of it belongs in this
# repository. The registration is written where the Job pods read provider
# configuration from (`${FOREMAN_STATE_ROOT}/home/<workspace>`, 02-contracts
# §8.7) and the test agent's model points at the selector it declares.
#
# An endpoint that accepts a connection and never answers only lengthens the
# agent's life; it is a diagnostic for the long-window cases, produces no
# completion terminal, and is never a default or an acceptance substitute.
MODEL_BASE_URL=${TEST_MULTICA_MODEL_BASE_URL:-}
MODEL_ID=${TEST_MULTICA_MODEL_ID:-}
MODEL_API_KEY=${TEST_MULTICA_MODEL_API_KEY:-}
MODEL_API_KEY_FILE=${TEST_MULTICA_MODEL_API_KEY_FILE:-}
# Provider identity in the registration; the agent points at
# `<provider>/<model id>`. `api` is the omp wire dialect of the endpoint
# (openai-completions for OpenAI-compatible gateways, anthropic-messages for
# Anthropic-compatible ones).
MODEL_PROVIDER=${TEST_MULTICA_MODEL_PROVIDER:-foreman-test}
MODEL_API=${TEST_MULTICA_MODEL_API:-openai-completions}
# The node-side state root the Job home is mounted from (deploy/README.md
# §Node-local state); it must equal the instance under test's
# FOREMAN_STATE_ROOT, which is why the Deployment's value wins when readable.
STATE_ROOT=${TEST_MULTICA_STATE_ROOT:-/var/lib/foreman}
# Carrier objects for the registration: one Secret (the declarative copy) and
# one one-shot Pod per node (the file omp actually reads). Pod names are
# prefixed so a stale one is recognisable, and the Secret is applied in place
# so a second reset changes nothing.
MODEL_SECRET=${TEST_MULTICA_MODEL_SECRET:-test-multica-model-provider}
MODEL_POD_PREFIX=${TEST_MULTICA_MODEL_POD_PREFIX:-test-multica-model-provider}
# How long one carrier Pod may take (image pull included) before the entry
# fails instead of leaving a half-registered environment behind.
MODEL_WAIT=${TEST_MULTICA_MODEL_WAIT:-600}
# The shell image the carrier Pods run. Alpine's busybox has everything the
# carrier does (cp/chown/chmod/mkdir/rm), and the image is small and public —
# a slow test-network pull costs seconds, where the carriers' node-observation
# image costs a minute per node.
MODEL_IMAGE=${TEST_MULTICA_MODEL_IMAGE:-alpine:3.20}

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

# psql against the test server's own database. It carries the reset entry's
# writes as well as its reads (UPDATE / DELETE), so it is named for what it
# does. Runs inside the persistence pod; no credential leaves the cluster.
psql_exec() {
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
	# The retry's own error is the one to report: the first read's message may
	# be a transient failure the retry already got past.
	_retry_out=$(k -n "$INSTANCE_NS" get "$_kind" "$_name" -o name 2>&1 >/dev/null) ||
		die "cannot read $_kind/$_name in $INSTANCE_NS — ${_retry_out:-$_out}"
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
	# An absent spec.replicas reads as empty, and the API server's default is
	# 1 — not 0. Empty must not take the "already stopped" path.
	if [ "${replicas:-1}" -eq 0 ]; then
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

# --- The model endpoint: registration ----------------------------------------

# model_endpoint_configured: 0 when the whole triple is set, 1 when none of it
# is. A partial triple is an operator error: running without the endpoint it
# names would masquerade as the baseline, and running with a guessed value
# would silently send traffic somewhere nobody named.
model_endpoint_configured() {
	if [ -z "$MODEL_BASE_URL" ] && [ -z "$MODEL_ID" ] &&
		[ -z "$MODEL_API_KEY" ] && [ -z "$MODEL_API_KEY_FILE" ]; then
		return 1
	fi
	[ -n "$MODEL_BASE_URL" ] ||
		die "model endpoint is incomplete: TEST_MULTICA_MODEL_BASE_URL is unset"
	[ -n "$MODEL_ID" ] ||
		die "model endpoint is incomplete: TEST_MULTICA_MODEL_ID is unset"
	[ -n "$MODEL_API_KEY" ] || [ -n "$MODEL_API_KEY_FILE" ] ||
		die "model endpoint is incomplete: set TEST_MULTICA_MODEL_API_KEY or TEST_MULTICA_MODEL_API_KEY_FILE"
	return 0
}

# model_api_key: the credential from the environment, or from the file it
# names (a mounted file is the other admitted carrier). A configured but
# unreadable file is a hard error: never a missing credential that quietly
# changes the round's behaviour.
model_api_key() {
	if [ -n "$MODEL_API_KEY" ]; then
		printf '%s' "$MODEL_API_KEY"
		return 0
	fi
	[ -n "$MODEL_API_KEY_FILE" ] || return 1
	[ -r "$MODEL_API_KEY_FILE" ] ||
		die "model endpoint credential file is not readable: $MODEL_API_KEY_FILE"
	_key=$(cat "$MODEL_API_KEY_FILE")
	[ -n "$_key" ] ||
		die "model endpoint credential file is empty: $MODEL_API_KEY_FILE"
	printf '%s' "$_key"
}

# model_selector: the value the test agent's `model` field carries. omp's
# persistable selector is `provider/id`; the bare id would let its provider
# ranking pick a different backend for the same id.
model_selector() {
	printf '%s/%s' "$MODEL_PROVIDER" "$MODEL_ID"
}

# yaml_scalar <value>: a YAML scalar carrying the value verbatim. jq quotes it
# (a JSON string is a YAML double-quoted scalar), so `:`, `#`, quotes and
# control characters cannot re-shape the document.
yaml_scalar() {
	jq -Rn --arg value "$1" '$value'
}

# secret_scalar: a YAML scalar built from stdin, so a credential never reaches
# an argument list (process lists, retry diagnostics). A trailing newline is
# not part of the value.
secret_scalar() {
	jq -Rn '[inputs] | join("\n")'
}

# model_provider_document: the registration the carrier Pods materialise —
# exactly one provider with one model. `apiKey` is the literal key: the file
# itself is the mounted carrier every Job pod reads, so no environment lookup
# is involved and discovery and task runs resolve the same credential.
model_provider_document() {
	_key=$(model_api_key) || die "model endpoint credential is missing"
	printf 'providers:\n'
	printf '  %s:\n' "$(yaml_scalar "$MODEL_PROVIDER")"
	printf '    baseUrl: %s\n' "$(yaml_scalar "$MODEL_BASE_URL")"
	printf '    apiKey: %s\n' "$(printf '%s' "$_key" | secret_scalar)"
	printf '    api: %s\n' "$(yaml_scalar "$MODEL_API")"
	printf '    models:\n'
	printf '      - id: %s\n' "$(yaml_scalar "$MODEL_ID")"
	printf '        name: %s\n' "$(yaml_scalar "$(model_selector)")"
}

# state_root: the node path the Job home is mounted from. The instance under
# test's own FOREMAN_STATE_ROOT wins — that is the path its Job template
# mounts — with the product default as the fallback for a round whose
# Deployment is not deployed yet.
state_root() {
	_root=$(instance_env FOREMAN_STATE_ROOT 2>/dev/null) || _root=
	printf '%s' "${_root:-$STATE_ROOT}"
}

# schedulable_linux_nodes: every node a Job can land on, one name per line.
# The registration has to exist on all of them: no selector pins a task Job to
# the node the catalog probe landed on (node homogeneity, 02-contracts §8.7).
schedulable_linux_nodes() {
	k get nodes -l kubernetes.io/os=linux -o json |
		jq -r '.items[]
			| select(.spec.unschedulable != true)
			| select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
			| .metadata.name'
}

# model_provider_secret <document>: apply the registration as the namespace's
# Secret — one object, updated in place. It carries the credential so the
# carrier Pods copy it without the value ever reaching an argument list.
model_provider_secret() {
	_work=$(mktemp -d)
	trap 'rm -rf "$_work"' 0
	printf '%s\n' "$1" >"$_work/models.yml"
	chmod 0600 "$_work/models.yml"
	k create secret generic "$MODEL_SECRET" -n "$NS" \
		--from-file="models.yml=$_work/models.yml" --dry-run=client -o yaml >"$_work/secret.yaml" ||
		die "cannot render the model provider Secret"
	k apply -f "$_work/secret.yaml" >/dev/null ||
		die "cannot apply the model provider Secret"
	rm -rf "$_work"
	trap - 0
}

# model_provider_pod <name> <node> <script> <with-secret> <capabilities>: run
# one one-shot Pod pinned to the node, with the node's Job home mounted at
# /node-home. `with-secret` mounts the registration read-only at /provider; the
# script itself never carries the credential. `capabilities` is the JSON array
# its own script needs — one set per carrier (MODEL_WRITE_CAPS /
# MODEL_REMOVE_CAPS) — not a shared maximum.
model_provider_pod() {
	_name=$1
	_node=$2
	_script=$3
	_with_secret=$4
	_caps=$5
	_home="$(state_root)/home/$(workspace_id)"
	_work=$(mktemp -d)
	trap 'rm -rf "$_work"' 0
	jq -n --arg name "$_name" --arg ns "$NS" --arg node "$_node" \
		--arg image "$MODEL_IMAGE" --arg home "$_home" \
		--arg secretname "$MODEL_SECRET" \
		--arg script "$_script" --argjson secret "$_with_secret" \
		--argjson caps "$_caps" '
		{
			apiVersion: "v1",
			kind: "Pod",
			metadata: {
				name: $name,
				namespace: $ns,
				labels: {
					"app.kubernetes.io/name": "test-multica",
					"test-multica/role": "model-provider"
				}
			},
			spec: {
				nodeName: $node,
				restartPolicy: "Never",
				automountServiceAccountToken: false,
				securityContext: { runAsUser: 0, runAsGroup: 0 },
				tolerations: [{ operator: "Exists" }],
				containers: [{
					name: "write",
					image: $image,
					imagePullPolicy: "IfNotPresent",
					command: ["sh", "-ec"],
					args: [$script],
					securityContext: {
						runAsUser: 0,
						allowPrivilegeEscalation: false,
						readOnlyRootFilesystem: true,
						capabilities: { drop: ["ALL"], add: $caps }
					},
					resources: {
						requests: { cpu: "10m", memory: "16Mi" },
						limits: { cpu: "200m", memory: "128Mi" }
					},
					volumeMounts: (
						[{ name: "home", mountPath: "/node-home" }] +
						(if $secret then
							[{ name: "provider", mountPath: "/provider", readOnly: true }]
						else [] end)
					)
				}],
				volumes: (
					[{ name: "home", hostPath: { path: $home, type: "DirectoryOrCreate" } }] +
					(if $secret then
						[{ name: "provider", secret: { secretName: $secretname, defaultMode: 256 } }]
					else [] end)
				)
			}
		}' >"$_work/pod.yaml" ||
		die "cannot render the model provider Pod"
	k delete pod "$_name" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	k apply -f "$_work/pod.yaml" >/dev/null ||
		die "cannot apply the model provider Pod $_name"
	_deadline=$(( $(date +%s) + MODEL_WAIT ))
	while :; do
		_phase=$(kubectl -n "$NS" get pod "$_name" -o jsonpath='{.status.phase}' 2>/dev/null) ||
			_phase=
		case $_phase in
		Succeeded) break ;;
		Failed)
			kubectl -n "$NS" describe pod "$_name" 2>&1 | tail -20 >&2
			die "model provider Pod $_name on node $_node failed"
			;;
		esac
		[ "$(date +%s)" -lt "$_deadline" ] || {
			printf 'test-multica: error: model provider Pod %s on node %s is %s after %ss\n' \
				"$_name" "$_node" "${_phase:-unknown}" "$MODEL_WAIT" >&2
			die "the model endpoint was not written on node $_node"
		}
		sleep 3
	done
	k delete pod "$_name" -n "$NS" --ignore-not-found --wait=true >/dev/null ||
		info "model provider Pod $_name outlived its run — delete it when convenient"
	rm -rf "$_work"
	trap - 0
}

# The carrier scripts, each with the capability set its Pod runs with. `write`
# hands the registration to the Job's agent uid (the reference template's
# prepare does the same for the rest of the home); `remove` takes only the
# registration file, never the rest of the node state. `write` needs CHOWN for
# the handover, DAC_OVERRIDE to rewrite the existing 0600 file, and FOWNER to
# chmod an inode chown just handed to 1000 (DAC_OVERRIDE covers read/write/
# execute only). `remove` only traverses that 1000-owned 0700 directory and
# unlinks one file: DAC_OVERRIDE alone.
MODEL_WRITE_SCRIPT='set -eu
[ -f /provider/models.yml ] || { echo "the provider Secret is not mounted at /provider" >&2; exit 2; }
dir=/node-home/.omp/agent
mkdir -p "$dir"
umask 077
cp /provider/models.yml "$dir/models.yml"
chown 1000:1000 /node-home/.omp "$dir" "$dir/models.yml"
chmod 0700 /node-home/.omp
chmod 0700 "$dir"
chmod 0600 "$dir/models.yml"'
MODEL_WRITE_CAPS='["CHOWN","DAC_OVERRIDE","FOWNER"]'
MODEL_REMOVE_SCRIPT='set -eu
rm -f /node-home/.omp/agent/models.yml'
MODEL_REMOVE_CAPS='["DAC_OVERRIDE"]'

# install_model_provider: the Secret first (the declarative copy), then one
# Pod per schedulable node. A failure anywhere is fatal: a half-registered
# environment would make the round's verdict unreadable.
install_model_provider() {
	_doc=$(model_provider_document) || die "cannot build the model provider registration"
	model_provider_secret "$_doc"
	_nodes=$(schedulable_linux_nodes) ||
		die "cannot list the schedulable nodes"
	[ -n "$_nodes" ] ||
		die "no schedulable Linux node — the model endpoint cannot be registered"
	for _node in $_nodes; do
		model_provider_pod "$MODEL_POD_PREFIX-$_node" "$_node" \
			"$MODEL_WRITE_SCRIPT" true "$MODEL_WRITE_CAPS"
		info "model endpoint registered on node $_node ($(model_selector))"
	done
}

# remove_model_provider: undo a previous round's registration. Runs on every
# endpoint-less reset, with no precondition: the registration lives on the
# node-local home, which outlives the test namespace (`recycle.sh` deletes the
# namespace only), so neither the Secret nor the recreated agent row can tell
# whether a previous round left one behind. Both actions are idempotent
# (`rm -f`, `delete --ignore-not-found`); the cost is one one-shot Pod per
# schedulable node.
remove_model_provider() {
	_nodes=$(schedulable_linux_nodes) ||
		die "cannot list the schedulable nodes"
	for _node in $_nodes; do
		model_provider_pod "$MODEL_POD_PREFIX-$_node" "$_node" \
			"$MODEL_REMOVE_SCRIPT" false "$MODEL_REMOVE_CAPS"
	done
	k delete secret "$MODEL_SECRET" -n "$NS" --ignore-not-found >/dev/null ||
		die "cannot delete the model provider Secret"
	info "removed the model provider registration"
}

# set_agent_model <agent-id> <selector>: point the test agent at the
# registration (or clear it with an empty selector).
set_agent_model() {
	_body=$(jq -nc --arg model "$2" '{model: $model}') ||
		die "cannot render the agent model update"
	wsapi PUT "/api/agents/$1" "$_body" >/dev/null
}

# set_agent_env <agent-id> <base-url>: the env endpoint replaces the map
# wholesale, so every branch states the whole map — the local engine's address
# when it is the model source, nothing otherwise.
set_agent_env() {
	if [ -n "$2" ]; then
		_body=$(jq -nc --arg url "$2" '{custom_env: {OLLAMA_BASE_URL: $url}}') ||
			die "cannot render the agent env update"
	else
		_body='{"custom_env":{}}'
	fi
	wsapi PUT "/api/agents/$1/env" "$_body" >/dev/null
}

# local_model_backend: the keyless engine ./deploy.sh installed, as the
# selector the agent carries. Its own pull step names the served id; the tag
# is dropped because omp validates a tagged selector against a startup catalog
# a fresh Job pod has not discovered yet. 0 = `local_model_text` holds the
# selector, 1 = no backend is deployed — a NotFound is an answer — and a read
# that fails for any other reason dies: a starved API server must not read as
# "no backend", which would clear a working agent's model.
local_model_backend() {
	_err=$(mktemp) || die "cannot create a temporary file"
	if _id=$(kubectl -n "$NS" get deploy "$MODEL_BACKEND_DEPLOY" \
		-o jsonpath='{.spec.template.spec.initContainers[0].env[?(@.name=="OLLAMA_MODEL")].value}' 2>"$_err"); then
		rm -f "$_err"
	else
		_err_text=$(cat "$_err")
		case $_err_text in
		*NotFound*)
			rm -f "$_err"
			return 1
			;;
		esac
		# The retry's own error is the one to report: the first read's message
		# may be a transient failure the retry already got past.
		if ! _id=$(k -n "$NS" get deploy "$MODEL_BACKEND_DEPLOY" \
			-o jsonpath='{.spec.template.spec.initContainers[0].env[?(@.name=="OLLAMA_MODEL")].value}' 2>"$_err"); then
			_err_text=$(cat "$_err")
			rm -f "$_err"
			die "cannot read Deployment $NS/$MODEL_BACKEND_DEPLOY — ${_err_text:-no output}"
		fi
		rm -f "$_err"
	fi
	[ -n "$_id" ] || return 1
	local_model_text="ollama/${_id%:*}"
}

# register_model_endpoint: the one writer of the agent's model and custom_env.
# The precedence is explicit: the deployment-supplied endpoint wins; without it
# the keyless local backend ./deploy.sh installed is the source; with neither
# the model is cleared. The endpoint-less rollback is unconditional — the agent
# row is not evidence of a carrier (it is recreated every round and the
# node-local file survives the namespace). `agent_id` and `AGENT_NAME` belong
# to the calling entry; `model_text` is the value the entry reports.
register_model_endpoint() {
	model_text=未配置
	if model_endpoint_configured; then
		install_model_provider ||
			die "the model endpoint could not be registered"
		model_text=$(model_selector)
		set_agent_model "$agent_id" "$model_text" ||
			die "cannot point agent $AGENT_NAME at $model_text"
		set_agent_env "$agent_id" "" ||
			die "cannot clear agent $AGENT_NAME's model backend address"
		info "agent $AGENT_NAME → $model_text"
		return 0
	fi
	info "未配置模型端点"
	remove_model_provider ||
		die "cannot clear the previous model endpoint registration"
	if local_model_backend; then
		model_text=$local_model_text
		set_agent_model "$agent_id" "$model_text" ||
			die "cannot point agent $AGENT_NAME at $model_text"
		set_agent_env "$agent_id" "$MODEL_BACKEND_URL" ||
			die "cannot set agent $AGENT_NAME's model backend address"
		info "agent $AGENT_NAME → $model_text ($MODEL_BACKEND_URL)"
		return 0
	fi
	set_agent_model "$agent_id" "" ||
		die "cannot clear the agent model"
	set_agent_env "$agent_id" "" ||
		die "cannot clear agent $AGENT_NAME's model backend address"
}
