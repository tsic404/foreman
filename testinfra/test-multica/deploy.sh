#!/bin/sh
# Deploy (or re-deploy) the test multica cluster — the deployment entry of
# the test environment's 运维接口 (`README.md`).
#
#   ./deploy.sh                              # render + apply + wait
#   TEST_MULTICA_HOST=10.0.0.5 ./deploy.sh   # pin the entry address
#
# The model source is the deployment-supplied endpoint when its triple is set
# (README.md §Model endpoint), else the keyless local backend model-backend.yaml
# declares. Only one of the two is deployed: ./deploy.sh renders and applies the
# local backend — and waits for its rollout — exactly when no endpoint is set.
#
# It is idempotent: an existing Secret (and therefore the existing database
# password) is never overwritten, and re-applying the manifests only rolls what
# changed. The version anchor is in deploy.yaml — upgrading means rebuilding
# the environment from a new anchor (./recycle.sh && ./deploy.sh), never an
# in-place upgrade.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need sed
need jq

for arg in "$@"; do
	case $arg in
	-h | --help)
		sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*) die "unknown argument: $arg" ;;
	esac
done

# The entry origin the browser (and the web UI's login redirects) must reach:
# lib.sh resolved it (loopback by default; TEST_MULTICA_HOST or
# TEST_MULTICA_ENTRY_URL to override). It is also stamped into the backend's
# pod template, because the origin lives in the ConfigMap and a container only
# reads its environment when it starts — a moved host must roll the server
# instead of leaving it answering with the old origin.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Which model source this deployment has. The deployment-supplied endpoint
# wins: it serves the round, so the local backend is neither rendered nor
# waited for — one model source per round, never two prerequisites. A partial
# triple is an operator error and fails here, before anything is applied, with
# the missing part named (lib.sh's model_endpoint_configured).
if model_endpoint_configured; then
	model_source=endpoint
	info "model source: endpoint $(model_selector) — the local backend is not deployed"
else
	model_source=local
	info "model source: local backend $MODEL_NAME"
fi

# __ENTRY_URL__ (the host-specific origin) and __MODEL__ (the model the local
# backend serves) are the two rendered values, both embedded in YAML
# double-quoted scalars. Each is substituted as a literal, so both layers are
# escaped in turn — YAML first (`\` and `"`), then sed's replacement grammar
# (`&` re-expands the match) — or the other layer re-reads the value.
render() {
	_rendered_yaml=$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')
	printf '%s' "$_rendered_yaml" | sed 's/[\\&|]/\\&/g'
}
render_manifest() {
	sed -e "s|__ENTRY_URL__|$entry_url_escaped|g" -e "s|__MODEL__|$model_escaped|g" \
		"$TEST_MULTICA_DIR/$1" >"$work/$1"
}
entry_url_escaped=$(render "$ENTRY_URL")
model_escaped=$(render "$MODEL_NAME")
render_manifest deploy.yaml
if [ "$model_source" = local ]; then
	render_manifest model-backend.yaml
fi

# 1. Namespace + Secret. The Secret is created once and then left alone: it
#    holds the database password, and the data PVC outlives a re-deploy.
k create namespace "$NS" --dry-run=client -o yaml >"$work/namespace.yaml"
k apply -f "$work/namespace.yaml" >/dev/null

# A node port already held by another Service (a leftover from an earlier
# hand-rolled deployment, say) makes the apply fail with an opaque "provided
# port is already allocated" — name the holder instead of guessing.
services=$(k get service --all-namespaces -o json) || die "cannot list services"
for entry in "30080 test-multica-server" "30300 test-multica-web"; do
	# shellcheck disable=SC2086 # the pair is split on purpose
	set -- $entry
	holder=$(printf '%s' "$services" | jq -r --argjson port "$1" --arg ns "$NS" --arg name "$2" \
		'[.items[]
			| select(any(.spec.ports[]?; .nodePort == $port))
			| select((.metadata.namespace != $ns) or (.metadata.name != $name))][0]
		 | if . then "\(.metadata.namespace)/\(.metadata.name)" else "" end')
	[ -z "$holder" ] ||
		die "node port $1 is held by service $holder — delete it (kubectl -n ${holder%%/*} delete service ${holder##*/}) and re-run"
done

secrets=$(k get secret -n "$NS" -o name) ||
	die "cannot list secrets in $NS — is the cluster reachable?"
if printf '%s\n' "$secrets" | grep -qx 'secret/test-multica-secrets'; then
	info "secret test-multica-secrets already present — keeping it"
else
	# APP_ENV=development + a fixed verification code = deterministic test
	# login (upstream SELF_HOSTING.md). Never do this on a reachable instance.
	info "creating secret test-multica-secrets"
	{
		printf 'POSTGRES_PASSWORD=%s\n' "$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
		printf 'JWT_SECRET=%s\n' "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
		printf 'MULTICA_DEV_VERIFICATION_CODE=%s\n' "$VERIFICATION_CODE"
	} >"$work/secret.env"
	k create secret generic test-multica-secrets -n "$NS" \
		--from-env-file="$work/secret.env" --dry-run=client -o yaml >"$work/secret.yaml"
	k apply -f "$work/secret.yaml" >/dev/null
fi

# 2. Manifests. The local model backend is applied only with no endpoint
#    configured: the round has one model source, and the one that is not it
#    must not exist as a prerequisite.
k apply -f "$work/deploy.yaml" >/dev/null
if [ "$model_source" = local ]; then
	k apply -f "$work/model-backend.yaml" >/dev/null
fi

# 3. Rollout. The backend runs its migrations before serving, so it is the
#    slow one normally; on the first rollout the model backend is slower still,
#    because its init step downloads the model (hundreds of MB) before the
#    server can become ready. The wait list names the model backend through
#    lib.sh's variable, the same name ./reset.sh and ./selfcheck.sh read, so one
#    value reaches every entry — and it carries that object only in the round
#    that applied it. Waiting for an endpoint round's absent backend would
#    answer NotFound and kill the deployment entry.
deployments="postgres backend web proxy"
if [ "$model_source" = local ]; then
	deployments="$MODEL_BACKEND_DEPLOY $deployments"
fi
for d in $deployments; do
	budget=300s
	case $d in
	"$MODEL_BACKEND_DEPLOY") budget=900s ;;
	esac
	k rollout status "deploy/$d" -n "$NS" --timeout=$budget >/dev/null ||
		die "deployment $d did not become ready — kubectl -n $NS describe deploy/$d"
	info "deploy/$d ready"
done

info "done. server: $SERVER_URL/api/config · entry: $ENTRY_URL"
