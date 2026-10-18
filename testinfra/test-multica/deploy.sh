#!/bin/sh
# Deploy (or re-deploy) the test multica cluster — the deployment entry of
# the test environment's 运维接口 (`README.md`).
#
#   ./deploy.sh                              # render + apply + wait
#   TEST_MULTICA_HOST=10.0.0.5 ./deploy.sh   # pin the entry address
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

for arg in "$@"; do
	case $arg in
	-h | --help)
		sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
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

# __ENTRY_URL__ is the only host-specific value in the manifests.
sed "s|__ENTRY_URL__|$ENTRY_URL|g" "$TEST_MULTICA_DIR/deploy.yaml" >"$work/deploy.yaml"

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

# 2. Manifests.
k apply -f "$work/deploy.yaml" >/dev/null

# 3. Rollout. The backend runs its migrations before serving, so it is the
#    slow one; the others follow it.
for d in postgres backend web proxy; do
	k rollout status "deploy/$d" -n "$NS" --timeout=300s >/dev/null ||
		die "deployment $d did not become ready — kubectl -n $NS describe deploy/$d"
	info "deploy/$d ready"
done

info "done. server: $SERVER_URL/api/config · entry: $ENTRY_URL"
