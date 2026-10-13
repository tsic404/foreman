#!/bin/sh
# Recycle the whole test environment — the recycle entry of
# test-cases/_infra/test-multica-cluster.md §7.
#
#   ./recycle.sh
#
# Deletes the test multica namespace, which takes the test server, its
# database and its frontend entry with it — and the fault-injection access path
# too when it is deployed next to them (§2). Nothing outside the namespace is
# touched: the `foreman` and `multica-agents` namespaces (the instance under
# test) and any production multica deployment are left alone.
#
# The CLI profile is kept: its token is dead once the namespace is gone, and
# ./reset.sh replaces it when the environment comes back.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl

for arg in "$@"; do
	case $arg in
	-h | --help)
		sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*) die "unknown argument: $arg" ;;
	esac
done

if ! kubectl get namespace "$NS" >/dev/null 2>&1; then
	info "namespace $NS does not exist — nothing to recycle"
	exit 0
fi

kubectl delete namespace "$NS" --wait=true --timeout=300s >/dev/null
info "recycled namespace $NS (test multica server, database, frontend entry)"
