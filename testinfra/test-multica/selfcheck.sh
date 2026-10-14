#!/bin/sh
# The environment self-check of test-cases/_infra/test-multica-cluster.md §2 —
# the three checks every acceptance round opens with.
#
#   ./selfcheck.sh
#
#   ① the instance under test points at the test access point (§2 接线; never
#      the production address);
#   ② the runtime list is empty — the reset-time state, asserted *before* the
#      instance starts (the AC-01 premise; once it runs the assertion becomes
#      "exactly one provider=omp runtime", judged by TC-tech-protocol-01);
#   ③ the test server reports the artifact anchor v0.6.1 — two stages, because
#      the field is `omitempty`: a missing `server_version` is a data-surface
#      mismatch, not a version mismatch.
#
# Exit codes: 0 = all three hold; 2 = the environment is blocked — fix it,
# never run a case against an unknown one. ② only holds before the instance
# under test starts: ./reset.sh and ./wire.sh run this at that moment.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need curl
need jq
need multica

anchor=${TEST_MULTICA_VERSION_ANCHOR:-v0.6.1}

case ${1:-} in
'') ;;
-h | --help)
	sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
	exit 0
	;;
*) die "unknown argument: $1" ;;
esac

# A failed check is the design's `QA_BLOCKED`, and the exit code says so.
blocked() {
	printf 'test-multica: %s\n' "$*" >&2
	exit 2
}

# --- ① the instance under test points at the access point --------------------

url=$(instance_env MULTICA_SERVER_URL) ||
	blocked "① cannot read Deployment $INSTANCE_NS/$INSTANCE — deploy the instance under test first (deploy/README.md), then run ./wire.sh"
[ "$url" = "$ACCESS_URL" ] ||
	blocked "① MULTICA_SERVER_URL is '$url', the access point is '$ACCESS_URL' — run ./wire.sh (the production address must never be wired into the test instance)"
info "① ok: $INSTANCE_NS/$INSTANCE → $url"

# --- ② the runtime list is empty (the reset-time, pre-start state) -----------

[ -r "$PROFILE_CONFIG" ] || blocked "② no CLI profile at $PROFILE_CONFIG — run ./reset.sh first"
runtimes=$(mcli runtime list --output json) || blocked "② the runtime list is unreadable on $SERVER_URL"
# Fail-closed on purpose: a malformed answer (a warning line mixed into the
# JSON, an HTML error page, a non-array) is not an empty runtime list — and the
# design's own form, `jq -e 'length == 0'`, is the same shape.
printf '%s' "$runtimes" | jq -e 'type == "array" and length == 0' >/dev/null || {
	printf '%s\n' "$runtimes" >&2
	blocked "② the runtime list is not empty, or is not a runtime array — the instance under test must be stopped and the rows wiped before it starts"
}
info "② ok: the runtime list is empty"

# --- ③ the version anchor, in two stages -------------------------------------

config=$(curl -fsS -m 30 "$SERVER_URL/api/config") ||
	blocked "③ $SERVER_URL/api/config is unreachable"
printf '%s' "$config" | jq -e 'has("server_version")' >/dev/null ||
	blocked "③ /api/config carries no server_version field — the data surface and the artifact differ (an untagged build hides the field with omitempty); this is not a version mismatch"
version=$(printf '%s' "$config" | jq -r .server_version)
[ "$version" = "$anchor" ] ||
	blocked "③ the server reports $version, the artifact anchor is $anchor"
info "③ ok: server_version $version"

info "self-check passed: ①②③ (access point $ACCESS_URL)"
