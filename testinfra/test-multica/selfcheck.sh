#!/bin/sh
# The environment gate — the four checks every acceptance round opens with.
#
#   ./selfcheck.sh
#
#   ① the instance under test points at the test access point (never
#      the production address);
#   ② the runtime list is empty — the reset-time state, asserted *before* the
#      instance starts (the AC-01 premise; once it runs the assertion becomes
#      "exactly one provider=omp runtime", judged by TC-tech-protocol-01);
#   ③ the test server reports the artifact anchor v0.6.1 — two stages, because
#      the field is `omitempty`: a missing `server_version` is a data-surface
#      mismatch, not a version mismatch;
#   ④ the selected model source serves the round — the deployment-supplied
#      endpoint when its triple is set, else the local backend and the model
#      its pull step installed. The premise of a real task result and a
#      non-zero token count, not something to discover at the inference step.
#
# Exit codes: 0 = all four hold; 2 = the environment is blocked — fix it,
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
	sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
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

# --- ④ the selected model source serves the round ----------------------------

# The agent must produce a real task result and a non-zero token count, so the
# source ./reset.sh selected is part of the environment the gate asserts. The
# deployment-supplied endpoint wins; the keyless local backend is the source
# otherwise, and its pull step must have completed.
if model_endpoint_configured; then
	model_source="endpoint $(model_selector)"
	info "④ ok: the deployment supplies the model endpoint ($model_source)"
else
	model=$(kubectl -n "$NS" get deploy "$MODEL_BACKEND_DEPLOY" \
		-o jsonpath='{.spec.template.spec.initContainers[0].env[?(@.name=="OLLAMA_MODEL")].value}' 2>/dev/null) ||
		blocked "④ cannot read Deployment $NS/$MODEL_BACKEND_DEPLOY — run ./deploy.sh (the model backend is part of the test cluster)"
	[ -n "$model" ] ||
		blocked "④ $NS/$MODEL_BACKEND_DEPLOY carries no OLLAMA_MODEL — re-run ./deploy.sh"
	served=$(kubectl -n "$NS" exec deploy/"$MODEL_BACKEND_DEPLOY" -- ollama list 2>/dev/null) ||
		blocked "④ the model backend is not serving — kubectl -n $NS logs deploy/$MODEL_BACKEND_DEPLOY, then re-run ./deploy.sh"
	printf '%s\n' "$served" | awk 'NR > 1 { print $1 }' | grep -Fqx "$model" ||
		blocked "④ the model backend does not serve $model — re-run ./deploy.sh (its pull step fills the models volume)"
	model_source="local backend $model"
	info "④ ok: model backend serves $model"
fi

info "self-check passed: ①②③④ (access point $ACCESS_URL, model source $model_source)"
