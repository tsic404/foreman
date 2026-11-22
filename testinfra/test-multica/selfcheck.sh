#!/bin/sh
# The environment gate — the five checks every acceptance round opens with.
#
#   ./selfcheck.sh
#
#   ① the instance under test points at the test access point (never
#      the production address);
#   ② the runtime list is empty — the reset-time state, asserted *before* the
#      instance starts (the AC-01 premise; once it runs the assertion becomes
#      "exactly one provider=omp runtime", judged by TC-tech-protocol-01);
#   ③ the running server is one artifact: `/api/config`'s `server_version` and
#      the version the backend image's own binary reports agree, and the pod's
#      digest is printed as the round's identity. There is no fixed version to
#      compare against — the server follows upstream's `latest`;
#   ④ the selected model source serves the round — the deployment-supplied
#      endpoint when its triple is set; otherwise the selected local source:
#      the scripted backend reports its declared model id, or the ollama engine
#      lists the model its pull step installed. The premise of a real task
#      result and a non-zero token count, not something to discover at the
#      inference step;
#   ⑤ the round's CLI delivery is frozen: the Job runs a digest-pinned rsbx
#      carrier (a moving tag is not an artifact), the runtime image the
#      supervisor is copied from is declared, and the per-attempt budget for
#      the two CLIs the Job downloads from the release registry is positive.
#      There is no Job image whose own binary could answer — the carrier
#      carries no CLI — so the gate judges the delivery settings, not a tag.
#
# Exit codes: 0 = the checks hold; 2 = the environment is blocked — fix it,
# never run a case against an unknown one. ② only holds before the instance
# under test starts: ./reset.sh and ./wire.sh run this at that moment.

set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/lib.sh"

need kubectl
need curl
need jq
need multica

case ${1:-} in
'') ;;
-h | --help)
	sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
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

# --- ③ the running artifact, in two stages -----------------------------------

# The server follows upstream's `latest`, so a fixed version is not the
# identity to compare against: the gate judges the artifact that is actually
# running. Two stages, because the field is `omitempty`: a missing
# `server_version` is a data-surface mismatch, not a version mismatch. The
# digest is what a round records — the tag alone cannot name the build.
config=$(curl -fsS -m 30 "$SERVER_URL/api/config") ||
	blocked "③ $SERVER_URL/api/config is unreachable"
printf '%s' "$config" | jq -e 'has("server_version")' >/dev/null ||
	blocked "③ /api/config carries no server_version field — the data surface and the artifact differ (an untagged build hides the field with omitempty); this is not a version mismatch"
version=$(printf '%s' "$config" | jq -r '.server_version // empty')
[ -n "$version" ] ||
	blocked "③ /api/config reports an empty server_version — the data surface and the artifact differ"
server_image=$(server_image_reference) || server_image=
[ -n "$server_image" ] ||
	blocked "③ cannot read the image of Deployment $NS/backend — run ./deploy.sh (the test server is part of the test cluster)"
server_digest=$(server_image_digest) || server_digest=
[ -n "$server_digest" ] ||
	blocked "③ cannot read the running backend pod's digest — is $NS/backend up? kubectl -n $NS get pods"
image_version=$(server_image_version) || image_version=
[ -n "$image_version" ] ||
	blocked "③ cannot read the version out of the running backend image $server_image — its own binary does not answer 'multica version'"
[ "$version" = "$image_version" ] ||
	blocked "③ the server reports $version, the running image's binary reports $image_version — /api/config does not describe the artifact that is serving"
info "③ ok: server_version $version, image $server_image @ $server_digest"

# --- ④ the selected model source serves the round ----------------------------

# The agent must produce a real task result and a non-zero token count, so the
# source ./reset.sh selected is part of the environment the gate asserts. The
# deployment-supplied endpoint wins; with no triple the selected local source
# is asserted — the scripted fixture backend by default, the ollama engine when
# a round asks for it.
source=$(model_source) || blocked "④ cannot resolve the model source"
case $source in
endpoint)
	model_source="endpoint $(model_selector)"
	info "④ ok: the deployment supplies the model endpoint ($model_source)"
	;;
scripted)
	model=$(kubectl -n "$NS" get deploy "$SCRIPTED_BACKEND_DEPLOY" \
		-o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SCRIPTED_MODEL_ID")].value}' 2>/dev/null) ||
		blocked "④ cannot read Deployment $NS/$SCRIPTED_BACKEND_DEPLOY — run ./deploy.sh (the scripted backend is part of the test cluster)"
	[ -n "$model" ] ||
		blocked "④ $NS/$SCRIPTED_BACKEND_DEPLOY carries no SCRIPTED_MODEL_ID — re-run ./deploy.sh"
	served=$(kubectl -n "$NS" exec "deploy/$SCRIPTED_BACKEND_DEPLOY" -- python3 -c \
		'import json,urllib.request;print(json.load(urllib.request.urlopen("http://127.0.0.1:8080/v1/models"))["data"][0]["id"])' 2>/dev/null) ||
		blocked "④ the scripted backend is not serving — kubectl -n $NS logs deploy/$SCRIPTED_BACKEND_DEPLOY, then re-run ./deploy.sh"
	[ "$served" = "$model" ] ||
		blocked "④ the scripted backend serves '$served', the Deployment declares '$model' — re-run ./deploy.sh"
	model_source="scripted backend $model"
	info "④ ok: scripted backend serves $model"
	;;
ollama)
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
	;;
esac

# --- ⑤ the round's CLI delivery is frozen -----------------------------------

# The Job runs the external rsbx carrier and fetches both CLIs from the Foreman
# release registry through the proxy download face, so there is no Job image
# whose own binary could answer a version probe. What the gate can assert is
# the delivery state V2-C08/V2-C28 fix: the carrier is the rsbx repository at a
# verified index digest (never a moving tag or the legacy foreman-job image),
# the runtime image the supervisor is copied from is declared (the carrier
# carries no CLI), and the two downloads have their own positive per-attempt
# budget. The download budget's isolation from heartbeat/lease/result is the
# separate limiter in internal/proxy, not an env value, so it is pinned by that
# package's saturation test rather than here.
carrier=$(instance_env FOREMAN_JOB_IMAGE 2>/dev/null) || carrier=
[ -n "$carrier" ] ||
	blocked "⑤ $INSTANCE_NS/$INSTANCE carries no FOREMAN_JOB_IMAGE — deploy the instance under test first (deploy/README.md)"
printf '%s' "$carrier" | grep -Eq '^ghcr\.io/tsic404/rsbx@sha256:[0-9a-f]{64}$' ||
	blocked "⑤ FOREMAN_JOB_IMAGE is '$carrier', not the rsbx carrier at a verified index digest (ghcr.io/tsic404/rsbx@sha256:<64 hex>) — V2-C08 names the carrier repository and freezes its digest; a movable tag or the legacy foreman-job image makes the round's CLI delivery unattributable"
runtime_image=$(instance_env FOREMAN_JOB_RUNTIME_IMAGE 2>/dev/null) || runtime_image=
[ -n "$runtime_image" ] ||
	blocked "⑤ $INSTANCE_NS/$INSTANCE carries no FOREMAN_JOB_RUNTIME_IMAGE — the Job supervisor has no declared source"
cli_budget=$(instance_env FOREMAN_JOB_CLI_BUDGET_BYTES 2>/dev/null) || cli_budget=
printf '%s' "${cli_budget:-}" | grep -Eq '^[0-9]+$' ||
	blocked "⑤ FOREMAN_JOB_CLI_BUDGET_BYTES is '${cli_budget:-}', not a byte count — the two frozen CLI downloads have no per-attempt budget (V2-C11)"
[ "$cli_budget" -gt 0 ] ||
	blocked "⑤ FOREMAN_JOB_CLI_BUDGET_BYTES is $cli_budget — the proxy download budget must be positive"
info "⑤ ok: CLI delivery frozen: carrier $carrier, runtime image $runtime_image, download budget $cli_budget bytes (separate limiter, V2-C28)"

info "self-check passed: ①②③④⑤ (server $version @ $server_digest, access point $ACCESS_URL, model source $model_source, CLI carrier $carrier)"
