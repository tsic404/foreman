#!/bin/sh
# TC-tech-provenance-01 — AC-13's provenance face: the multica CLI inside the
# Job image the deployment references is the upstream release artifact, not a
# locally patched build. The judgement is byte identity: the binary's SHA-256
# equals the hash of the `multica` member extracted from the upstream release
# archive named by the CLI's own version, and that archive's hash equals the
# release's own checksum manifest.
#
#   [JOB_IMAGE=ref] [UPSTREAM_VERSION=x.y.z] ./tech-provenance-01-cluster.sh
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
FOREMAN_URL=${FOREMAN_URL:-http://127.0.0.1:8080}
JOB_IMAGE=${JOB_IMAGE:-}
UPSTREAM_VERSION=${UPSTREAM_VERSION:-}
UPSTREAM_REPO=${UPSTREAM_REPO:-multica-ai/multica}
PULL_SECRET=${PULL_SECRET:-registry-tsic}
WAIT=${WAIT:-180}
RUN=tc-provenance-01-$(date +%s)-$$

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need curl
need jq
need sha256sum
need tar

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

# The image the deployment references is the judged object. /foreman/version
# reports the effective value; the Deployment env is the fallback when the ops
# endpoint is not port-forwarded.
if [ -z "$JOB_IMAGE" ]; then
	JOB_IMAGE=$(curl -fsS -m 30 "$FOREMAN_URL/foreman/version" 2>/dev/null |
		jq -r '.job_image // empty' 2>/dev/null || true)
fi
if [ -z "$JOB_IMAGE" ]; then
	JOB_IMAGE=$(kubectl -n "$FOREMAN_NS" get deploy foreman -o json 2>/dev/null |
		jq -r '[.spec.template.spec.containers[]?.env[]? | select(.name == "FOREMAN_JOB_IMAGE") | .value][0] // empty' ||
		true)
fi
[ -n "$JOB_IMAGE" ] ||
	blocked "cannot resolve the Job image (port-forward the foreman Service or set JOB_IMAGE)"
observe "job image=$JOB_IMAGE"

tmp=$(mktemp -d)
POD=""
cleanup() {
	[ -z "$POD" ] || kubectl -n "$JOB_NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
	rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# ---- The in-image binary ----------------------------------------------------
POD="tc-provenance-01-$RUN"
pull_secret=""
if kubectl -n "$JOB_NS" get secret "$PULL_SECRET" >/dev/null 2>&1; then
	pull_secret="  imagePullSecrets:
    - { name: $PULL_SECRET }"
fi
# The escaped \$() must land verbatim in the pod — the container shell evaluates it, not this one.
cat >"$tmp/pod.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: $JOB_NS
  labels:
    app.kubernetes.io/name: tc-provenance-01
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
$pull_secret
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: cli
      image: $JOB_IMAGE
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-ec"]
      args:
        - |
          echo "uname_m=\$(uname -m)"
          echo "hash=\$(sha256sum /usr/local/bin/multica | cut -d' ' -f1)"
          /usr/local/bin/multica version --output json
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
EOF
kubectl -n "$JOB_NS" apply -f "$tmp/pod.yaml" >/dev/null ||
	blocked "cannot create the provenance pod $POD"
deadline=$(( $(date +%s) + WAIT ))
phase=""
while [ "$(date +%s)" -lt "$deadline" ]; do
	phase=$(kubectl -n "$JOB_NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)
	case $phase in
	Succeeded | Failed) break ;;
	esac
	sleep 2
done
logs=$(kubectl -n "$JOB_NS" logs "pod/$POD" 2>/dev/null) || true
[ -n "$logs" ] || blocked "the provenance pod produced no logs (phase=${phase:-unknown}, image $JOB_IMAGE)"
printf '%s\n' "$logs" | while IFS= read -r line; do observe "pod: $line"; done
[ "$phase" = "Succeeded" ] ||
	blocked "the provenance pod did not complete (phase=${phase:-unknown}): $logs"
in_image_hash=$(printf '%s\n' "$logs" | sed -n 's/^hash=\([0-9a-f]\{64\}\)$/\1/p' | head -1)
[ -n "$in_image_hash" ] || blocked "the pod printed no binary hash: $logs"
version_json=$(printf '%s\n' "$logs" | sed -n '/^{/,$p')
in_version=$(printf '%s' "$version_json" | jq -r '.version // empty' 2>/dev/null || true)
in_arch=$(printf '%s' "$version_json" | jq -r '.arch // empty' 2>/dev/null || true)
in_commit=$(printf '%s' "$version_json" | jq -r '.commit // empty' 2>/dev/null || true)
[ -n "$in_version" ] || blocked "the pod printed no parseable version JSON: $logs"
[ -n "$in_arch" ] || blocked "the pod's version JSON carries no arch: $logs"
observe "in-image: version=$in_version arch=$in_arch commit=$in_commit hash=$in_image_hash"
[ -z "$UPSTREAM_VERSION" ] || [ "$UPSTREAM_VERSION" = "$in_version" ] ||
	fail "the image carries $in_version, UPSTREAM_VERSION names $UPSTREAM_VERSION"

# ---- The upstream release artifact -----------------------------------------
base="https://github.com/$UPSTREAM_REPO/releases/download/v$in_version"
asset="multica-cli-$in_version-linux-$in_arch.tar.gz"
observe "upstream: $base/$asset"
curl -fsSL -m 120 -o "$tmp/$asset" "$base/$asset" ||
	blocked "cannot download $base/$asset — the acceptance host has no upstream release access"
curl -fsSL -m 120 -o "$tmp/checksums.txt" "$base/checksums.txt" ||
	blocked "cannot download $base/checksums.txt"
manifest_hash=$(grep -E "[[:space:]]$asset\$" "$tmp/checksums.txt" | cut -d' ' -f1 | head -1)
printf '%s' "$manifest_hash" | grep -Eq '^[0-9a-f]{64}$' ||
	blocked "the upstream checksum manifest carries no SHA-256 for $asset"
asset_hash=$(sha256sum "$tmp/$asset" | cut -d' ' -f1)
binary_hash=$(tar -xzf "$tmp/$asset" -O multica | sha256sum | cut -d' ' -f1)
observe "upstream: asset_hash=$asset_hash manifest_hash=$manifest_hash binary_hash=$binary_hash"
[ "$asset_hash" = "$manifest_hash" ] ||
	blocked "the downloaded archive does not match the release manifest ($asset_hash vs $manifest_hash)"
[ -n "$binary_hash" ] || blocked "cannot extract the multica member from $asset"

# ---- Byte identity ----------------------------------------------------------
[ "$in_image_hash" = "$binary_hash" ] ||
	fail "the image binary hash $in_image_hash differs from the upstream artifact's $binary_hash — the CLI is patched or built from another source"
echo "PASS: TC-tech-provenance-01 (image $JOB_IMAGE carries upstream $in_version/$in_arch, sha256 $in_image_hash)"
