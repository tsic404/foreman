#!/usr/bin/env bash
# Check the delivery chain end to end: registry API + TLS, both images reachable
# by the references deploy/ pins, and — with --cluster — the pods actually
# running on them. --static checks deploy/ only, without touching the network.
#
#   scripts/verify-delivery.sh [--version v0.1.0] [--ca <pem>] [--cluster|--static]
#
# REGISTRY_HOST/PATH, DEPLOY_DIR, DEPLOY_NAMESPACE, JOB_NAMESPACE and TIMEOUT
# override the defaults. Every failure prints `ALERT:` and exits non-zero.
set -euo pipefail

REGISTRY_HOST=${REGISTRY_HOST:-registry.tsic.top}
REGISTRY_PATH=${REGISTRY_PATH:-multica}
VERSION=${VERSION:-}
CA_FILE=${CA_FILE:-}
DEPLOY_DIR=${DEPLOY_DIR:-deploy}
DEPLOY_NAMESPACE=${DEPLOY_NAMESPACE:-foreman}
JOB_NAMESPACE=${JOB_NAMESPACE:-multica-agents}
TIMEOUT=${TIMEOUT:-180}
STATIC=0
CLUSTER=${CLUSTER:-0}

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER=1; shift ;;
    --static) STATIC=1; shift ;;
    --version) VERSION=${2:-}; shift 2 ;;
    --ca) CA_FILE=${2:-}; shift 2 ;;
    -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) printf 'verify-delivery: unknown argument %s\n' "$1" >&2; exit 2 ;;
  esac
done

fail() { printf 'ALERT: %s\n' "$*" >&2; exit 1; }
ok() { printf 'ok    %s\n' "$*"; }

CURL=(curl -sS --max-time 20 --retry 3 --retry-connrefused --retry-delay 2)
if [ -n "$CA_FILE" ]; then
  [ -f "$CA_FILE" ] || fail "CA file $CA_FILE not found"
  CURL+=(--cacert "$CA_FILE")
fi
MANIFEST_ACCEPT='application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json'

# 1. Registry API reachable over a TLS connection we actually trust.
headers=$(mktemp)
pods_json=$(mktemp)
trap 'rm -f "$headers" "$pods_json"' EXIT
if [ "$STATIC" = "0" ]; then
  code=$("${CURL[@]}" -D "$headers" -o /dev/null -w '%{http_code}' "https://$REGISTRY_HOST/v2/" 2>&1) \
    || fail "GET https://$REGISTRY_HOST/v2/ failed (TLS trust or connectivity): $code"
  [ "$code" = "200" ] || fail "GET https://$REGISTRY_HOST/v2/ returned HTTP $code (expected 200)"
  grep -qi '^docker-distribution-api-version:[[:space:]]*registry/2\.0' "$headers" \
    || fail "https://$REGISTRY_HOST/v2/ is not a Distribution API endpoint (no Docker-Distribution-API-Version: registry/2.0)"
  ok "registry API https://$REGISTRY_HOST/v2/ → 200, Distribution API v2"

  # 2. The foreman tag deploy/ references exists.
  if [ -n "$VERSION" ]; then
    tags=$("${CURL[@]}" "https://$REGISTRY_HOST/v2/$REGISTRY_PATH/foreman/tags/list") \
      || fail "cannot list tags for $REGISTRY_PATH/foreman"
    case "$tags" in
      *"\"$VERSION\""*) ok "foreman tag published: $VERSION" ;;
      *) fail "tag $VERSION missing from $REGISTRY_PATH/foreman (registry reports: $tags)" ;;
    esac
  fi
else
  ok "static mode: registry API and tag checks skipped"
fi

# 3. The two digest-pinned references in deploy/ agree and are served.
foreman_yaml="$DEPLOY_DIR/30-foreman.yaml"
gc_yaml="$DEPLOY_DIR/40-foreman-gc.yaml"
[ -f "$foreman_yaml" ] || fail "$foreman_yaml not found"
[ -f "$gc_yaml" ] || fail "$gc_yaml not found"
env_digest=$(awk '/^[[:space:]]*value:[[:space:]]*"sha256:/{sub(/[[:space:]]*#.*$/,""); gsub(/[",]/,""); sub(/^[[:space:]]*value:[[:space:]]*/,""); print}' "$foreman_yaml" | tail -n1)
gc_image=$(awk '/^[[:space:]]*image:[[:space:]]*[^[:space:]]*@sha256:/{print $2}' "$gc_yaml" | tail -n1)
gc_digest=${gc_image##*@}
[[ "$env_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "$foreman_yaml FOREMAN_JOB_IMAGE_DIGEST is not a real digest: '$env_digest'"
zeros="sha256:"; for _ in $(seq 1 64); do zeros="${zeros}0"; done
[ "$env_digest" != "$zeros" ] \
  || fail "$foreman_yaml still carries the placeholder digest — pin the released Job digest first (scripts/pin-job-image-digest.sh --digest sha256:<released>)"
[ "$env_digest" = "$gc_digest" ] \
  || fail "Job image digests disagree: 30-foreman.yaml=$env_digest vs 40-foreman-gc.yaml=$gc_digest"
ok "deploy digests agree: $env_digest"

job_repo=${gc_image%@*}
job_repo=${job_repo#"$REGISTRY_HOST"/}
if [ "$STATIC" = "0" ]; then
  code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -H "Accept: $MANIFEST_ACCEPT" \
    "https://$REGISTRY_HOST/v2/$job_repo/manifests/$gc_digest") \
    || fail "manifest request for $REGISTRY_HOST/$job_repo@$gc_digest failed"
  [ "$code" = "200" ] || fail "job image $REGISTRY_HOST/$job_repo@$gc_digest returned HTTP $code (digest not published?)"
  ok "job image digest served: $job_repo@$gc_digest"
fi

# 4. Deployed pods: rollout completes and no container is stuck pulling.
if [ "$CLUSTER" = "1" ]; then
  command -v kubectl >/dev/null || fail "--cluster needs kubectl"
  kubectl -n "$DEPLOY_NAMESPACE" rollout status deployment/foreman --timeout="${TIMEOUT}s" \
    || fail "foreman Deployment in $DEPLOY_NAMESPACE did not roll out within ${TIMEOUT}s"
  kubectl -n "$JOB_NAMESPACE" rollout status daemonset/foreman-gc --timeout="${TIMEOUT}s" \
    || fail "foreman-gc DaemonSet in $JOB_NAMESPACE did not roll out within ${TIMEOUT}s"
  ok "rollouts complete: foreman/$DEPLOY_NAMESPACE, foreman-gc/$JOB_NAMESPACE"

  {
    kubectl -n "$DEPLOY_NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.name}: {range .status.containerStatuses[*]}{.name}={.state.waiting.reason} {end}{"\n"}{end}'
    kubectl -n "$JOB_NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.name}: {range .status.containerStatuses[*]}{.name}={.state.waiting.reason} {end}{"\n"}{end}'
  } >"$pods_json" 2>/dev/null || true
  bad=$(grep -E '=(ErrImagePull|ImagePullBackOff|InvalidImageName|CreateContainerConfigError)' "$pods_json" || true)
  [ -z "$bad" ] || fail "pods cannot run their image:
$bad"
  ok "no image-pull failures in $DEPLOY_NAMESPACE / $JOB_NAMESPACE"
fi

printf 'verify-delivery: delivery chain verified\n'
