#!/usr/bin/env bash
# Build and push both Foreman images, then report the references and digests
# this run published (deploy/README.md §Images). CI runs this; the same command
# works against any registry by hand.
#
# Required: VERSION, MULTICA_CLI_URL/_SHA256, OMP_URL/_SHA256 (upstream release
# artifacts, F1/AC-13). Optional: REGISTRY, BASE_IMAGE, CONTAINER_TOOL,
# PUSH_LATEST, OUT. A build, push or digest failure exits non-zero, so the
# CI job goes red instead of reporting a half-published release.
set -euo pipefail

case "${1:-}" in
-h | --help)
  sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
  ;;
esac

VERSION=${VERSION:-}
REGISTRY=${REGISTRY:-ghcr.io/tsic404}
BASE_IMAGE=${BASE_IMAGE:-alpine:3.20}
CONTAINER_TOOL=${CONTAINER_TOOL:-docker}
PUSH_LATEST=${PUSH_LATEST:-0}
OUT=${OUT:-dist/release-images.env}

die() {
  printf 'publish-images: %s\n' "$*" >&2
  exit 1
}
note() { printf 'publish-images: %s\n' "$*"; }

[ -n "$VERSION" ] || die "VERSION is required (e.g. VERSION=v0.1.0)"
[[ $VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "VERSION must be a semver tag (got '$VERSION')"
for name in MULTICA_CLI_URL MULTICA_CLI_SHA256 OMP_URL OMP_SHA256; do
  [ -n "${!name:-}" ] || die "$name is required (upstream artifact URL + SHA-256, AC-13 provenance)"
done
command -v "$CONTAINER_TOOL" >/dev/null || die "container tool '$CONTAINER_TOOL' not found"
command -v make >/dev/null || die "make not found"

FOREMAN_IMAGE="$REGISTRY/foreman:$VERSION"
JOB_IMAGE="$REGISTRY/foreman-job:$VERSION"
SHA7=$(git rev-parse --short=7 HEAD)
[ -n "$SHA7" ] || die "not a git checkout: cannot record the commit mapping"

note "building $FOREMAN_IMAGE and $JOB_IMAGE from $SHA7"
make build-foreman-image build-job-image \
  VERSION="$VERSION" REGISTRY="$REGISTRY" BASE_IMAGE="$BASE_IMAGE" \
  CONTAINER_TOOL="$CONTAINER_TOOL" \
  MULTICA_CLI_URL="$MULTICA_CLI_URL" MULTICA_CLI_SHA256="$MULTICA_CLI_SHA256" \
  OMP_URL="$OMP_URL" OMP_SHA256="$OMP_SHA256"

# manifest digest of a pushed reference: buildx first, `inspect` as fallback.
image_digest() {
  local ref=$1 digest
  digest=$("$CONTAINER_TOOL" buildx imagetools inspect \
    --format '{{.Manifest.Digest}}' "$ref" 2>/dev/null || true)
  if [ -z "$digest" ]; then
    digest=$("$CONTAINER_TOOL" inspect --format '{{index .RepoDigests 0}}' "$ref" 2>/dev/null || true)
    digest=${digest##*@}
  fi
  [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || die "cannot read the pushed digest of $ref (got '$digest')"
  printf '%s\n' "$digest"
}

note "pushing $FOREMAN_IMAGE"
"$CONTAINER_TOOL" push "$FOREMAN_IMAGE" >/dev/null
FOREMAN_DIGEST=$(image_digest "$FOREMAN_IMAGE")

note "pushing $JOB_IMAGE"
"$CONTAINER_TOOL" push "$JOB_IMAGE" >/dev/null
JOB_DIGEST=$(image_digest "$JOB_IMAGE")

if [ "$PUSH_LATEST" = 1 ]; then
  # `latest` follows main for both images; the version tags stay the rollback
  # path (ADR-012: never publish `latest` alone).
  for image in "$FOREMAN_IMAGE" "$JOB_IMAGE"; do
    latest="${image%:*}:latest"
    note "publishing $latest"
    "$CONTAINER_TOOL" tag "$image" "$latest"
    "$CONTAINER_TOOL" push "$latest" >/dev/null
  done
fi

mkdir -p "$(dirname "$OUT")"
cat >"$OUT" <<EOF
# Produced by scripts/publish-images.sh — the release record for this run.
GIT_SHA=$SHA7
VERSION=$VERSION
REGISTRY=$REGISTRY
FOREMAN_IMAGE=$FOREMAN_IMAGE
FOREMAN_DIGEST=$FOREMAN_DIGEST
JOB_IMAGE=$JOB_IMAGE
JOB_DIGEST=$JOB_DIGEST
EOF

note "release record written to $OUT"
printf '%s@%s\n' "$JOB_IMAGE" "$JOB_DIGEST"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    printf '## Foreman images %s (%s)\n\n' "$VERSION" "$SHA7"
    printf '| Image | Reference | Digest |\n|---|---|---|\n'
    printf '| foreman | `%s` | `%s` |\n' "$FOREMAN_IMAGE" "$FOREMAN_DIGEST"
    printf '| foreman-job | `%s` | `%s` |\n' "$JOB_IMAGE" "$JOB_DIGEST"
    printf '\nThe Job image is referenced by movable tag (`FOREMAN_JOB_IMAGE`); no deploy/ backfill.\n'
  } >>"$GITHUB_STEP_SUMMARY"
fi
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    printf 'job_digest=%s\n' "$JOB_DIGEST"
    printf 'foreman_digest=%s\n' "$FOREMAN_DIGEST"
    printf 'job_image=%s\n' "$JOB_IMAGE"
  } >>"$GITHUB_OUTPUT"
fi
