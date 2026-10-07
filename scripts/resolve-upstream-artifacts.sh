#!/usr/bin/env bash
# Resolve the upstream Job-image artifacts for linux/amd64 and print
# MULTICA_CLI_URL/_SHA256 + OMP_URL/_SHA256 (docs/04-architecture.md
# §部署/运行方式). Digests come from the release's own checksum manifest, never
# from a hand-copied constant (AC-13 provenance).
#
# Pinned by default to the versions this production deployment runs; bump with
# MULTICA_CLI_VERSION / OMP_VERSION (repo variables override them in CI).
set -euo pipefail

case "${1:-}" in
-h | --help)
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
  ;;
esac

MULTICA_CLI_VERSION=${MULTICA_CLI_VERSION:-0.6.1}
OMP_VERSION=${OMP_VERSION:-18.6.1}
MULTICA_CLI_ASSET="multica-cli-${MULTICA_CLI_VERSION}-linux-amd64.tar.gz"
# Must match the Job image base's libc: alpine:3.20 is musl (deploy/README.md §Images).
OMP_ASSET=${OMP_ASSET:-omp-linux-musl-x64}

die() {
  printf 'resolve-upstream-artifacts: %s\n' "$*" >&2
  exit 1
}
# digest_of <checksum-manifest-url> <asset-name>: the release's SHA-256 for one asset.
digest_of() {
  local manifest=$1 asset=$2 sha
  sha=$(curl -fsSL "$manifest" | grep -E "[[:space:]]${asset}$" | cut -d' ' -f1 | head -n1)
  [[ $sha =~ ^[0-9a-f]{64}$ ]] || die "no SHA-256 for $asset in $manifest (got '$sha')"
  printf '%s\n' "$sha"
}

MULTICA_CLI_URL="https://github.com/multica-ai/multica/releases/download/v${MULTICA_CLI_VERSION}/${MULTICA_CLI_ASSET}"
MULTICA_CLI_SHA256=$(digest_of \
  "https://github.com/multica-ai/multica/releases/download/v${MULTICA_CLI_VERSION}/checksums.txt" \
  "$MULTICA_CLI_ASSET")
OMP_URL="https://github.com/can1357/oh-my-pi/releases/download/v${OMP_VERSION}/${OMP_ASSET}"
OMP_SHA256=$(digest_of \
  "https://github.com/can1357/oh-my-pi/releases/download/v${OMP_VERSION}/SHA256SUMS.txt" \
  "$OMP_ASSET")

values=$(
  printf 'MULTICA_CLI_URL=%s\nMULTICA_CLI_SHA256=%s\nOMP_URL=%s\nOMP_SHA256=%s\n' \
    "$MULTICA_CLI_URL" "$MULTICA_CLI_SHA256" "$OMP_URL" "$OMP_SHA256"
)
printf '%s\n' "$values"
# GitHub Actions reads step outputs from $GITHUB_OUTPUT; local runs just print.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf '%s\n' "$values" >>"$GITHUB_OUTPUT"
fi
