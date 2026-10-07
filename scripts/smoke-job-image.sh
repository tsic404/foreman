#!/usr/bin/env bash
# Pull the Job image and verify what the deploy surface depends on
# (deploy/README.md §Images): the digest a tag resolves to, the upstream
# `multica` binary digest (AC-13), and that the binaries the Job spec and the
# foreman-gc DaemonSet exec are present and runnable. Needs a container CLI
# with a reachable daemon (CI runner or a local nix shell) — no nix dependency.
#
# Usage: smoke-job-image.sh <image-ref> [--expect-digest sha256:…] [--expect-cli-sha256 <hex64>]
set -euo pipefail

die() {
  printf 'smoke-job-image: %s\n' "$*" >&2
  exit 1
}
note() { printf 'smoke-job-image: %s\n' "$*"; }

case "${1:-}" in
-h | --help)
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
  ;;
esac

IMAGE=${1:-}
[ -n "$IMAGE" ] || die "usage: smoke-job-image.sh <image-ref> [--expect-digest sha256:…] [--expect-cli-sha256 <hex64>]"
shift

EXPECT_DIGEST=
EXPECT_CLI_SHA256=
while [ $# -gt 0 ]; do
  case "$1" in
  --expect-digest)
    EXPECT_DIGEST=${2:-}
    shift 2
    ;;
  --expect-cli-sha256)
    EXPECT_CLI_SHA256=${2:-}
    shift 2
    ;;
  *) die "unknown argument $1" ;;
  esac
done

CONTAINER_TOOL=${CONTAINER_TOOL:-docker}
command -v "$CONTAINER_TOOL" >/dev/null || die "container tool '$CONTAINER_TOOL' not found"

note "pulling $IMAGE"
"$CONTAINER_TOOL" pull "$IMAGE" >/dev/null

if [ -n "$EXPECT_DIGEST" ]; then
  # Compares the digest a tag resolves to with one an independent source
  # published (CI's `<sha7>:<digest>` mapping). A digest-pinned reference would
  # only compare the input with itself, so require the tag form.
  case "$IMAGE" in
  *@sha256:*)
    die "--expect-digest needs a tag reference; pass '$IMAGE' without @sha256:… to check what the tag resolves to"
    ;;
  esac
  pulled=$("$CONTAINER_TOOL" inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || true)
  pulled=${pulled##*@}
  [ -n "$pulled" ] || die "cannot read the digest $IMAGE resolved to"
  [ "$pulled" = "$EXPECT_DIGEST" ] || die "$IMAGE resolved to $pulled, want $EXPECT_DIGEST"
  note "digest verified: $pulled"
fi

cli_sha=$("$CONTAINER_TOOL" run --rm --entrypoint sha256sum "$IMAGE" /usr/local/bin/multica | cut -d' ' -f1)
note "multica binary sha256: $cli_sha"
if [ -n "$EXPECT_CLI_SHA256" ]; then
  [ "$cli_sha" = "$EXPECT_CLI_SHA256" ] || die "multica in the image is $cli_sha, want the upstream release artifact $EXPECT_CLI_SHA256"
  note "upstream binary verified (AC-13)"
fi

# The Job pods and the foreman-gc DaemonSet exec these; a missing or
# non-executable path fails at pod start, not at build time.
"$CONTAINER_TOOL" run --rm --entrypoint sh "$IMAGE" -ec \
  'test -x /usr/local/bin/omp && test -x /usr/local/bin/foreman-gc' ||
  die "the image is missing /usr/local/bin/omp or /usr/local/bin/foreman-gc"
note "omp + foreman-gc present and executable"

# `test -x` also passes for a binary built against another libc, or one whose
# shared libraries the image lacks — both die at exec, so run it.
omp_version=$("$CONTAINER_TOOL" run --rm --entrypoint omp "$IMAGE" --version) ||
  die "omp is present but does not run here (wrong libc or missing shared libraries? check OMP_ASSET and the base image)"
note "omp runs: $omp_version"
