#!/usr/bin/env bash
# Keep the Job image digest pinning in deploy/ honest (AC-13, deploy/README.md
# §Images). The digest is referenced twice — FOREMAN_JOB_IMAGE_DIGEST in
# 30-foreman.yaml (rendered into every Job) and the foreman-gc DaemonSet image
# in 40-foreman-gc.yaml — and the two must always agree: a divergence runs the
# collector against a different image than the Jobs.
#
# Usage:
#   deploy-digest.sh [--check [--expect sha256:…]]
#   deploy-digest.sh --pin sha256:…
set -euo pipefail

die() {
  printf 'deploy-digest: %s\n' "$*" >&2
  exit 1
}
note() { printf 'deploy-digest: %s\n' "$*"; }

MODE=check
EXPECT=
DIGEST=
DEPLOY_DIR=${DEPLOY_DIR:-deploy}
while [ $# -gt 0 ]; do
  case "$1" in
  --check) MODE=check ;;
  --expect)
    EXPECT=${2:-}
    shift
    ;;
  --pin)
    MODE=pin
    DIGEST=${2:-}
    shift
    ;;
  -h | --help)
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *) die "unknown argument $1" ;;
  esac
  shift
done

FOREMAN_YAML="$DEPLOY_DIR/30-foreman.yaml"
GC_YAML="$DEPLOY_DIR/40-foreman-gc.yaml"
DIGEST_RE='^sha256:[0-9a-f]{64}$'
HEX_GREP='sha256:[0-9a-f]{64}'
HEX_SED='sha256:[0-9a-f]\{64\}'

# Exactly one digest reference per file, so a rewrite cannot hit the wrong
# field and a second reference cannot drift silently.
digest_of() {
  local file=$1 hits
  hits=$(grep -oE "$HEX_GREP" "$file" | sort -u | tr '\n' ' ')
  [ "$(printf '%s' "$hits" | wc -w)" = 1 ] || die "$file: want exactly one sha256 digest, found: ${hits:-none}"
  printf '%s\n' "$hits" | tr -d ' '
}

if [ "$MODE" = pin ]; then
  [[ $DIGEST =~ $DIGEST_RE ]] || die "--pin needs sha256:<64 hex> (got '${DIGEST:-none}')"
  for file in "$FOREMAN_YAML" "$GC_YAML"; do
    digest_of "$file" >/dev/null
    sed -i "s|$HEX_SED|$DIGEST|" "$file"
    [ "$(digest_of "$file")" = "$DIGEST" ] || die "$file: the digest rewrite did not take"
  done
  note "pinned $DIGEST in $FOREMAN_YAML and $GC_YAML"
fi

foreman_digest=$(digest_of "$FOREMAN_YAML")
gc_digest=$(digest_of "$GC_YAML")
[ "$foreman_digest" = "$gc_digest" ] ||
  die "digest mismatch: $FOREMAN_YAML has $foreman_digest, $GC_YAML has $gc_digest"
note "digest references agree: $foreman_digest"

# The Job image the Deployment hands to the job builder must be the GHCR one: a
# leftover private-registry reference is exactly the ErrImagePull this delivery
# removes.
grep -qE '^[[:space:]]+value:[[:space:]]+"?ghcr\.io/tsic404/foreman-job' "$FOREMAN_YAML" ||
  die "$FOREMAN_YAML: FOREMAN_JOB_IMAGE is not a ghcr.io/tsic404/foreman-job reference"

if [ -n "$EXPECT" ]; then
  [ "$foreman_digest" = "$EXPECT" ] ||
    die "deploy/ pins $foreman_digest, the published image is $EXPECT (run: make pin-job-image-digest DIGEST=$EXPECT)"
fi
