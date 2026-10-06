#!/usr/bin/env bash
# Pin the Job image digest into the two files that must agree (AC-13: the Job
# image is digest-pinned; deploy/README.md §Images).
#
#   scripts/pin-job-image-digest.sh --digest sha256:<64 hex>
#   scripts/pin-job-image-digest.sh --from-file dist/release-images.env
#
# Refuses to touch a file whose shape changed: a YAML edit cannot silently
# desynchronise the two references.
set -euo pipefail

DEPLOY_DIR=${DEPLOY_DIR:-deploy}
FOREMAN_YAML="$DEPLOY_DIR/30-foreman.yaml"
GC_YAML="$DEPLOY_DIR/40-foreman-gc.yaml"
DIGEST=""

usage() { sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --digest) DIGEST=${2:-}; shift 2 ;;
    --from-file)
      file=${2:-}
      [ -n "$file" ] && [ -f "$file" ] || { printf 'pin-job-image-digest: mapping file %s not found\n' "${file:-}" >&2; exit 1; }
      DIGEST=$(sed -n 's/^JOB_DIGEST=//p' "$file" | tail -n1)
      shift 2
      ;;
    -h|--help) usage ;;
    *) printf 'pin-job-image-digest: unknown argument %s\n' "$1" >&2; exit 2 ;;
  esac
done

[ -n "$DIGEST" ] || { printf 'pin-job-image-digest: --digest or --from-file is required\n' >&2; usage; }
[[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  printf 'pin-job-image-digest: %s is not a sha256:<64 hex> digest\n' "$DIGEST" >&2; exit 1; }
for f in "$FOREMAN_YAML" "$GC_YAML"; do
  [ -f "$f" ] || { printf 'pin-job-image-digest: %s not found\n' "$f" >&2; exit 1; }
done

# 30-foreman.yaml: the `value:` line that follows the digest env key.
rewrite_env_value() {
  awk -v dig="$DIGEST" -v file="$1" '
    /^[[:space:]]*value:[[:space:]]*"sha256:[0-9a-f]{64}"/ && prev ~ /FOREMAN_JOB_IMAGE_DIGEST/ {
      sub(/[[:space:]]*#.*$/, "")            # the REPLACE marker is stale now
      sub(/sha256:[0-9a-f]{64}/, dig); hits++
    }
    { prev = $0; print }
    END {
      if (hits != 1) {
        printf "pin-job-image-digest: expected 1 FOREMAN_JOB_IMAGE_DIGEST value in %s, found %d\n", file, hits > "/dev/stderr"
        exit 1
      }
    }' "$1"
}

# 40-foreman-gc.yaml: the image reference itself.
rewrite_image_ref() {
  awk -v dig="$DIGEST" -v file="$1" '
    /^[[:space:]]*image:[[:space:]]*[^[:space:]]*@sha256:[0-9a-f]{64}/ {
      sub(/[[:space:]]*#.*$/, "")
      sub(/sha256:[0-9a-f]{64}/, dig); hits++
    }
    { print }
    END {
      if (hits != 1) {
        printf "pin-job-image-digest: expected 1 digest-pinned image in %s, found %d\n", file, hits > "/dev/stderr"
        exit 1
      }
    }' "$1"
}

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
rewrite_env_value "$FOREMAN_YAML" >"$tmp"
cat "$tmp" >"$FOREMAN_YAML"
rewrite_image_ref "$GC_YAML" >"$tmp"
cat "$tmp" >"$GC_YAML"

env_ref=$(awk '/^[[:space:]]*value:[[:space:]]*"sha256:/{sub(/[[:space:]]*#.*$/,""); gsub(/[",]/,""); sub(/^[[:space:]]*value:[[:space:]]*/,""); print}' "$FOREMAN_YAML")
gc_ref=$(awk '/^[[:space:]]*image:[[:space:]]*[^[:space:]]*@sha256:/{print $2}' "$GC_YAML")
gc_ref=${gc_ref#*@}

[ "$env_ref" = "$DIGEST" ] || { printf 'pin-job-image-digest: %s holds %s, expected %s\n' "$FOREMAN_YAML" "$env_ref" "$DIGEST" >&2; exit 1; }
[ "$gc_ref" = "$DIGEST" ] || { printf 'pin-job-image-digest: %s holds %s, expected %s\n' "$GC_YAML" "$gc_ref" "$DIGEST" >&2; exit 1; }

printf 'pin-job-image-digest: %s FOREMAN_JOB_IMAGE_DIGEST=%s\n' "$FOREMAN_YAML" "$DIGEST"
printf 'pin-job-image-digest: %s foreman-gc image digest=%s\n' "$GC_YAML" "$DIGEST"
