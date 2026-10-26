#!/bin/sh
# Repoint the reference template's agent-backend origin at a deployer mirror.
#
#   FOREMAN_AGENT_BACKEND_BASE=http://ompmirror.multica-agents.svc:8080 \
#     ./deploy/optional/agent-backend-mirror.sh | kubectl apply -f -
#   kubectl -n foreman rollout restart deploy/foreman
#
# The shipped ConfigMap (25-job-template.yaml) stays byte-identical to the
# AC-20 baseline: this script rewrites exactly one line — the omp init step's
# `base=` — and writes the resulting ConfigMap to stdout. The mirror must serve
# a release download directory's layout:
#
#   <base>/SHA256SUMS.txt
#   <base>/omp-linux-musl-x64
#   <base>/omp-linux-musl-arm64
#
# The init step still verifies the asset against the manifest it downloads from
# the same base. Repointing makes the deployment leave the reference-template
# window (deploy/README.md §Agent backend origin).
#
# FOREMAN_JOB_TEMPLATE overrides the source manifest (a release tarball's copy).
set -eu

origin='base=https://github.com/can1357/oh-my-pi/releases/latest/download'
template=${FOREMAN_JOB_TEMPLATE:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/25-job-template.yaml}
base=${FOREMAN_AGENT_BACKEND_BASE:-}

die() {
	printf 'agent-backend-mirror: %s\n' "$*" >&2
	exit 2
}

[ -n "$base" ] || die 'FOREMAN_AGENT_BACKEND_BASE is required (e.g. http://ompmirror.multica-agents.svc:8080)'
# The value is pasted into the init step's shell line (base=<value>), so the
# character set excludes whitespace and shell metacharacters outright instead
# of escaping them for the shell and for awk in turn.
case $base in
*[!A-Za-z0-9._~:/@%+,=-]*) die "FOREMAN_AGENT_BACKEND_BASE contains a character the init step's shell would read as syntax: $base" ;;
esac
case $base in
http://* | https://*) ;;
*) die "FOREMAN_AGENT_BACKEND_BASE must be an http:// or https:// URL: $base" ;;
esac
[ -r "$template" ] || die "cannot read the template manifest: $template"

# Fail closed on drift: a template that no longer carries the reference origin
# exactly once would otherwise render unchanged and report success. The count
# is a separate pass so a refusal writes nothing to stdout — a half-streamed
# manifest is exactly what `| kubectl apply -f -` would install.
carries=$(awk -v origin="$origin" '
	{
		at = index($0, origin)
		if (at > 0 && substr($0, at) == origin) found++
	}
	END { print found + 0 }
' "$template")
[ "$carries" = 1 ] ||
	die "$template carries the reference origin on $carries lines, want exactly one (sync the manifest with the reference template)"

awk -v origin="$origin" -v base="$base" '
	{
		at = index($0, origin)
		if (at > 0 && substr($0, at) == origin) {
			print substr($0, 1, at - 1) "base=" base
			next
		}
		print
	}
' "$template"
