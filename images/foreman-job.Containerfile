# Job image (contracts §5.3 "Job 镜像", 05-modules/job-template.md §Job Manifest).
#
# Contents, by contract:
#   * the upstream `multica` CLI, used verbatim — never patched (F1/ADR-004),
#   * `omp`, the agent backend, at /usr/local/bin/omp (MULTICA_OMP_PATH),
#   * `foreman-gc`, which the foreman-gc DaemonSet runs from this same image,
#   * a shell + git + CA roots, which the Job's prepare init container and the
#     daemon's repo cache need,
#   * the C++ runtime `omp` links (Alpine's `libstdc++` package): the upstream
#     release binary is dynamically linked, so the bare musl base cannot run it.
#
# The upstream artifacts are inputs, not build logic: the build passes each
# release artifact URL and the SHA-256 from the release's own checksum manifest,
# so the image can be diffed against the upstream release (AC-13). One build
# produces the whole manifest list, so every platform's pair is passed and the
# one matching TARGETARCH is the one consumed. The CLI ships as an archive; only
# its `multica` member is installed.
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG MULTICA_CLI_URL_AMD64
ARG MULTICA_CLI_SHA256_AMD64
ARG MULTICA_CLI_URL_ARM64
ARG MULTICA_CLI_SHA256_ARM64
ARG OMP_URL_AMD64
ARG OMP_SHA256_AMD64
ARG OMP_URL_ARM64
ARG OMP_SHA256_ARM64

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) MULTICA_CLI_URL="$MULTICA_CLI_URL_AMD64"; MULTICA_CLI_SHA256="$MULTICA_CLI_SHA256_AMD64"; OMP_URL="$OMP_URL_AMD64"; OMP_SHA256="$OMP_SHA256_AMD64" ;; \
      arm64) MULTICA_CLI_URL="$MULTICA_CLI_URL_ARM64"; MULTICA_CLI_SHA256="$MULTICA_CLI_SHA256_ARM64"; OMP_URL="$OMP_URL_ARM64"; OMP_SHA256="$OMP_SHA256_ARM64" ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    test -n "$MULTICA_CLI_URL" && test -n "$MULTICA_CLI_SHA256"; \
    test -n "$OMP_URL" && test -n "$OMP_SHA256"; \
    apk add --no-cache ca-certificates git libstdc++ tzdata; \
    addgroup -g 1000 agent; \
    adduser -D -u 1000 -G agent -h /home/agent agent; \
    mkdir -p /home/agent /state/workspaces; \
    wget -q -O /tmp/multica-cli.tar.gz "$MULTICA_CLI_URL"; \
    echo "$MULTICA_CLI_SHA256  /tmp/multica-cli.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/multica-cli.tar.gz -C /tmp multica; \
    mv /tmp/multica /usr/local/bin/multica; \
    chmod 0755 /usr/local/bin/multica; \
    rm -f /tmp/multica-cli.tar.gz; \
    wget -q -O /usr/local/bin/omp "$OMP_URL"; \
    echo "$OMP_SHA256  /usr/local/bin/omp" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/omp; \
    mkdir -p /home/agent/.multica /state/workspaces; \
    chown -R 1000:1000 /home/agent /state

COPY foreman-gc-linux-${TARGETARCH} /usr/local/bin/foreman-gc

# No ENTRYPOINT: the Job pod and the DaemonSet both state the command
# explicitly, so a pod that forgets it fails instead of running something
# unintended against the node's state root.
WORKDIR /home/agent
