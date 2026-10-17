# Job image (contracts §5.3 "Job 镜像", 05-modules/job-template.md §Job Manifest).
#
# Contents, by contract:
#   * the upstream `multica` CLI, used verbatim — never patched (F1/ADR-004),
#   * `foreman-gc`, which the foreman-gc DaemonSet runs from this same image,
#   * a shell + git + CA roots + the busybox applets the reference template's
#     init step needs (sh/cp/chmod/chown for `prepare`, `wget`/`uname`/
#     `sha256sum` for the `omp` install the template performs),
#   * the C++ runtime `omp` links (Alpine's `libstdc++` package, which brings
#     `libgcc`): the agent binary is installed by the template at start-up, not
#     built into this image, but it is dynamically linked against musl +
#     libstdc++ and executes inside this image — removing the package makes
#     every Job fail at agent start (ADR-031),
#   * `dumb-init`, the container PID 1 the Job and probe Job `command`s start
#     with, so orphans are reaped and SIGTERM is not swallowed (ADR-022).
#
# `omp` and `nix` are deliberately absent (ADR-031): the agent binary comes
# from the consuming Job template's init step, and the task-side toolchain is
# the template's business — the image carries no agent backend of its own.
#
# The upstream CLI artifact is an input, not build logic: the build passes its
# release artifact URL and the SHA-256 from that release's own checksum
# manifest, so the image can be diffed against the upstream release (AC-13).
# One build produces the whole manifest list, so every platform's pair is
# passed and the one matching TARGETARCH is the one consumed. The CLI ships as
# an archive; only its `multica` member is installed.
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG MULTICA_CLI_URL_AMD64
ARG MULTICA_CLI_SHA256_AMD64
ARG MULTICA_CLI_URL_ARM64
ARG MULTICA_CLI_SHA256_ARM64

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) MULTICA_CLI_URL="$MULTICA_CLI_URL_AMD64"; MULTICA_CLI_SHA256="$MULTICA_CLI_SHA256_AMD64" ;; \
      arm64) MULTICA_CLI_URL="$MULTICA_CLI_URL_ARM64"; MULTICA_CLI_SHA256="$MULTICA_CLI_SHA256_ARM64" ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    test -n "$MULTICA_CLI_URL" && test -n "$MULTICA_CLI_SHA256"; \
    apk add --no-cache ca-certificates dumb-init git libstdc++ tzdata; \
    addgroup -g 1000 agent; \
    adduser -D -u 1000 -G agent -h /home/agent agent; \
    mkdir -p /home/agent /state/workspaces; \
    wget -q -O /tmp/multica-cli.tar.gz "$MULTICA_CLI_URL"; \
    echo "$MULTICA_CLI_SHA256  /tmp/multica-cli.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/multica-cli.tar.gz -C /tmp multica; \
    mv /tmp/multica /usr/local/bin/multica; \
    chmod 0755 /usr/local/bin/multica; \
    rm -f /tmp/multica-cli.tar.gz; \
    mkdir -p /home/agent/.multica /state/workspaces; \
    chown -R 1000:1000 /home/agent /state

COPY foreman-gc-linux-${TARGETARCH} /usr/local/bin/foreman-gc

# No ENTRYPOINT and an explicitly empty CMD: the Job pod and the DaemonSet both
# state the command explicitly, so a pod that forgets it fails at start instead
# of running the base image's inherited `Cmd=["/bin/sh"]` (contracts §5.3).
CMD []

WORKDIR /home/agent
