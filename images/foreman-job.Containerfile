# Job image (contracts §5.3 "Job 镜像", 05-modules/job-template.md §Job Manifest).
#
# Contents, by contract:
#   * the upstream `multica` CLI, used verbatim — never patched (F1/ADR-004),
#   * `omp`, the agent backend, at /usr/local/bin/omp (MULTICA_OMP_PATH),
#   * `foreman-gc`, which the foreman-gc DaemonSet runs from this same image,
#   * a shell + git + CA roots, which the Job's prepare init container and the
#     daemon's repo cache need.
#
# The upstream artifacts are inputs, not build logic: CI (or `make
# build-job-image`) passes the release URL and its SHA-256, so the resulting
# image can be diffed against the upstream release binary (AC-13).
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG MULTICA_CLI_URL
ARG MULTICA_CLI_SHA256
ARG OMP_URL
ARG OMP_SHA256

RUN set -eux; \
    test -n "${MULTICA_CLI_URL}" && test -n "${MULTICA_CLI_SHA256}"; \
    test -n "${OMP_URL}" && test -n "${OMP_SHA256}"; \
    apk add --no-cache ca-certificates git tzdata; \
    addgroup -g 1000 agent; \
    adduser -D -u 1000 -G agent -h /home/agent agent; \
    mkdir -p /home/agent /state/workspaces

# Upstream artifacts, verified by digest before they become part of the image.
RUN set -eux; \
    wget -q -O /usr/local/bin/multica "${MULTICA_CLI_URL}"; \
    echo "${MULTICA_CLI_SHA256}  /usr/local/bin/multica" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/multica; \
    wget -q -O /usr/local/bin/omp "${OMP_URL}"; \
    echo "${OMP_SHA256}  /usr/local/bin/omp" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/omp; \
    mkdir -p /home/agent/.multica /state/workspaces; \
    chown -R 1000:1000 /home/agent /state

# foreman-gc rides the same image (the DaemonSet mounts ${FOREMAN_STATE_ROOT}
# at /state and runs it against the node's cache).
COPY foreman-gc /usr/local/bin/foreman-gc

# No ENTRYPOINT: the Job pod and the DaemonSet both state the command
# explicitly, so a pod that forgets it fails instead of running something
# unintended against the node's state root.
WORKDIR /home/agent
