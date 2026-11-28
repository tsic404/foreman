# Foreman image (deploy/30-foreman.yaml and deploy/40-foreman-gc.yaml). The
# build stages static binaries per platform and buildx sets TARGETARCH for each
# platform of the manifest list. Besides the server it carries the Job-side
# supervisor and foreman-gc: a Job's init container copies the supervisor into
# the rsbx carrier, and the gc DaemonSet runs the collector from this image.
# It also carries the reference Job execution adapters (02-contracts.md
# V2-C29): the init container copies them beside the supervisor, and a
# deployment that supplies its own adapters replaces this image. The Job
# carrier itself is ghcr.io/tsic404/rsbx and carries none of them.
#
# CGO_ENABLED=0: the Go binaries carry no runtime libraries. The one library
# this image installs is libstdc++, because the control plane executes the
# verified omp payload itself during bound verification (it is a C++ binary
# linked against the musl libstdc++ that Alpine ships; ADR-033 lists the same
# requirement for the Job image).
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG TARGETARCH

RUN set -eux; \
    case "$TARGETARCH" in amd64 | arm64) ;; *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; esac; \
    apk add --no-cache ca-certificates libstdc++; \
    addgroup -g 1000 foreman; \
    adduser -D -u 1000 -G foreman foreman

COPY foreman-linux-${TARGETARCH} /usr/local/bin/foreman
COPY foreman-job-linux-${TARGETARCH} /usr/local/bin/foreman-job
COPY foreman-gc-linux-${TARGETARCH} /usr/local/bin/foreman-gc
COPY foreman-job-cli-linux-${TARGETARCH} /usr/local/bin/foreman-job-cli
COPY rsbx-remote-linux-${TARGETARCH} /usr/local/bin/rsbx-remote
COPY foreman-job-register-linux-${TARGETARCH} /usr/local/bin/foreman-job-register
COPY foreman-job-runner-linux-${TARGETARCH} /usr/local/bin/foreman-job-runner
COPY foreman-job-slots-linux-${TARGETARCH} /usr/local/bin/foreman-job-slots

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/foreman"]
