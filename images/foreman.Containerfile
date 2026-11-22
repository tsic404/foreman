# Foreman image (deploy/30-foreman.yaml and deploy/40-foreman-gc.yaml). The
# build stages static binaries per platform and buildx sets TARGETARCH for each
# platform of the manifest list. Besides the server it carries the Job-side
# supervisor and foreman-gc: a Job's init container copies the supervisor into
# the rsbx carrier, and the gc DaemonSet runs the collector from this image.
# The Job carrier itself is ghcr.io/tsic404/rsbx and carries neither.
#
# CGO_ENABLED=0: the image carries no runtime libraries.
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG TARGETARCH

RUN set -eux; \
    case "$TARGETARCH" in amd64 | arm64) ;; *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; esac; \
    apk add --no-cache ca-certificates; \
    addgroup -g 1000 foreman; \
    adduser -D -u 1000 -G foreman foreman

COPY foreman-linux-${TARGETARCH} /usr/local/bin/foreman
COPY foreman-job-linux-${TARGETARCH} /usr/local/bin/foreman-job
COPY foreman-gc-linux-${TARGETARCH} /usr/local/bin/foreman-gc

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/foreman"]
