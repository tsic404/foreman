# Foreman server image (deploy/30-foreman.yaml). The build stages one static
# binary per platform as foreman-linux-<arch> and buildx sets TARGETARCH for
# each platform of the manifest list, so a single build covers linux/amd64 and
# linux/arm64. CGO_ENABLED=0: the image carries no runtime libraries.
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

ARG TARGETARCH

RUN set -eux; \
    case "$TARGETARCH" in amd64 | arm64) ;; *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; esac; \
    apk add --no-cache ca-certificates; \
    addgroup -g 1000 foreman; \
    adduser -D -u 1000 -G foreman foreman

COPY foreman-linux-${TARGETARCH} /usr/local/bin/foreman

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/foreman"]
