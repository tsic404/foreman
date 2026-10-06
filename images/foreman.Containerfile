# Foreman server image (deploy/30-foreman.yaml). Built from bin/ by
# `make build-foreman-image`; the binary is static (CGO_ENABLED=0).
ARG BASE_IMAGE=alpine:3.20
FROM ${BASE_IMAGE}

RUN apk add --no-cache ca-certificates \
 && addgroup -g 1000 foreman \
 && adduser -D -u 1000 -G foreman foreman

COPY foreman /usr/local/bin/foreman

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/foreman"]
