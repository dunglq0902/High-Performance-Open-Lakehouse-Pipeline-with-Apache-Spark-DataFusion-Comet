# syntax=docker/dockerfile:1.7

# The last public MinIO image predates CVE-2025-62506's fix. Compile the fixed upstream tag into
# a minimal scratch runtime so no vulnerable server binary remains in a lower image layer.
FROM golang:1.24.8-bookworm@sha256:4ed690d6649d63c312b99a6120025ec79ce3b542968a37da53d6236c7c61a848 AS builder

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG MINIO_SOURCE_SHA256=be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c

WORKDIR /src
COPY infrastructure/docker/minio/healthcheck.go /tmp/healthcheck.go
RUN set -eux; \
    mkdir -p /out /runtime/data /runtime/tmp /runtime/usr/bin /runtime/etc/ssl/certs; \
    wget -q -O /tmp/minio.tar.gz \
      "https://codeload.github.com/minio/minio/tar.gz/refs/tags/${MINIO_VERSION}"; \
    echo "${MINIO_SOURCE_SHA256}  /tmp/minio.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/minio.tar.gz --strip-components=1; \
    rm /tmp/minio.tar.gz; \
    CGO_ENABLED=0 go build -tags kqueue -trimpath \
      -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z \
        -X github.com/minio/minio/cmd.CopyrightYear=2025 \
        -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_VERSION} \
        -X github.com/minio/minio/cmd.CommitID=${MINIO_COMMIT} \
        -X github.com/minio/minio/cmd.ShortCommitID=9e49d5e7a648" \
      -o /out/minio .

RUN set -eux; \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/healthcheck /tmp/healthcheck.go; \
    cp /out/minio /runtime/usr/bin/minio; \
    cp /out/healthcheck /runtime/usr/bin/healthcheck; \
    cp /etc/ssl/certs/ca-certificates.crt /runtime/etc/ssl/certs/ca-certificates.crt; \
    chown -R 1000:1000 /runtime

FROM scratch

COPY --from=builder --chown=1000:1000 /runtime/ /

LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.version="RELEASE.2025-10-15T17-29-55Z" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

ENV HOME=/tmp
USER 1000:1000
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
