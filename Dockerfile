# Multi-stage build for Distributed Headless RPA Fleet Runner & Control Plane
FROM golang:1.24-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/controlplane ./cmd/controlplane
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/worker ./cmd/worker

# Runtime environment with minimal Chromium dependencies and fonts
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    ca-certificates \
    curl \
    dumb-init \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/bin/controlplane /usr/local/bin/controlplane
COPY --from=builder /app/bin/worker /usr/local/bin/worker

ENV CHROME_PATH=/usr/bin/chromium
ENV STORAGE_DIR=/data/artifacts
RUN mkdir -p /data/artifacts

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/controlplane"]
