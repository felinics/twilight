FROM golang:1.25.7-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags='-s -w' -o /out/owner ./cmd/owner && \
    go build -trimpath -ldflags='-s -w' -o /out/worker ./cmd/worker && \
    go build -trimpath -ldflags='-s -w' -o /out/model-backend ./cmd/model-backend && \
    go build -trimpath -ldflags='-s -w' -o /out/tool-backend ./cmd/tool-backend

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin twilight

COPY --from=build /out/ /usr/local/bin/

USER 10001:10001
WORKDIR /var/lib/twilight
