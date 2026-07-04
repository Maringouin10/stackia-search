# Multi-arch build; buildx picks GOARCH from the target platform (e.g.
# linux/arm64 for a Raspberry Pi 4/5). Produces a tiny static-ish image.
FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w" -o /out/stackia ./cmd/stackia

# Runtime image: distroless keeps the footprint minimal on the Pi.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/stackia /app/stackia
COPY config.yaml /app/config.yaml
# Index / store / bloom live here; mount a volume to persist across restarts.
VOLUME ["/app/data"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/stackia", "-config", "/app/config.yaml"]
