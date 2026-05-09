FROM cgr.dev/chainguard/go:latest AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w" -o /out/sirocco ./cmd/sirocco

RUN mkdir -p /out/state && touch /out/state/.keep

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=nonroot:nonroot /out/state /var/lib/sirocco
COPY --from=build /out/sirocco /sirocco

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/sirocco"]