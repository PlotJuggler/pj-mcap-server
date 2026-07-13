# syntax=docker/dockerfile:1
# Distroless static build of pj-cloud-server (Plan A Task 36). Pure-Go SQLite
# (modernc) means CGO_ENABLED=0 — a single static binary on a nonroot base.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/pj-cloud-server ./cmd/pj-cloud-server
# Pre-create the catalog data dir owned by the nonroot uid (65532) so a fresh
# named volume mounted at /var/lib/pj-cloud inherits a writable owner — distroless
# has no shell to mkdir/chown at runtime, and the server writes its SQLite WAL here.
RUN mkdir -p /out/var/lib/pj-cloud && chown -R 65532:65532 /out/var/lib/pj-cloud

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pj-cloud-server /pj-cloud-server
COPY --from=build --chown=65532:65532 /out/var/lib/pj-cloud /var/lib/pj-cloud
USER nonroot:nonroot
# 8443 = TLS (wss:// + https dashboard); expose 8080 too for plaintext dev.
EXPOSE 8443 8080
ENTRYPOINT ["/pj-cloud-server"]
CMD ["--config", "/etc/pj-cloud/config.yaml"]
