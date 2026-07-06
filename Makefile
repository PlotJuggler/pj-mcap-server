.PHONY: all proto build test race lint integration bench bench-storage docker clean \
        smoke matrix ci-integration server-start server-stop

# This repo root IS the pj-cloud root (per CLAUDE.md): the Go module lives in
# server/ and the canonical wire schema in proto/.
#
# Requires Go (1.23+) and protoc (3.21+) on PATH. The Go toolchain here is
# installed at $HOME/.local/go/bin and is NOT on the default PATH; export it
# before invoking make:
#
#   export PATH=$HOME/.local/go/bin:$HOME/go/bin:$PATH
#
GO_DIR := server
PROTO_DIR := proto
PROTO_OUT := $(GO_DIR)/internal/wire/pj_cloud
PROTOC_GEN_GO ?= $(shell go env GOPATH)/bin/protoc-gen-go

all: build

proto:
	@command -v protoc >/dev/null || { echo "protoc not installed"; exit 1; }
	@test -x $(PROTOC_GEN_GO) || GOBIN=$$(go env GOPATH)/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	mkdir -p $(PROTO_OUT)
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
		-I=$(PROTO_DIR) \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		$(PROTO_DIR)/pj_cloud.proto

build: proto
	cd $(GO_DIR) && go build -o bin/pj-cloud-server ./cmd/pj-cloud-server

test:
	cd $(GO_DIR) && go test ./...

race:
	cd $(GO_DIR) && go test -race ./...

lint:
	cd $(GO_DIR) && go vet ./...
	cd $(GO_DIR) && gofmt -l . | tee /dev/stderr | (! read)

integration:
	cd $(GO_DIR) && go test -tags=integration -count=1 ./integration_test/...

bench:
	cd $(GO_DIR) && go test -tags=bench -bench=. -benchmem -count=1 ./bench/...

# bench-storage: the Plan A Task 46a storage-parity microbench (GetRange MB/s,
# S3/Minio vs GCS/fake-gcs, on one ground-truth object). Brings the fake-gcs
# emulator up + seeds it, points the GCS store at it via STORAGE_EMULATOR_HOST,
# runs the parity test (reports both numbers, asserts gcs >= 25% of s3 — a
# generous co-resident floor; the plan's ~10% is a reference-machine figure), then
# tears the emulator down. Mirrors `make bench`. See server/bench/storage_parity_test.go.
FAKE_GCS_DIR := infra/fake-gcs
FAKE_GCS_COMPOSE := $(FAKE_GCS_DIR)/docker-compose.yml
bench-storage:
	docker compose -f $(FAKE_GCS_COMPOSE) up -d --wait
	bash $(FAKE_GCS_DIR)/seed.sh
	cd $(GO_DIR) && STORAGE_EMULATOR_HOST=localhost:4443 \
		go test -tags=bench -run TestStorageParity -count=1 -v ./bench/... ; \
		rc=$$? ; \
		docker compose -f ../$(FAKE_GCS_COMPOSE) down >/dev/null 2>&1 || true ; \
		exit $$rc

docker:
	docker build -t pj-cloud-server:dev -f $(GO_DIR)/deploy/Dockerfile $(GO_DIR)

clean:
	rm -rf $(GO_DIR)/bin $(GO_DIR)/dist
	rm -f coverage.out coverage.html

# ── SDK harness ──────────────────────────────────────────────────────────────
# smoke: the single headless regression gate — rewritten for the catalog-
# migration cutover (2026-07-06). Ensures Minio is up, generates + seeds a
# FRESH synthetic Hive-keyed MCAP corpus into a dedicated bucket, starts its OWN
# Python `mcap_catalog` builder daemon (the sole catalog writer + tag-edit IPC
# server) and its OWN Go server in `-external-builder` read-only mode on :8081
# (never touching the interactive :8080 instance nor /tmp/pj-cloud-server.pid),
# runs the Go discovery assertions (cross-checked against an independent
# `mcaptopics` oracle, never hardcoded counts) and the C++ SDK tests (hermetic +
# live), a full round-trip matrix, a restart-persistence check, and a tag-edit
# flow that survives a full catalog REBUILD — then prints SMOKE PASS / SMOKE
# FAIL. Self-contained and idempotent; needs the venv at
# ~/.venvs/pj-catalog (boto3, mcap, watchdog — see scripts/smoke.sh's bootstrap
# error for the exact command if it's missing). See scripts/smoke.sh for the
# full architecture and ground-truth derivation.
smoke:
	bash scripts/smoke.sh

# matrix: the DEEPER, slower end-to-end correctness gate (spec §11 L3/L4 round-trip
# MATRIX). Self-contained like smoke but on :8082 with its own temp DB, always
# reaped on exit; never touches :8080/:8081. Runs the half-topics / none-matching /
# outside-range / spans-boundary / 8-file-stitch / 4-parallel / overlap-rejection
# legs, each mcapdiff-verified. Prints MATRIX PASS / MATRIX FAIL. See scripts/matrix.sh.
matrix:
	bash scripts/matrix.sh

# ci-integration: the LOCAL driver for the CI {s3,gcs} integration legs — the
# SAME test (`-tags=ci_integration` over internal/ws, invoking the Python
# mcap_catalog builder `--once` + catalog.OpenReadOnly — M6 §5.4's catalog-
# migration cutover) that .github/workflows/ci.yml runs in GitHub Actions
# service containers, but proven here WITHOUT GitHub. It stands up its OWN
# Minio + fake-gcs emulators on FRESH HIGH PORTS (19010 / 14450 — never
# :8080/:8081/:8082/:9000/:4443), seeds both buckets with the deterministic
# Hive-keyed synthetic MCAPs from cmd/gen-ci-fixtures -hive, runs the tagged
# test per backend, and ALWAYS reaps the containers. Prints CI-INTEGRATION
# PASS / FAIL. Requires docker + curl + PJ_CI_BUILDER_PYTHON (a python3 with
# boto3, google-cloud-storage, mcap, watchdog installed — see
# scripts/ci-integration.sh's header for the one-time venv bootstrap). Scope
# one leg with `LEG=s3 make ci-integration` (or LEG=gcs).
ci-integration:
	bash scripts/ci-integration.sh

# ── interactive server (:8080) convenience targets ──────────────────────────
# These manage the USER-FACING server instance on :8080 with its own PID/log
# files. They are independent of `smoke` (which uses :8081). server-start is a
# no-op if a live instance is already recorded and running.
SERVER_PID := /tmp/pj-cloud-server.pid
SERVER_LOG := /tmp/pj-cloud-server.log

server-start: build
	@if [ -f $(SERVER_PID) ] && kill -0 "$$(cat $(SERVER_PID))" 2>/dev/null; then \
		echo "pj-cloud-server already running (pid $$(cat $(SERVER_PID)))"; \
	else \
		echo "starting pj-cloud-server on :8080 (log $(SERVER_LOG))"; \
		( $(GO_DIR)/bin/pj-cloud-server -listen :8080 >$(SERVER_LOG) 2>&1 & echo $$! >$(SERVER_PID) ); \
		echo "started (pid $$(cat $(SERVER_PID)))"; \
	fi

server-stop:
	@if [ -f $(SERVER_PID) ] && kill -0 "$$(cat $(SERVER_PID))" 2>/dev/null; then \
		echo "stopping pj-cloud-server (pid $$(cat $(SERVER_PID)))"; \
		kill "$$(cat $(SERVER_PID))" 2>/dev/null || true; \
		rm -f $(SERVER_PID); \
	else \
		echo "pj-cloud-server not running (no live $(SERVER_PID))"; \
		rm -f $(SERVER_PID); \
	fi
