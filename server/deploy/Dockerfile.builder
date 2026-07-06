# syntax=docker/dockerfile:1
# Dockerfile.builder — the Python mcap_catalog builder daemon: the SOLE
# catalog writer + tag-edit IPC server since the M6 catalog-migration cutover
# (docs/CATALOG_CONTRACT.md). Pairs with ./Dockerfile (the Go read-only
# server); see docker-compose.yml for how the two share a volume.
#
# Build context is the REPO ROOT (the package lives at mcap_catalog/, a
# sibling of server/, not under server/deploy/):
#   docker build -t pj-cloud-builder:dev -f server/deploy/Dockerfile.builder .
#
# PREREQUISITE: mcap_catalog/ is a git submodule of this repo — run
#   git submodule update --init mcap_catalog
# (from the repo root) before building, or the COPY below finds an empty
# directory and the build fails.
FROM python:3.12-slim

# Pins match CI (.github/workflows/ci.yml) and scripts/{smoke,ci-integration}.sh's
# venv bootstrap — keep these three in lockstep.
RUN pip install --no-cache-dir \
    boto3==1.43.40 \
    google-cloud-storage==3.12.0 \
    mcap==1.4.0 \
    watchdog==6.0.0

WORKDIR /app
# The package has no setup.py/pyproject.toml — it is run in place via
# `python3 -m mcap_catalog_builder` with the package directory as a CHILD of
# /app (this WORKDIR), i.e. /app/mcap_catalog_builder — `python3 -m` finds it
# via the cwd's implicit sys.path entry, mirroring how scripts/smoke.sh and
# run.sh invoke it (`cd mcap_catalog && python3 -m mcap_catalog_builder`, where
# mcap_catalog_builder/ is likewise a child of the cwd mcap_catalog/). Only the
# importable package is copied — not the repo's CLAUDE.md/README/tests/examples.
COPY mcap_catalog/mcap_catalog_builder/ /app/mcap_catalog_builder/

# Pre-create the shared catalog dir owned by the nonroot UID this container
# runs as (see USER below) — mirrors ./Dockerfile's identical trick: an EMPTY
# named volume first mounted at this path inherits the image directory's
# ownership, so the builder can create the DB + socket here without a root
# step or an init container.
RUN mkdir -p /var/lib/pj-cloud && chown -R 65532:65532 /var/lib/pj-cloud

# Run as the same numeric UID as the Go server image's nonroot user (65532) —
# the LOCKED hardening scheme this compose/systemd pair uses: same numeric UID
# across both services (rather than a shared group) so both can read/write
# the shared catalog volume (DB + tag socket) with no group dance. A bare-metal
# deploy instead runs both under one dedicated `pjcloud` user (see
# pj-cloud-builder.service).
USER 65532:65532

# Daemon mode: --no-watch (rescan-only; no local inotify / SQS long-poll
# thread) + --tag-socket/--db on the shared volume, as defaults. Bucket/source
# are NOT defaulted here — S3 vs GCS, bucket, and prefix are deployment-
# specific — so docker-compose.yml's `command:` overrides CMD with the full
# argument list (--source/--s3-bucket or --gcs-bucket/...). AWS/GCS
# credentials come from the environment (AWS_* / GOOGLE_APPLICATION_CREDENTIALS
# / ADC), exactly like the Go server's storage credentials.
ENTRYPOINT ["python3", "-m", "mcap_catalog_builder"]
CMD ["--no-watch", "--tag-socket", "/var/lib/pj-cloud/tag.sock", "--db", "/var/lib/pj-cloud/catalog.db"]
