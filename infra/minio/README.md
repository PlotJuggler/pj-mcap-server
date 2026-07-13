# Minio — the "end" endpoint (local S3, no AWS account)

This is the storage end of the PJ Cloud Connector during development: a local,
free, S3-compatible Minio server with the `recordings` bucket pre-created.
All identifiers match Plan C's pins so the future server config / integration
harness work against it unchanged.

| | |
|---|---|
| S3 API endpoint | `http://localhost:9000` |
| Web console | `http://localhost:9001` (admin / password123) |
| Bucket | `recordings` |
| Credentials | `admin` / `password123` (dev only) |

## Run

```bash
cd infra/minio
docker compose up -d        # minio + one-shot bucket-creation init job
docker compose logs minio-init   # expect: "minio ready: bucket recordings exists"
```

Data persists in the `minio-data` named volume across restarts
(`docker compose down -v` wipes it).

## Seeding synthetic data (later)

Deterministic MCAP fixtures come from Plan C Task 2 (`gen-fixtures`) once the
integration harness lands:

```bash
cd integration-tests
go run ./cmd/gen-fixtures --out fixtures
# upload via the harness's upload helper, or by hand:
#   mc alias set local http://localhost:9000 admin password123
#   mc cp fixtures/*.mcap local/recordings/
```

The future `pj-cloud-server` points at this endpoint with:

```yaml
storage:
  s3:
    bucket: recordings
    region: us-east-1
    endpoint: http://localhost:9000
```
