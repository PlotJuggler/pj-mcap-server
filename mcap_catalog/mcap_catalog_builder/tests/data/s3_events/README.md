# S3 event notification payload fixtures

**SYNTHESIZED** documents (2026-08-10) following the AWS S3 event notification
structure (eventVersion 2.2/2.3), covering the three families the translator
must classify (design 2026-07-30 §3.2) plus the record-less `s3:TestEvent`:

- `create_*` → exactly one `catalog` record
- `delete_*` → exactly one `delete` record (incl. both `LifecycleExpiration`
  shapes — `:Delete` and the versioned-bucket `:DeleteMarkerCreated`)
- `ack_*`    → zero records (acked immediately)

Object keys are URL-encoded (`%3D` for `=`) exactly as AWS emits them, so the
tests also pin the translator's `unquote_plus` decoding.

**Follow-up (requires privileged staging AWS access):** replace these with
CAPTURED real payloads per the design's §9 — the capture procedure is in
`server/deploy/README.md` (drain the queue to files while the consumer is
paused; never purge the shared event queue). Keep the filename convention —
the test derives the expected translation from the `create_`/`delete_`/`ack_`
prefix.
