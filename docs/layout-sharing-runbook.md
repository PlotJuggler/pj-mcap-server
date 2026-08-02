# Sharing Layouts That Re-Download Their Data — Runbook

**Status: LIVE (2026-08-01).** Operator/user-facing guide for the canonical
layout-import feature: what a shared `.pj4.xml` carries, what each machine must
do once before an import works, how the local cache behaves, how to drive the
whole thing headlessly, and how to read the result.

- The **as-built engineering reference** is `docs/layout-import-architecture.md`
  (component map, runtime flows, invariants I-1…I-16).
- The **design record** (rationale, security model, rejected alternatives) is
  `docs/canonical-layout-import.md`.

This document is the *sharing policy + operations* half. Read §2 before you send
a layout to anyone.

---

## 1. What a shared layout embeds

A layout saved from a cloud-fetched dataset (see §5) records that dataset twice:
once the ordinary way, once as a durable *source descriptor*.

```xml
<fileInfo filename="/home/alice/.cache/mcap_cloud/sessions/ab12….mcap">
  <dataset source_index="0" .../>
  <plugin ID="MCAP Loader" manifest_id="mcap-loader" filepath_mode="source">
    <![CDATA[ { "filepath": "...", "use_log_time": true, ... } ]]></plugin>
  <materialize provider="mcap-cloud"
               identity="mcap-cloud:v1:sha256/128:ab12…">
    <![CDATA[ {"v":1,"kind":"mcap-cloud-session","server_uri":"wss://mcap.example.com",
               "s3_keys":["cust/site/robot/2026-07-12/a.mcap"],"topics":[],
               "start_ns":"0","end_ns":"0","include_latched":true,"display_name":"Run 42"} ]]>
  </materialize>
</fileInfo>
```

| Part | What it is |
|---|---|
| `<fileInfo filename>` | The **request-addressed cache path** on the authoring machine (§4). On another machine this file will not exist — that is expected and handled. |
| `<plugin …>` | The ordinary loader preset. Both identities are written: `ID` (the loader's display name, for older readers) and `manifest_id` (the stable id `mcap-loader`, which new readers match first). A layout with a valid cache file loads through this element alone, with the connector plugin absent entirely. |
| `<materialize provider=… identity=…>` | The provider record: the stable provider manifest id (`mcap-cloud`), the content-addressed identity `mcap-cloud:v1:sha256/128:<32 hex>`, and the canonical descriptor JSON in CDATA. |
| descriptor JSON | Server URI, MCAP object keys, topic subset, absolute time window (decimal-nanosecond strings), `include_latched`, and a cosmetic `display_name`. |

**No secrets are embedded — ever.** The descriptor deliberately cannot carry a
bearer token, a certificate path, `allow_insecure`, or query history; validation
also *rejects* a `server_uri` carrying userinfo, a query string, or a fragment.
Credentials are resolved **per machine, at import time**:

- the token stored for that exact origin in the plugin's own credential store
  (`$XDG_CONFIG_HOME/mcap_cloud/`, mode 0600), or
- `MCAP_CLOUD_API_KEY` — **honored only when `MCAP_CLOUD_URL` is also set and its
  parsed origin (scheme + host + effective port) equals the layout's target
  origin** (spec §7 guard 2). A mismatched or absent `MCAP_CLOUD_URL` means the
  environment token is ignored outright, never sent to the wrong server.

`display_name` is excluded from the identity digest and from identity comparison
— renaming a session is not a new session.

---

## 2. Sharing policy — read before you send a layout

A secret-free layout is **not** a metadata-free layout. Verbatim from the design
record (`docs/canonical-layout-import.md` §7, "Residual, documented"):

> Residual, documented: secret-free layouts still leak customer/site/robot
> metadata via keys, hostnames, topics, time ranges — and the absolute cache path
> leaks the author's username/cache root. Inherent to shareable layouts; a
> sharing-policy note, not a mechanism fix.

Practical consequences:

- **Treat a layout like a document naming the recording.** Object keys carry the
  Hive dimensions (`customer=…/customer_site=…/robot=…`), the `server_uri` names
  your deployment's hostname, `topics` names your topic taxonomy, and
  `start_ns`/`end_ns` name when the run happened. Share layouts inside the trust
  boundary that already sees those recordings.
- **The `<fileInfo filename>` path leaks the author's username** (e.g.
  `/home/alice/.cache/…`). Harmless inside a team, awkward in a public issue
  tracker or a support ticket.
- **Never commit a real, customer-authored layout to this repository as "manual
  evidence".** It embeds real bucket keys, real hostnames, and a real home-directory
  path. The automated proof already exists (§10) and uses synthetic fixtures with
  frozen descriptor vectors — use that, or hand-author a synthetic layout, never a
  captured production one.
- Redacting a layout by hand is a trap. The identity is a digest **of the
  descriptor bytes**, so any edit makes it a different request (a cache miss —
  correct-but-slower, never wrong), while the `identity` attribute you left behind
  is not cross-checked by the host and silently becomes a lie in the file. If you
  need a sanitized layout, re-author it against a synthetic session; do not edit
  bytes.

---

## 3. Trust bootstrap — the one-time step on each machine

Layout import will not connect anywhere the user has not already connected
interactively. The gate is a durable ledger:

```
$XDG_CONFIG_HOME/mcap_cloud/trusted_origins.json      (0600, dir 0700)
{"v":1,"origins":["ws://localhost:8082","wss://mcap.example.com:443"]}
```

- One entry per **origin** — `scheme://host:port`, with the **effective port
  always explicit** (`wss` ⇒ 443, `ws` ⇒ 80 when the URL omits it), so
  `wss://h` and `wss://h:443` are one entry, not two.
- The ledger is written by **exactly one event: a successful *interactive* Hello**
  — i.e. connecting to that server once in PlotJuggler's **MCAP Cloud** panel.
  Saving credentials does not trust an origin; only a connection that actually
  succeeded does. If the durable write fails, the origin is **not** trusted
  anywhere (the panel says so) — there is no transient in-memory trust.
- A layout import never trusts an origin silently.

**Bootstrap on a new machine:** open PlotJuggler → MCAP Cloud toolbox → connect
to the server once (this also stores the API key if you enter one) → then the
shared layout imports with zero prompts.

Behavior when the origin is *not* trusted:

| Mode | Behavior |
|---|---|
| GUI (**File → Load Layout**) | An explicit confirmation prompt naming the server and the sources it wants to download. |
| Headless (`--layout`) | That source **fails with `layout-import-untrusted` and touches the network zero times** — the trust verdict comes from the provider's bounded, offline query. |

The refusal is **per source**: other sources in the same layout keep going, and
the layout still restores (see §6's exit-code note).

> Note: a **cache hit deliberately bypasses the trust gate** — the gate guards a
> network touch, and a hit downloads nothing. Trust matters on a *miss*.

---

## 4. The cache

```
$XDG_CACHE_HOME/mcap_cloud/sessions/<32-hex identity>.mcap     (files 0600, dir 0700)
```

- `MCAP_CLOUD_CACHE_DIR` **overrides the sessions directory itself** (it is used
  verbatim as the artifact directory — it is *not* a parent under which
  `mcap_cloud/sessions/` is created). With neither variable set, the root falls
  back to `$HOME/.cache/mcap_cloud/sessions`.
- The artifact is a normal, fully summarized MCAP file that additionally embeds
  the canonical descriptor as an MCAP Metadata record — a cache file
  self-describes which request produced it.

**Hit vs. miss.**

| | What runs | Cost |
|---|---|---|
| **Hit** (artifact present and structurally valid) | Ordinary lazy `data_load_mcap` file load | Zero network. Works even with the connector plugin absent. |
| **Miss** | Trust gate → one provider import job → progressive download with plots growing → promotion onto the freshly written artifact | One full download of that request. |

**Purge and recovery.** Deleting an artifact is always *safe*: the next load is a
cache miss, which is **correct-but-slower, never wrong**. There is exactly one
restriction, and it is real: **wipe the cache only when no cache-backed datasets
are loaded.** A loaded MCAP dataset re-opens its file lazily on cold-chunk
misses, so deleting the file out from under a live dataset breaks *that session*,
not the cache design.

**Sidecar files you will see next to an artifact** (all expected, none are data):

| Name | What |
|---|---|
| `<hex>.mcap.lock` | The identity's lock file — a *shared* lease while a dataset is loaded, an *exclusive* lock during materialization/eviction. |
| `<hex>.mcap.partial.<pid>` | A download in flight. Non-complete exits delete it; partials never get published. |
| `<hex>.mcap.touch` | The LRU timestamp (`atime` is unreliable under `relatime`/`noatime`). |

Housekeeping defaults: size-capped LRU at **20 GiB** with a **2 GiB** free-space
reserve enforced before a materialization starts; leased files are skipped;
orphan partials are cleaned only under the exclusive lock and past a 24 h age
threshold.

**Import ceilings.** The enforcement that exists today is plugin-side, against
*actual* transferred bytes and elapsed time: `MCAP_CLOUD_IMPORT_MAX_BYTES`
(default **32 GiB**) and `MCAP_CLOUD_IMPORT_MAX_SECONDS` (default **3600**).
Exceeding either aborts the job, which surfaces as `layout-import-job-failed`.
The host-side *estimate* ceiling exists in the mechanism but is wired to `0`
(no ceiling) in the shipped `MainWindow`, so `layout-import-size-limit` does not
fire today — the configurable preference is recorded future work.

---

## 5. Authoring a shareable layout (the GUI flow)

1. **MCAP Cloud** toolbox → connect to the server (this is also what trusts the
   origin, §3) → pick customer + site → select the recording(s), the topic
   subset, and the time window → **Fetch**.
2. Plots grow while the download runs. On completion the dataset is *promoted*:
   it silently becomes an ordinary file-backed dataset over the cache artifact,
   with the same `DatasetId`/`TopicId`s, so every curve you already placed
   survives.
3. Arrange plots as usual.
4. **File → Save Layout** → in the save dialog leave **"Bind to this data
   source"** checked (it is checked by default). That checkbox is what emits the
   `<fileInfo>` + `<materialize>` pair.
5. Reopen the layout (**File → Load Layout**) to confirm it restores.

> **This checklist is exploratory guidance only — it is NOT the acceptance
> proof.** The literal GUI flow (the real Save/Load `QAction`s, the real modal,
> the "Bind to this data source" default, the warm reload) is automated as
> **scenario 2 of the PJ4 `MainWindowLayoutImportE2ETest`** and is what the gate
> actually verifies. Run the checklist to explore; trust §10 to certify.

---

## 6. Headless / scripted use

```bash
plotjuggler4 --nosplash \
  --layout /path/to/session.pj4.xml \
  --exit-after-layout --exit-after-layout-timeout 300 \
  --dump-diagnostics /path/to/diagnostics.json
```

- `--exit-after-layout` quits at the **restore settlement boundary** — after the
  import batch has finished and every restore waiter has cleared, so it can never
  tear down a mid-flight import. It **requires** `--layout`.
- `--exit-after-layout-timeout <seconds>` (default 300) bounds the wait.
- `--dump-diagnostics <json>` serializes **every diagnostic of the whole run**
  (not the UI's 200-record ring) on exit — including on the failure paths.
- `--screenshot` is a watchdog only: it quits unconditionally and reports 0.
  **Never use it as the success oracle**; use `--exit-after-layout`'s exit code.

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | The restore settled and the layout was committed. |
| `1` | The restore settled without committing (open/parse/apply failure, cancel, cancelled batch). |
| `2` | No settlement within `--exit-after-layout-timeout`. |
| `64` | Usage error (`EX_USAGE`): `--exit-after-layout` without `--layout`, or an invalid timeout value. |

**Diagnostic dump envelope** (versioned — never a bare array):

```json
{
  "version": 1,
  "records": [
    { "level": "warning", "source": "Layout", "id": "layout-import-untrusted",
      "message": "Layout source '…' requires confirmation of an untrusted origin; …",
      "timestamp": "2026-08-01T12:34:56.789Z" }
  ]
}
```

`level` ∈ `info` | `warning` | `error`.

**Two disciplines that matter for scripting:**

1. **Assert on `id`, never on `message` text.** Messages are translated and
   reworded; ids are the stable contract.
2. **A failed-but-committed import still exits 0.** Per-source failures are
   deliberately non-fatal — the batch keeps going, unresolved curves are retained
   and diagnosed rather than deleted, and the layout commits. So the exit code
   tells you "the layout restored"; **the diagnostic ids tell you whether it
   restored the data you wanted.** A script that only checks `$?` will pass on a
   layout that imported nothing.

---

## 7. Diagnostic-ID reference

Emitted by `LayoutImportBatch` / `MainWindow` (source field `Layout`).

**Failure ids — any of these means a source did not import as intended:**

| id | Cause |
|---|---|
| `layout-import-untrusted` | The origin is not in the trust ledger and the run is non-interactive. Fix: connect once in the MCAP Cloud panel (§3). |
| `layout-import-refused` | The provider actively rejected the descriptor (e.g. unknown `v`/`kind`, failed validation, limits). Fails the source even on a local hit — fail-closed. |
| `layout-import-descriptor-invalid` | The `<materialize>` record is unparseable / missing provider or identity. Falls back to the saved path. |
| `layout-import-provider-unavailable` | The `mcap-cloud` provider plugin could not be bound (not installed, failed to load). Falls back to the saved path. |
| `layout-import-query-failed` | The bounded provider query could not resolve the source (no local path / query error). Falls back to the saved path. |
| `layout-import-query-invalid` | The provider answered without a source identity — malformed; classification is refused rather than guessed. |
| `layout-import-materialized-missing` | The provider reported the artifact as materialized but nothing exists at that path — an inconsistent cache verdict, failed loudly instead of silently re-importing. |
| `layout-import-size-limit` | The query's `estimated_bytes` exceeds the host's configured per-machine transfer ceiling; refused non-interactively. **Dormant today** — the shipped `MainWindow` wires that ceiling to 0 (§4); the live ceilings are the plugin's byte/second env caps, which surface as `layout-import-job-failed`. |
| `layout-import-job-start-failed` | `start_import` never started (bad request, provider refusal at start). |
| `layout-import-job-failed` | The import job ended `FAILED` — network, auth, decode, or a byte/time ceiling. The message carries the provider's own text; on a genuinely credential-less auth rejection it appends the machine-gated hint *"no stored credential for this server — connect once in the MCAP Cloud toolbox or set `MCAP_CLOUD_API_KEY` (with `MCAP_CLOUD_URL` matching this origin)"*. A presented-but-rejected key gets no hint (it would lie). |
| `layout-import-cancelled` | The import (or the whole batch) was cancelled — Stop, superseded layout, shutdown. |
| `layout-import-load-rejected` | The loader refused to enqueue the file at all (no ticket). |
| `layout-import-load-failed` | The stock load of a cache-hit artifact failed. |
| `layout-import-source-missing` | A non-provider source in the layout does not exist on disk and nothing could re-obtain it. |
| `layout-import-rollback-failed` | A cancelled import was unwound but the previous workspace could not be restored exactly. |

**Non-failure ids — informational, but load-bearing:**

| id | Meaning |
|---|---|
| `layout-import-eager-only` (`info`) | **The data is usable, but no re-importable source record was attached.** Per spec §10: the download completed and the dataset is fully populated, but promotion onto a cache artifact did not happen (typically the cache tee was dropped — unwritable cache root, disk pressure, a lock refusal). Consequence: **re-saving the layout will NOT carry a `<materialize>` stanza for that dataset**, so the layout stops being re-importable. Everything else works. Fix the cache root and re-run. |
| `layout-import-unresolved-curves` (`warning`) | Curves could not be bound after the import drained. The curves are **retained**, not deleted, so a later binding pass can still resolve them. In practice this is the *decode oracle*: it usually means the data arrived but nothing decoded it (missing/incompatible parser plugin, or a topic/field that no longer exists). |
| `layout-import-unresolved-curves-sync` (`warning`) | Same, on the synchronous restore leg (no import job involved). |

---

## 8. Server-side signals for troubleshooting

The server's Prometheus endpoint (`http://<server>:<port>/metrics`) is the
cheapest way to tell a cache hit from a real import:

| Counter | Meaning |
|---|---|
| `pj_cloud_sessions_total` | Total streaming sessions opened. **Unchanged across a layout load ⇒ the load read the cache**, it did not download. |
| `pj_cloud_ws_connections_total` | Total WebSocket connections accepted. **Unchanged ⇒ the client did not even connect.** |

Scrape both before and after a `--layout` run:

```bash
before=$(curl -fsS http://localhost:8082/metrics | awk '$1=="pj_cloud_sessions_total"{print $2}')
plotjuggler4 --layout session.pj4.xml --exit-after-layout --dump-diagnostics dump.json
after=$(curl -fsS  http://localhost:8082/metrics | awk '$1=="pj_cloud_sessions_total"{print $2}')
```

Interpretation:

- both counters unchanged + exit 0 + no failure ids ⇒ **warm cache hit**;
- `pj_cloud_sessions_total` incremented ⇒ **cold import** (expected on a fresh
  machine, unexpected on a second run — check whether the artifact is actually
  where the identity says, and whether something purged it);
- `pj_cloud_ws_connections_total` unchanged while a *miss* was expected ⇒ the
  trust gate refused before any network touch: look for
  `layout-import-untrusted` in the dump.

---

## 9. The version/artifact matrix (pinned loader + parser)

Import has a **runtime dependency on a compatible loader and parser set** —
promotion and cache-hit loads both go through the `mcap-loader` manifest id with
a locked preset, and curves only bind if a real ROS parser decodes the payloads.
Version strings on a manifest are not proof of ABI compatibility, so the E2E
harness does not trust them: it **rebuilds and records**.

What the harness stages, per run (`scripts/e2e-layout-import.sh`, step f):

| DSO | Source |
|---|---|
| `libtoolbox_mcap_cloud_plugin.so` (`mcap-cloud`) | This repo's plugin build; its SDK is preflight-checked against `plugin/SDK_VERSION`. |
| `libmcap_source_plugin.so` (`mcap-loader`) | `pj-official-plugins/data_load_mcap`, **rebuilt against `plugin/SDK_VERSION`**. |
| `libparser_ros_plugin.so` (`ros-parser`) | `pj-official-plugins/parser_ros`, same rule. |

For each staged DSO one provenance line is recorded:

```
mcap-loader=1.0.0 sdk=0.20.0 repo=/home/…/pj-official-plugins rev=<12-hex>[-dirty] sha256=<64-hex> so=libmcap_source_plugin.so
```

The record lands in `provenance.txt` inside the **kept-artifacts directory**
(`/tmp/pj-e2e-layout-artifacts/<timestamp>/`, override `E2E_ARTIFACT_DIR`)
alongside every diagnostic dump and log — it survives the harness's scratch
teardown precisely so a failed run can be explained afterwards.

Before the DSOs are used, `plotjuggler4 --validate-plugins <dir>` runs with an
`--expect-plugin <id>=<version>` entry for **all three** (the validator also
rejects loaded-but-*unexpected* plugins).

**Standing caveat (recorded follow-up):** the `pj-official-plugins` checkout's
own `SDK_VERSION` is pinned at **0.18.0**, while PJ4 and this repo's plugin are
on **0.20.0**. The harness therefore temp-edits that file, rebuilds the loader
and parser against 0.20.0, records the result, and **always restores the file**
(nothing is committed there). Staging the checkout's pre-built 0.18.0 binaries is
explicitly refused. Moving the upstream pin to 0.20.0 is a separate
`pj-official-plugins` PR and a recorded follow-up, not a harness workaround.

---

## 10. Running the gate

```bash
make e2e-layout                     # or: bash scripts/e2e-layout-import.sh
bash scripts/e2e-layout-import.sh --dry-run   # print the plan, touch nothing
```

Final line is exactly `E2E-LAYOUT-IMPORT PASS` or
`E2E-LAYOUT-IMPORT FAIL: <step>` (exit code matches); preflight problems exit 2
with a one-line `E2E-LAYOUT-IMPORT PREFLIGHT FAIL` reason.

**What it owns and what it borrows:**

- **Owns `:8082`** — its own Go server, its own `e2e-layout` bucket, its own
  catalog DB, config, logs, staged DSOs and XDG sandboxes in a per-run
  `mktemp -d` scratch that is removed on every exit path. Preflight fails if
  `:8082` is already taken.
- **Borrows Minio** — starts it if it is down, and **never** `docker compose
  down`s it. `make smoke` and `run.sh` share that daemon.
- **Shares one lock with smoke** — both take a machine-wide flock on
  `/tmp/pj-cloud-harness.lock`. If the other harness is running, this one **waits
  with a message**; it does not fail. (Both rebuild `server/bin`, both poke
  Minio.)

**Prerequisites** (preflight names them; all are hard requirements):

- A **PJ4 build carrying the stage-5 surface**: `--dump-diagnostics`,
  `--exit-after-layout`, and the `main_window_layout_import_e2e_test` binary.
  Default build tree `~/ws_plotjuggler/PJ4/build`; move both halves at once with
  `E2E_PJ4_BUILD` (the shipped binary and the gui-test must come from ONE tree so
  the staged DSOs meet the same ABI on both sides). `E2E_REQUIRE_PJ4_FLAGS=0`
  downgrades steps h/i to a loud SKIPPED-pending — for bisecting bring-up only.
- The connector plugin built in this repo (`./build.sh toolbox_mcap_cloud`),
  against `plugin/SDK_VERSION`.
- A `pj-official-plugins` checkout (`E2E_OFFICIAL_PLUGINS`).
- The builder venv at `~/.venvs/pj-catalog`, Docker, Go, `flock`.

**The bucket-wipe rule.** The corpus in `s3://e2e-layout` is seeded **once** and
left in place across runs (the seed is idempotent: `seed -check` only seeds an
empty bucket). It is the decode oracle for both halves of the gate, so **after
any change to `gen-ci-fixtures` / `internal/genmcap` you must empty the bucket to
force a reseed.** A stale corpus surfaces as `layout-import-unresolved-curves`,
and the harness's failure message says exactly that. (`make smoke` wipes and
reseeds its own bucket every run and is unaffected.)

**What the gate proves**, in one line each:

- steps a–g — shared Go tools, deterministic corpus, one-shot catalog build,
  read-only server on `:8082`, the frozen `e2e-8082-*` descriptor vectors bound
  to the served corpus, three real DSOs staged with provenance, and
  `--validate-plugins`;
- step h — the live PJ4 gui-test, five scenarios through a real `MainWindow`:
  cold promotion with a live progressive witness, the literal GUI save/reload
  flow warm and zero-network, `layout-import-eager-only` in-process, the trust
  gate, and a three-way catalog-equality signature. The harness sets the live
  env, so a **SKIPPED test fails the run**;
- step i — three legs of the **shipped** `plotjuggler4` binary (cold / warm /
  EAGER) in private XDG sandboxes, asserting diagnostic ids, the artifact, and
  the zero-network metrics.
