"""Drain S3 event notifications (S3 -> SQS) into the worker queue, ack-safely.

This is the cloud-native replacement for the watchdog/inotify observer: a
PRODUCER that only enqueues work, leaving the single worker as the sole DB
writer. Design: docs/plans/2026-07-30-builder-event-discovery-design.md §3.

The at-least-once contract (§3.3) is a batch handshake, not ack-on-enqueue:

- The producer parses and validates the COMPLETE message body BEFORE enqueueing
  anything. A malformed body is never acked (SQS redrives it to the DLQ); a
  record-less body (e.g. ``s3:TestEvent``) is acked immediately.
- Every translated record shares one ``SqsBatch``. The worker marks a record
  terminal only after its DB outcome has COMMITTED (cataloged, ETag-skipped,
  committed quarantine, confirmed delete, safe no-op). A raised exception is
  NOT terminal — the message stays unacked and redelivers after the visibility
  timeout; worker idempotency absorbs the replay.
- When the last record of a batch goes terminal the batch lands on an ack
  queue; THIS producer thread performs ``DeleteMessage`` (all SQS I/O stays off
  the DB-writer thread). A delete failing (e.g. expired receipt) logs and moves
  on — the redelivered copy is absorbed by idempotency.

Robustness (§3.5): the drain loop is self-supervising (bounded-backoff retry
around every iteration — a network/IAM error must never silently kill the
thread), backpressure is bounded by BUFFERED RECORDS (one message can carry
many records; SQS holds the durable backlog while we stop receiving), and an
``IntakeGate`` lets the audit coordinator pause intake and wait until every
received batch is terminal AND acked before an audit runs.

Like ``s3_storage``, this module does not import boto3; the SQS client is
injected so it stays testable and AWS stays a runtime-only dependency. The
optional ``status`` collaborator is duck-typed (``producer_poll_ok`` /
``producer_acked`` / ``event_unknown`` — see status.StatusWriter).
"""

import json
import logging
import queue
import threading
import urllib.parse
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Supervision backoff for a failing receive/parse iteration (seconds).
_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 60.0
# How long the loop sleeps while paused or backpressured before re-checking.
_IDLE_WAIT = 0.2


class SqsBatch:
    """Ack accounting for one SQS message: terminal-record countdown -> ack queue.

    The worker thread calls ``record_terminal`` once per COMMITTED record; the
    batch enqueues itself on ``ack_q`` when the last record completes. All SQS
    I/O (the actual DeleteMessage) happens on the producer thread.
    """

    def __init__(self, receipt_handle: str, total: int, ack_q: "queue.Queue",
                 gate: "IntakeGate | None" = None) -> None:
        self.receipt_handle = receipt_handle
        self._remaining = total
        self._ack_q = ack_q
        self._gate = gate
        self._lock = threading.Lock()

    def record_terminal(self) -> None:
        with self._lock:
            if self._remaining <= 0:
                return  # defensive: never double-count / double-ack
            self._remaining -= 1
            done = self._remaining == 0
        if self._gate is not None:
            self._gate.record_done()
        if done:
            self._ack_q.put(self)


@dataclass(frozen=True)
class EventRecord:
    """One translated S3 event, carrying its batch for ack accounting."""

    kind: str  # "catalog" | "delete"
    key: str  # decoded object key
    batch: SqsBatch = field(repr=False)


class IntakeGate:
    """Producer/coordinator handshake (§3.5): pause intake, drain, run audit.

    Tracks records buffered (enqueued, not yet terminal) for backpressure and
    batches outstanding (received, not yet acked) for the drain barrier.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._paused = False
        self._records_buffered = 0
        self._batches_outstanding = 0

    # -- producer side -----------------------------------------------------
    @property
    def paused(self) -> bool:
        with self._cond:
            return self._paused

    def records_buffered(self) -> int:
        with self._cond:
            return self._records_buffered

    def batch_received(self, n_records: int) -> None:
        with self._cond:
            self._batches_outstanding += 1
            self._records_buffered += n_records

    def record_done(self) -> None:
        with self._cond:
            self._records_buffered = max(0, self._records_buffered - 1)
            self._cond.notify_all()

    def batch_acked(self) -> None:
        with self._cond:
            self._batches_outstanding = max(0, self._batches_outstanding - 1)
            self._cond.notify_all()

    # -- coordinator side --------------------------------------------------
    def pause(self) -> None:
        with self._cond:
            self._paused = True

    def resume(self) -> None:
        with self._cond:
            self._paused = False
            self._cond.notify_all()

    def wait_drained(self, stop_event: threading.Event, poll: float = 0.1) -> bool:
        """Block until every received batch is acked (or stop). True = drained."""
        while True:
            with self._cond:
                if self._batches_outstanding == 0:
                    return True
            if stop_event.wait(poll):
                return False


def _classify(event_name: str) -> str | None:
    """Map an S3 event name to a record kind, or None for irrelevant names.

    Lifecycle expirations (§3.2) are first-class deletes (both
    ``LifecycleExpiration:Delete`` and ``:DeleteMarkerCreated``): the worker's
    HEAD-guard turns a versioned-bucket expiry that leaves a live current
    version into a re-catalog instead of a row delete.
    """
    if event_name.startswith("ObjectCreated"):
        return "catalog"
    if event_name.startswith("ObjectRemoved"):
        return "delete"
    if event_name.startswith("LifecycleExpiration"):
        return "delete"
    return None


def translate_body(body: str, on_unknown=None) -> "list[tuple[str, str]]":
    """Parse ONE complete SQS body into ``(kind, key)`` pairs, or raise.

    Raises on any malformed body (bad JSON, wrong shapes, missing keys) so the
    caller can leave the message unacked for DLQ redrive. A valid body with no
    relevant records (``s3:TestEvent``, non-.mcap keys, unknown event names)
    returns ``[]``. ``on_unknown(name)`` is called for each unknown event name.
    """
    doc = json.loads(body)
    if not isinstance(doc, dict):
        raise ValueError("SQS body is not a JSON object")
    records = doc.get("Records", [])
    if not isinstance(records, list):
        raise ValueError("Records is not a list")
    out: list[tuple[str, str]] = []
    for rec in records:
        name = rec.get("eventName", "")
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        kind = _classify(name)
        if kind is None:
            if on_unknown is not None:
                on_unknown(name)
            logger.info("ignoring unsupported S3 event name %r for %s", name, key)
            continue
        if not key.endswith(".mcap"):
            continue
        out.append((kind, key))
    return out


def _drain_acks(sqs, queue_url: str, ack_q: "queue.Queue", gate, status) -> None:
    """Delete every completed batch's message; failures log and move on."""
    while True:
        try:
            batch = ack_q.get_nowait()
        except queue.Empty:
            return
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=batch.receipt_handle)
            if status is not None:
                status.producer_acked()
        except Exception:  # noqa: BLE001 - expired receipt etc.: redelivery is absorbed
            logger.warning(
                "DeleteMessage failed (receipt may have expired); "
                "redelivery will be absorbed by idempotency",
                exc_info=True,
            )
        finally:
            if gate is not None:
                gate.batch_acked()


def s3_event_producer(
    sqs,
    queue_url: str,
    work_q: "queue.Queue",
    stop_event: threading.Event,
    *,
    gate: IntakeGate | None = None,
    status=None,
    max_buffered_records: int = 256,
    poll_wait_seconds: int = 20,
    backoff_initial: float = _BACKOFF_INITIAL,
) -> None:
    """Supervised long-poll drain loop; runs until ``stop_event`` is set.

    Every iteration is wrapped in bounded-backoff retry so a transient
    network/credential failure can never silently kill the producer thread
    (§3.5) — the daemon would otherwise look healthy with a dead intake.
    """
    ack_q: "queue.Queue[SqsBatch]" = queue.Queue()
    backoff = backoff_initial
    while not stop_event.is_set():
        try:
            _drain_acks(sqs, queue_url, ack_q, gate, status)

            if gate is not None and (
                gate.paused or gate.records_buffered() >= max_buffered_records
            ):
                # Paused for an audit, or backpressured: SQS holds the durable
                # backlog while we stop receiving. Keep draining acks so a
                # paused audit's wait_drained() can complete.
                stop_event.wait(_IDLE_WAIT)
                continue

            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=poll_wait_seconds,
            )
            if status is not None:
                status.producer_poll_ok()
            for msg in resp.get("Messages", []):
                _process_message(msg, queue_url, sqs, work_q, ack_q, gate, status)
            backoff = backoff_initial  # a full successful iteration resets it
        except Exception:  # noqa: BLE001 - supervision: log, back off, keep serving
            logger.exception("SQS intake iteration failed; retrying in %.1fs", backoff)
            if stop_event.wait(backoff):
                break
            backoff = min(_BACKOFF_MAX, backoff * 2)
    # Final ack sweep so records the worker completed during shutdown are not
    # needlessly redelivered (best-effort; anything left simply redelivers).
    _drain_acks(sqs, queue_url, ack_q, gate, status)


def _process_message(msg, queue_url: str, sqs, work_q, ack_q, gate, status) -> None:
    """Validate one message fully, then enqueue its records or ack/skip it."""
    body = msg.get("Body", "{}")
    receipt = msg["ReceiptHandle"]
    try:
        pairs = translate_body(
            body,
            on_unknown=(status.event_unknown if status is not None else None),
        )
    except Exception:  # noqa: BLE001 - malformed: leave UNACKED for DLQ redrive
        logger.exception("unparseable SQS message left for redrive (DLQ)")
        return
    if not pairs:
        # Valid but nothing to do (s3:TestEvent, non-.mcap, unknown names):
        # ack immediately — there is no work to gate the ack on.
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except Exception:  # noqa: BLE001
            logger.warning("DeleteMessage failed for empty message", exc_info=True)
        return
    batch = SqsBatch(receipt, len(pairs), ack_q, gate)
    if gate is not None:
        gate.batch_received(len(pairs))
    for kind, key in pairs:
        work_q.put(EventRecord(kind, key, batch))
