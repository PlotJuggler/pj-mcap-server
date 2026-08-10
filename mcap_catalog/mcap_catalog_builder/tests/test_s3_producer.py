"""Tests for the ack-hardened SQS producer (design §3.2/§3.3/§3.5).

The contract under test (docs/plans/2026-07-30-builder-event-discovery-design.md):
full-body validation BEFORE enqueue, batch ack only after every record is
terminal, malformed bodies left unacked for DLQ redrive, lifecycle event names
translated as deletes, supervision surviving receive failures, record-bounded
backpressure, and the pause/drain intake gate.
"""

import json
import queue
import threading
import time

import pytest

from mcap_catalog_builder.s3_producer import (
    EventRecord,
    IntakeGate,
    SqsBatch,
    s3_event_producer,
    translate_body,
)

_KEY_ENC = "customer%3Dacme/customer_site%3Dhq/robot%3Dr1/source%3Dros/date%3D2026-06-02/x.mcap"
_KEY_DEC = "customer=acme/customer_site=hq/robot=r1/source=ros/date=2026-06-02/x.mcap"


def _s3_event(records: list[tuple[str, str]]) -> str:
    return json.dumps(
        {"Records": [{"eventName": n, "s3": {"object": {"key": k}}} for n, k in records]}
    )


def _wait_until(cond, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


# --------------------------------------------------------------------------
# translate_body: full-body validation + event-name classification (§3.2)
# --------------------------------------------------------------------------

def test_translate_created_and_removed():
    body = _s3_event([("ObjectCreated:Put", _KEY_ENC), ("ObjectRemoved:Delete", _KEY_ENC)])
    assert translate_body(body) == [("catalog", _KEY_DEC), ("delete", _KEY_DEC)]


def test_translate_lifecycle_expirations_are_deletes():
    body = _s3_event([
        ("LifecycleExpiration:Delete", _KEY_ENC),
        ("LifecycleExpiration:DeleteMarkerCreated", _KEY_ENC),
    ])
    assert translate_body(body) == [("delete", _KEY_DEC), ("delete", _KEY_DEC)]


def test_translate_unknown_event_name_is_counted_not_dropped_silently():
    seen: list[str] = []
    body = _s3_event([("ObjectRestore:Completed", _KEY_ENC), ("ObjectCreated:Put", _KEY_ENC)])
    assert translate_body(body, on_unknown=seen.append) == [("catalog", _KEY_DEC)]
    assert seen == ["ObjectRestore:Completed"]


def test_translate_non_mcap_keys_are_skipped():
    body = _s3_event([("ObjectCreated:Put", "customer%3Dacme/notes.txt")])
    assert translate_body(body) == []


def test_translate_test_event_yields_no_records():
    assert translate_body(json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent"})) == []


@pytest.mark.parametrize("body", [
    "{not json",
    json.dumps([1, 2]),                       # not an object
    json.dumps({"Records": {"a": 1}}),        # Records not a list
    json.dumps({"Records": [{"eventName": "ObjectCreated:Put"}]}),  # no s3.object.key
])
def test_translate_malformed_bodies_raise(body):
    with pytest.raises(Exception):
        translate_body(body)


# --------------------------------------------------------------------------
# Producer loop: batch ack handshake, DLQ redrive, supervision, backpressure
# --------------------------------------------------------------------------

class ScriptedSQS:
    """Serves each scripted receive result once, then empty batches forever."""

    def __init__(self, results) -> None:
        self._results = list(results)
        self.deleted: list[str] = []
        self.receive_calls = 0
        self.delete_error: Exception | None = None

    def receive_message(self, QueueUrl, MaxNumberOfMessages=10, WaitTimeSeconds=20):
        self.receive_calls += 1
        if self._results:
            nxt = self._results.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return {"Messages": nxt}
        return {"Messages": []}

    def delete_message(self, QueueUrl, ReceiptHandle):
        if self.delete_error is not None:
            err, self.delete_error = self.delete_error, None
            raise err
        self.deleted.append(ReceiptHandle)


def _run_producer(sqs, stop, work_q, gate=None, **kw):
    t = threading.Thread(
        target=s3_event_producer,
        args=(sqs, "http://q", work_q, stop),
        kwargs={"gate": gate, "poll_wait_seconds": 0, **kw},
        daemon=True,
    )
    t.start()
    return t


def test_multi_record_message_acked_only_after_every_record_commits():
    stop, work_q, gate = threading.Event(), queue.Queue(), IntakeGate()
    body = _s3_event([("ObjectCreated:Put", _KEY_ENC), ("ObjectRemoved:Delete", _KEY_ENC)])
    sqs = ScriptedSQS([[{"Body": body, "ReceiptHandle": "rh-1"}]])
    t = _run_producer(sqs, stop, work_q, gate)
    try:
        rec1 = work_q.get(timeout=2)
        rec2 = work_q.get(timeout=2)
        assert isinstance(rec1, EventRecord) and rec1.kind == "catalog"
        assert rec2.kind == "delete" and rec2.key == _KEY_DEC
        time.sleep(0.3)
        assert sqs.deleted == []  # nothing terminal yet: no ack
        rec1.batch.record_terminal()
        time.sleep(0.3)
        assert sqs.deleted == []  # one of two records terminal: still no ack
        rec2.batch.record_terminal()
        assert _wait_until(lambda: sqs.deleted == ["rh-1"])
        assert gate.wait_drained(stop)  # acked batch left the outstanding set
    finally:
        stop.set()
        t.join(timeout=3)


def test_malformed_body_left_unacked_for_dlq_redrive():
    stop, work_q = threading.Event(), queue.Queue()
    sqs = ScriptedSQS([[{"Body": "{not json", "ReceiptHandle": "rh-bad"}]])
    t = _run_producer(sqs, stop, work_q)
    try:
        assert _wait_until(lambda: sqs.receive_calls >= 2)  # loop moved on
        assert work_q.empty()
        assert sqs.deleted == []  # NEVER acked: SQS redrives to the DLQ
    finally:
        stop.set()
        t.join(timeout=3)


def test_record_less_message_acked_immediately():
    stop, work_q = threading.Event(), queue.Queue()
    body = json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent"})
    sqs = ScriptedSQS([[{"Body": body, "ReceiptHandle": "rh-test"}]])
    t = _run_producer(sqs, stop, work_q)
    try:
        assert _wait_until(lambda: sqs.deleted == ["rh-test"])
        assert work_q.empty()
    finally:
        stop.set()
        t.join(timeout=3)


def test_expired_receipt_on_delete_logs_and_moves_on():
    stop, work_q, gate = threading.Event(), queue.Queue(), IntakeGate()
    body = _s3_event([("ObjectCreated:Put", _KEY_ENC)])
    sqs = ScriptedSQS([[{"Body": body, "ReceiptHandle": "rh-exp"}]])
    sqs.delete_error = RuntimeError("ReceiptHandle is invalid")
    t = _run_producer(sqs, stop, work_q, gate)
    try:
        rec = work_q.get(timeout=2)
        rec.batch.record_terminal()
        # The failed DeleteMessage must still drain the gate (redelivery is
        # absorbed by idempotency) and must not kill the loop.
        assert _wait_until(lambda: gate.wait_drained(stop))
        assert _wait_until(lambda: sqs.receive_calls >= 3)
    finally:
        stop.set()
        t.join(timeout=3)


def test_supervision_survives_receive_failures_with_backoff():
    stop, work_q = threading.Event(), queue.Queue()
    body = _s3_event([("ObjectCreated:Put", _KEY_ENC)])
    sqs = ScriptedSQS([
        RuntimeError("network down"),
        RuntimeError("still down"),
        [{"Body": body, "ReceiptHandle": "rh-after"}],
    ])
    t = _run_producer(sqs, stop, work_q, backoff_initial=0.01)
    try:
        rec = work_q.get(timeout=3)  # the loop outlived two failures
        assert rec.key == _KEY_DEC
        rec.batch.record_terminal()
        assert _wait_until(lambda: sqs.deleted == ["rh-after"])
    finally:
        stop.set()
        t.join(timeout=3)


def test_backpressure_bounded_by_buffered_records():
    stop, work_q, gate = threading.Event(), queue.Queue(), IntakeGate()
    one = [{"Body": _s3_event([("ObjectCreated:Put", _KEY_ENC)]), "ReceiptHandle": "rh-a"}]
    two = [{"Body": _s3_event([("ObjectCreated:Put", _KEY_ENC)]), "ReceiptHandle": "rh-b"}]
    sqs = ScriptedSQS([one, two])
    t = _run_producer(sqs, stop, work_q, gate, max_buffered_records=1)
    try:
        rec_a = work_q.get(timeout=2)
        time.sleep(0.4)
        # One record is buffered (>= max): the second message must NOT have
        # been received — SQS holds the durable backlog.
        assert sqs.receive_calls == 1
        rec_a.batch.record_terminal()
        rec_b = work_q.get(timeout=3)  # backpressure released
        rec_b.batch.record_terminal()
        assert _wait_until(lambda: set(sqs.deleted) == {"rh-a", "rh-b"})
    finally:
        stop.set()
        t.join(timeout=3)


def test_paused_gate_stops_receiving_until_resume():
    stop, work_q, gate = threading.Event(), queue.Queue(), IntakeGate()
    one = [{"Body": _s3_event([("ObjectCreated:Put", _KEY_ENC)]), "ReceiptHandle": "rh-p"}]
    sqs = ScriptedSQS([one])
    gate.pause()
    t = _run_producer(sqs, stop, work_q, gate)
    try:
        time.sleep(0.4)
        assert sqs.receive_calls == 0  # paused: no intake at all
        gate.resume()
        rec = work_q.get(timeout=3)
        rec.batch.record_terminal()
        assert _wait_until(lambda: sqs.deleted == ["rh-p"])
    finally:
        stop.set()
        t.join(timeout=3)


# --------------------------------------------------------------------------
# SqsBatch / IntakeGate unit behavior
# --------------------------------------------------------------------------

def test_batch_double_terminal_never_double_acks():
    ack_q: queue.Queue = queue.Queue()
    b = SqsBatch("rh", 1, ack_q)
    b.record_terminal()
    b.record_terminal()  # defensive no-op
    assert ack_q.get_nowait() is b
    assert ack_q.empty()


def test_gate_wait_drained_returns_false_on_stop():
    gate, stop = IntakeGate(), threading.Event()
    gate.batch_received(1)
    stop.set()
    assert gate.wait_drained(stop) is False


# --------------------------------------------------------------------------
# §3.2 event-family payloads (tests/data/s3_events; SYNTHESIZED per the AWS
# event structure — see the data dir's README for the capture follow-up)
# --------------------------------------------------------------------------

import pathlib

from mcap_catalog_builder.s3_producer import translate_body

_DATA = pathlib.Path(__file__).parent / "data" / "s3_events"
_EXPECT = {"create": "catalog", "delete": "delete", "ack": None}


@pytest.mark.parametrize(
    "path", sorted(_DATA.glob("*.json")), ids=lambda p: p.name
)
def test_translator_handles_event_family_payloads(path):
    """Family encoded in the filename prefix: create_* -> one catalog record,
    delete_* -> one delete record (incl. both LifecycleExpiration shapes),
    ack_* -> zero records (record-less bodies ack immediately). The key must
    round-trip URL-DECODED (S3 events carry %-encoded keys — '=' as %3D)."""
    family = path.name.split("_", 1)[0]
    records = translate_body(path.read_text())
    if _EXPECT[family] is None:
        assert records == []
    else:
        assert [kind for kind, _key in records] == [_EXPECT[family]]
        key = records[0][1]
        assert key.endswith(".mcap")
        assert "%" not in key and key.startswith("customer=")  # decoded


def test_event_family_fixtures_cover_all_three_families():
    """All translator families are pinned by a fixture — without this, an
    empty data dir would silently collect zero parametrized tests above."""
    names = [p.name for p in _DATA.glob("*.json")]
    assert any(n.startswith("create_") for n in names)
    assert any(n.startswith("delete_lifecycleexpiration") for n in names)
    assert any(n.startswith("ack_") for n in names)
