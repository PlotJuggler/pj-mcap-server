"""Targeted MCAP summary read: fetch only the record groups the catalog needs.

The catalog consumes schemas, channels and statistics — never the ChunkIndex
group that dominates a chunked recording's summary section. The footer's
summary-offset section indexes the summary by group, so this reader fetches:

    read 1: the file's last ``tail`` bytes (footer + summary-offset section,
            usually also the Statistics group)
    read 2: (when not already inside the tail) the Schema+Channel groups,
            coalesced into one range.

This is a BOUNDED SPECULATIVE OVER-READ, not a byte-exact fetch: the fixed
tail may include trailing DATA-section (body) bytes — a small file arrives
entirely inside it — and the <=4 KiB coalescing gap may bridge unrelated
summary records. The guarantee is BOUNDEDNESS: total reads never exceed
``tail`` + the needed groups + 4 KiB per bridged gap, and every COMPUTED
range is validated against [summary_start, summary_offset_start), so a
corrupt footer/offset can never trigger an unbounded or body-sized read.

Measured 2026-07-29 (25,559-file staging corpus): 3 GETs/file -> 1.58,
354 -> 214 KiB/file, cold build 1.58x, catalog semantically identical.

Transport-agnostic: all reads go through ``pread(lo, hi_incl) -> bytes``.

STRICTLY AN OPTIMIZATION LAYER: any structural surprise raises
``TargetedUnavailable`` and ``read_summary_with_fallback`` re-reads via the
streamed baseline, which is the sole authority for quarantine verdicts. One
carve-out: a well-framed footer with ``summary_start == 0`` raises the
baseline's exact ``ValueError("no summary/statistics in MCAP")`` directly.
Accepted relaxation (2026-07-29 review Q1): offset 0 is only validated when
the tail already covers it, so a large file corrupt ONLY in its leading magic
is cataloged here; the Go FormatCodec still rejects it at stream-open.
"""

import io
import logging
import struct
from typing import BinaryIO, Callable

from mcap.data_stream import ReadDataStream
from mcap.opcode import Opcode
from mcap.records import Channel, Schema, Statistics, SummaryOffset

from .mcap_summary import ChannelInfo, FileSummary, summary_from_stream

logger = logging.getLogger(__name__)

MAGIC = b"\x89MCAP0\r\n"
DEFAULT_TAIL = 64 * 1024   # 42% of the staging corpus resolves in ONE read
_FOOTER_LEN = 37           # op(1) + len(8) + payload(20) + trailing magic(8)
_FOOTER_PAYLOAD = 20
_COALESCE_GAP = 4096       # merge needed groups separated by AT MOST this much
_MAX_OFFSETS_LEN = 64 * 1024  # sanity cap: ~7 SummaryOffset records expected

_READERS = {Opcode.SCHEMA: Schema.read, Opcode.CHANNEL: Channel.read,
            Opcode.STATISTICS: Statistics.read,
            Opcode.SUMMARY_OFFSET: SummaryOffset.read}


class TargetedUnavailable(Exception):
    """The targeted path cannot serve this file — use the streamed fallback."""


class _NoSummary(ValueError):
    """Well-framed footer with summary_start == 0 (the baseline's verdict)."""


def _iter_records(buf: bytes):
    """Yield ``(opcode, record | None)`` for each record framed in ``buf``.

    Validates that each known reader consumes exactly its declared payload;
    any framing anomaly is TargetedUnavailable (never a wrong parse).
    """
    stream = ReadDataStream(io.BytesIO(buf))
    end = len(buf)
    while stream.count < end:
        if stream.count + 9 > end:
            raise TargetedUnavailable("truncated record framing")
        opcode = stream.read1()
        length = stream.read8()
        payload_start = stream.count
        if payload_start + length > end:
            raise TargetedUnavailable("record overruns fetched range")
        reader = _READERS.get(opcode)
        if reader is None:
            stream.read(length)
            yield opcode, None
            continue
        try:
            rec = reader(stream)
        except Exception as e:
            raise TargetedUnavailable(f"record reader failed: {e}") from e
        consumed = stream.count - payload_start
        if consumed > length:
            raise TargetedUnavailable("record reader overran declared length")
        if consumed < length:
            stream.read(length - consumed)  # spec-permitted trailing padding
        yield opcode, rec


def read_summary_targeted(
    pread: Callable[[int, int], bytes], size: int, tail: int = DEFAULT_TAIL
) -> FileSummary:
    """Read ``FileSummary`` via footer + summary-offset section only.

    ``pread(lo, hi)`` must return the INCLUSIVE byte range ``[lo, hi]`` (i.e.
    ``hi`` is the last byte returned, not one-past-the-end). Raises
    ``TargetedUnavailable`` for any structural surprise (the caller should
    fall back to the streamed reader), or the carve-out ``_NoSummary``
    (a ``ValueError`` subclass) when a well-framed footer already gives the
    baseline's own "no summary/statistics in MCAP" verdict.
    """
    if size < _FOOTER_LEN:
        raise TargetedUnavailable("too small for an MCAP footer")
    tail_start = max(0, size - tail)
    buf_tail = pread(tail_start, size - 1)
    if len(buf_tail) != size - tail_start:
        raise TargetedUnavailable("short tail read (stale listing size?)")
    if buf_tail[-8:] != MAGIC:
        raise TargetedUnavailable("bad trailing magic")
    if tail_start == 0 and buf_tail[:8] != MAGIC:
        raise TargetedUnavailable("bad leading magic")  # zero-cost when visible
    footer = buf_tail[-_FOOTER_LEN:-8]
    if footer[0] != Opcode.FOOTER:
        raise TargetedUnavailable("footer opcode mismatch")
    (footer_len,) = struct.unpack("<Q", footer[1:9])
    if footer_len != _FOOTER_PAYLOAD:
        raise TargetedUnavailable("footer length field mismatch")
    summary_start, summary_offset_start, _crc = struct.unpack("<QQI", footer[9:29])
    if summary_start == 0:
        # A well-framed footer that plainly says "no summary" — this IS the
        # baseline's exact verdict, not a targeted-path limitation, so raise
        # it directly (a typed ValueError subclass, not TargetedUnavailable):
        # no need to pay for a second, streamed read just to get the same
        # answer. Typed (rather than a bare ValueError) so the wrapper can
        # narrowly distinguish this carve-out from an unrelated ValueError
        # a buggy/adversarial ``pread`` transport might raise.
        raise _NoSummary("no summary/statistics in MCAP")
    if summary_offset_start == 0:
        raise TargetedUnavailable("no summary-offset section")

    footer_pos = size - _FOOTER_LEN
    if not summary_start <= summary_offset_start < footer_pos:
        raise TargetedUnavailable("summary-offset section out of bounds")
    if footer_pos - summary_offset_start > _MAX_OFFSETS_LEN:
        raise TargetedUnavailable("summary-offset section implausibly large")
    if summary_offset_start >= tail_start:
        so_buf = buf_tail[summary_offset_start - tail_start:footer_pos - tail_start]
    else:
        # Live only for a shrunk (non-default) ``tail`` — DEFAULT_TAIL covers
        # the offsets section on its own in every default-tail test.
        so_buf = pread(summary_offset_start, footer_pos - 1)
        if len(so_buf) != footer_pos - summary_offset_start:
            raise TargetedUnavailable("short summary-offset read")

    groups: dict[int, tuple[int, int]] = {}
    for op, rec in _iter_records(so_buf):
        if op == Opcode.SUMMARY_OFFSET and rec is not None:
            groups[rec.group_opcode] = (rec.group_start, rec.group_length)
    if Opcode.STATISTICS not in groups:
        raise TargetedUnavailable("no statistics group in summary offsets")

    want = sorted(
        groups[op] for op in (Opcode.SCHEMA, Opcode.CHANNEL, Opcode.STATISTICS)
        if op in groups
    )
    merged: list[list[int]] = []
    for start, length in want:
        # Every needed group must lie wholly inside the summary section — a
        # corrupt footer/offset must never trigger a body read (R2).
        if not (summary_start <= start and start + length <= summary_offset_start):
            raise TargetedUnavailable("summary group outside summary section")
        if merged and start - merged[-1][1] <= _COALESCE_GAP:
            merged[-1][1] = max(merged[-1][1], start + length)
        else:
            merged.append([start, start + length])

    schemas: dict[int, Schema] = {}
    channels: dict[int, Channel] = {}   # id-keyed: duplicates overwrite (baseline dict semantics)
    stats: Statistics | None = None
    for lo, hi in merged:
        if lo >= tail_start:
            buf = buf_tail[lo - tail_start:hi - tail_start]
        else:
            buf = pread(lo, hi - 1)
            if len(buf) != hi - lo:
                raise TargetedUnavailable("short group read")
        for op, rec in _iter_records(buf):
            if op == Opcode.SCHEMA and rec is not None:
                schemas[rec.id] = rec
            elif op == Opcode.CHANNEL and rec is not None:
                channels[rec.id] = rec
            elif op == Opcode.STATISTICS and rec is not None:
                stats = rec
    if stats is None:
        raise TargetedUnavailable("statistics record missing from its group")
    if len(channels) != stats.channel_count or len(schemas) != stats.schema_count:
        raise TargetedUnavailable("schema/channel count mismatch vs statistics")

    counts = stats.channel_message_counts
    return FileSummary(
        start_time_ns=stats.message_start_time,
        end_time_ns=stats.message_end_time,
        message_count=stats.message_count,
        chunk_count=stats.chunk_count,
        channels=[
            ChannelInfo(
                channel_id=ch.id,
                topic=ch.topic,
                schema_name=schemas[ch.schema_id].name if ch.schema_id in schemas else "",
                schema_encoding=schemas[ch.schema_id].encoding
                if ch.schema_id in schemas else "",
                message_count=counts.get(ch.id, 0),  # zero-msg channels absent: .get MANDATORY
            )
            for ch in channels.values()
        ],
    )


def read_summary_with_fallback(
    pread: Callable[[int, int], bytes],
    open_stream: Callable[[], BinaryIO],
    key: str,
    size: int,
    tail: int = DEFAULT_TAIL,
) -> tuple[FileSummary, str]:
    """Targeted read with streamed fallback; returns ``(summary, via)``.

    ``via`` ∈ {"targeted", "fallback-unavailable", "fallback-unexpected"} feeds
    the reconcile disposition counters (status sidecar) — a systematic targeted
    bug must be VISIBLE, not a silent slow path. When the FALLBACK itself
    raises (the quarantine path), the disposition is preserved by stamping it
    onto the propagating exception as ``summary_via`` so failed files still
    count (review Q2: telemetry must not undercount on the error path).
    """
    try:
        return read_summary_targeted(pread, size, tail=tail), "targeted"
    except _NoSummary:
        # The carve-out: a well-framed footer already gave the baseline's
        # exact verdict — propagate it verbatim, no fallback re-read needed.
        # Narrowly typed (not a bare ValueError) so a ValueError raised by a
        # buggy/adversarial ``pread`` transport does NOT take this shortcut —
        # it falls through to the generic handler below and gets a real
        # streamed-fallback verdict instead.
        raise
    except TargetedUnavailable as e:
        # Demoted to DEBUG (2026-07-29 review): at million-object scale a
        # per-file INFO line here is noise — the sidecar's aggregate
        # summary_fallbacks_unavailable counter (status.ReconcileProgress) is
        # the operator-facing signal for this path. The fallback-unexpected
        # WARNING below stays per-file: that path suggests an actual bug in
        # the targeted reader, which deserves immediate visibility.
        logger.debug("targeted summary read unavailable for %s (%s); using streamed read",
                     key, e)
        via = "fallback-unavailable"
    except Exception as e:  # noqa: BLE001 — a targeted bug must never change verdicts
        logger.warning("targeted summary read failed for %s (%s: %s); using streamed read",
                       key, type(e).__name__, e)
        via = "fallback-unexpected"
    try:
        with open_stream() as stream:
            return summary_from_stream(stream), via
    except Exception as e:  # noqa: BLE001 — preserve disposition on the quarantine path
        try:
            e.summary_via = via
        except Exception:  # noqa: BLE001 — exotic exception without a __dict__
            pass
        raise
