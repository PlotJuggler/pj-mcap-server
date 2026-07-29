"""Parity + adversarial tests for the targeted summary read."""
import struct
from pathlib import Path

import pytest

from mcap_catalog_builder.mcap_summary import summary_from_stream
from mcap_catalog_builder.targeted_summary import (
    DEFAULT_TAIL,
    TargetedUnavailable,
    read_summary_targeted,
    read_summary_with_fallback,
)
from .fixtures import write_minimal_mcap

MAGIC = b"\x89MCAP0\r\n"


class PreadRecorder:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.calls: list[tuple[int, int]] = []

    def __call__(self, lo: int, hi: int) -> bytes:
        self.calls.append((lo, hi))
        return self.data[lo:hi + 1]


def _mcap_bytes(tmp_path, **kw) -> bytes:
    p = tmp_path / "f.mcap"
    write_minimal_mcap(str(p), **kw)
    return p.read_bytes()


def _baseline(data: bytes):
    import io
    return summary_from_stream(io.BytesIO(data))


def _open(data):
    import io
    return io.BytesIO(data)


def _patch_footer(data: bytes, *, length=None, summary_start=None,
                  summary_offset_start=None) -> bytes:
    # trailing 37 bytes: op(1)+len(8)+summary_start(8)+summary_offset_start(8)+crc(4)+magic(8)
    assert data[-8:] == MAGIC and data[-37] == 0x02
    b = bytearray(data)
    if length is not None:
        b[-36:-28] = struct.pack("<Q", length)
    if summary_start is not None:
        b[-28:-20] = struct.pack("<Q", summary_start)
    if summary_offset_start is not None:
        b[-20:-12] = struct.pack("<Q", summary_offset_start)
    return bytes(b)


# --- parity -----------------------------------------------------------------

def test_parity_minimal(tmp_path):
    data = _mcap_bytes(tmp_path)
    got, via = read_summary_with_fallback(
        PreadRecorder(data), lambda: _open(data), "k", len(data))
    assert got == _baseline(data) and via == "targeted"


def test_parity_many_channels_zero_message(tmp_path):
    chans = [(f"/t{i}", "std_msgs/msg/String", "ros2msg", i % 5) for i in range(40)]
    data = _mcap_bytes(tmp_path, channels=chans)
    got = read_summary_targeted(PreadRecorder(data), len(data))
    assert got == _baseline(data)
    assert any(c.message_count == 0 for c in got.channels)


def test_single_read_when_file_fits_tail(tmp_path):
    data = _mcap_bytes(tmp_path)
    assert len(data) < DEFAULT_TAIL
    pread = PreadRecorder(data)
    read_summary_targeted(pread, len(data))
    assert len(pread.calls) == 1


def test_second_read_when_groups_before_tail(tmp_path):
    chans = [(f"/topic{i}", f"pkg/msg/T{i}", "ros2msg", 2) for i in range(30)]
    data = _mcap_bytes(tmp_path, channels=chans)
    tail = 2048
    assert len(data) > tail
    pread = PreadRecorder(data)
    assert read_summary_targeted(pread, len(data), tail=tail) == _baseline(data)
    assert len(pread.calls) >= 2


# --- adversarial: every corruption must be UNAVAILABLE (fallback decides),
# --- except the framed-and-valid no-summary case ----------------------------

def test_no_summary_error_parity(tmp_path):
    data = _patch_footer(_mcap_bytes(tmp_path), summary_start=0)
    with pytest.raises(ValueError, match="no summary/statistics"):
        read_summary_targeted(PreadRecorder(data), len(data))
    with pytest.raises(ValueError, match="no summary/statistics"):
        _baseline(data)


def test_truly_unsummarized_bytes(tmp_path):
    # Hand-built MCAP with no summary section at all: magic+header+data_end+
    # footer(0,0)+magic. Both paths must agree (via fallback for targeted).
    def rec(op, payload):
        return bytes([op]) + struct.pack("<Q", len(payload)) + payload
    header = rec(0x01, struct.pack("<I", 0) + struct.pack("<I", 0))  # empty strings
    data_end = rec(0x0F, struct.pack("<I", 0))
    footer = rec(0x02, struct.pack("<QQI", 0, 0, 0))
    data = MAGIC + header + data_end + footer + MAGIC
    with pytest.raises(ValueError, match="no summary/statistics"):
        read_summary_with_fallback(PreadRecorder(data), lambda: _open(data),
                                   "k", len(data))


@pytest.mark.parametrize("mutate", [
    lambda d: _patch_footer(d, length=21),                 # corrupt footer length
    lambda d: _patch_footer(d, summary_offset_start=5),    # offsets before summary
    lambda d: _patch_footer(d, summary_offset_start=len(d)),  # offsets past footer
    lambda d: d[:-8] + b"NOTMAGIC",                        # corrupt trailing magic
])
def test_corrupt_footer_is_unavailable(tmp_path, mutate):
    data = mutate(_mcap_bytes(tmp_path))
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(data), len(data))


def _corrupt_group_start_below_summary(data: bytearray, target_opcodes=(0x03, 0x04)):
    """Scan the summary-offset section for a SCHEMA(0x03)/CHANNEL(0x04) group
    and rewrite its ``group_start`` to point at byte 8 (just past the leading
    magic — squarely in the header/data section, below ``summary_start``),
    keeping ``group_length`` as-is (a plausible length, just relocated).
    Records here are framed op(1) + len u64(8) + payload; a SummaryOffset
    payload is group_opcode u8(1) + group_start u64(8) + group_length u64(8).
    Returns ``(summary_start, summary_offset_start)`` for the caller.
    """
    footer = bytes(data[-37:-8])
    summary_start, summary_offset_start, _crc = struct.unpack("<QQI", footer[9:29])
    footer_pos = len(data) - 37
    pos = summary_offset_start
    while pos < footer_pos:
        op = data[pos]
        (length,) = struct.unpack_from("<Q", data, pos + 1)
        payload_off = pos + 9
        if op == 0x0E and data[payload_off] in target_opcodes:  # SUMMARY_OFFSET
            struct.pack_into("<Q", data, payload_off + 1, 8)  # group_start := 8
            return summary_start, summary_offset_start
        pos = payload_off + length
    raise AssertionError("no SCHEMA/CHANNEL SummaryOffset record found to corrupt")


def test_corrupt_group_start_below_summary_never_reads_body(tmp_path):
    # Pins BOUNDEDNESS ITSELF (not just this guard's specific message): a
    # SummaryOffset record whose group_start is corrupted to point below
    # summary_start (into the header/data section) must be rejected WITHOUT
    # ever issuing a fetch for that out-of-bounds range — removing the
    # "summary group outside summary section" guard must make this test fail
    # via the boundedness assertion even if some other check happens to still
    # raise TargetedUnavailable.
    chans = [(f"/topic{i}", f"pkg/msg/T{i}", "ros2msg", 2) for i in range(30)]
    data = bytearray(_mcap_bytes(tmp_path, channels=chans))
    tail = 2048
    assert len(data) > tail
    summary_start, _summary_offset_start = _corrupt_group_start_below_summary(data)
    data = bytes(data)

    pread = PreadRecorder(data)
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(pread, len(data), tail=tail)
    for lo, _hi in pread.calls[1:]:  # calls[0] is the initial tail read (exempt)
        assert lo >= summary_start


def test_corrupt_leading_magic_small_file_unavailable(tmp_path):
    # File fits in the tail -> offset 0 is visible -> leading magic IS checked.
    data = _mcap_bytes(tmp_path)
    assert len(data) < DEFAULT_TAIL
    bad = b"XXXXXXXX" + data[8:]
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(bad), len(bad))


def test_corrupt_leading_magic_large_file_is_cataloged(tmp_path):
    # THE ACCEPTED Q1 RELAXATION, pinned: head outside the tail -> targeted
    # parses fine where the streamed path errors. The Go FormatCodec still
    # rejects such a file at stream-open (server/internal/format/format.go).
    chans = [(f"/t{i}", "pkg/msg/T", "ros2msg", 5) for i in range(60)]
    data = _mcap_bytes(tmp_path, channels=chans)
    tail = 1024
    assert len(data) > tail
    bad = b"XXXXXXXX" + data[8:]
    got = read_summary_targeted(PreadRecorder(bad), len(bad), tail=tail)
    assert got == _baseline(data)  # baseline on the UNCORRUPTED bytes
    with pytest.raises(Exception):
        _baseline(bad)  # streamed path rejects the corrupt head


def test_stale_size_short_tail_unavailable(tmp_path):
    data = _mcap_bytes(tmp_path)
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(data), len(data) + 512)


def test_tiny_input_unavailable():
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(b"xx"), 2)


def test_fallback_dispositions(tmp_path, caplog):
    import logging
    data = _patch_footer(_mcap_bytes(tmp_path), summary_offset_start=0)  # no offsets
    with caplog.at_level(logging.INFO):
        got, via = read_summary_with_fallback(
            PreadRecorder(data), lambda: _open(data), "k", len(data))
    assert via == "fallback-unavailable"
    assert got == _baseline(data)

    def boom(lo, hi):
        raise RuntimeError("targeted bug")
    good = _mcap_bytes(tmp_path)
    with caplog.at_level(logging.WARNING):
        got, via = read_summary_with_fallback(boom, lambda: _open(good), "k", len(good))
    assert via == "fallback-unexpected" and got == _baseline(good)


def test_transport_valueerror_does_not_take_carveout_shortcut(tmp_path):
    # A ValueError raised by the PREAD TRANSPORT ITSELF (not the well-framed
    # summary_start==0 carve-out) must NOT be mistaken for that carve-out: it
    # has to fall through to the generic handler and get a real
    # streamed-fallback verdict, not propagate verbatim past the fallback and
    # past the .summary_via stamping (which would silently mis-quarantine a
    # perfectly healthy file on a transient/buggy transport error).
    good = _mcap_bytes(tmp_path)

    def bad_pread(lo, hi):
        raise ValueError("transport bug")

    got, via = read_summary_with_fallback(
        bad_pread, lambda: _open(good), "k", len(good))
    assert via == "fallback-unexpected"
    assert got == _baseline(good)


def test_garbage_fallback_error_parity(tmp_path):
    data = b"\x00" * 4096
    base_exc = None
    try:
        _baseline(data)
    except Exception as e:  # noqa: BLE001
        base_exc = e
    with pytest.raises(type(base_exc)):
        read_summary_with_fallback(PreadRecorder(data), lambda: _open(data),
                                   "k", len(data))


def test_fallback_failure_stamps_disposition(tmp_path):
    # When the STREAMED fallback itself raises, the propagating exception must
    # carry .summary_via so quarantined files still land in the counters.
    data = b"\x00" * 4096
    try:
        read_summary_with_fallback(PreadRecorder(data), lambda: _open(data),
                                   "k", len(data))
    except Exception as e:  # noqa: BLE001
        assert e.summary_via == "fallback-unavailable"
    else:
        pytest.fail("expected the fallback to raise on garbage bytes")


# --- statistics count cross-check: directional (2026-07-29 census) ----------

def _patch_stats_counts(data: bytes, *, schema_count=None, channel_count=None) -> bytes:
    """Rewrite Statistics.schema_count/channel_count inside the stats group.

    Payload layout: message_count u64, schema_count u16, channel_count u32, ...
    """
    import io as _io
    from mcap.data_stream import ReadDataStream
    from mcap.opcode import Opcode
    from mcap.records import SummaryOffset

    footer = data[-37:-8]
    _summary_start, so_start, _crc = struct.unpack("<QQI", footer[9:29])
    so_buf = data[so_start:len(data) - 37]
    stats_start = None
    stream = ReadDataStream(_io.BytesIO(so_buf))
    while stream.count < len(so_buf):
        op = stream.read1()
        length = stream.read8()
        if op == Opcode.SUMMARY_OFFSET:
            rec = SummaryOffset.read(stream)
            if rec.group_opcode == Opcode.STATISTICS:
                stats_start = rec.group_start
        else:
            stream.read(length)
    assert stats_start is not None
    payload = stats_start + 9  # opcode(1) + length(8)
    b = bytearray(data)
    if schema_count is not None:
        b[payload + 8:payload + 10] = struct.pack("<H", schema_count)
    if channel_count is not None:
        b[payload + 10:payload + 14] = struct.pack("<I", channel_count)
    return bytes(b)


def test_statistics_underreport_is_accepted_with_parity(tmp_path):
    # Real-fleet writer bug (arri-86 census 2026-07-29, 941/25.5k files):
    # Statistics claims FEWER schemas/channels than the summary groups hold.
    # The streamed baseline ignores those counts and catalogs every summary
    # record — the targeted path must do the same, not fall back.
    chans = [(f"/t{i}", f"pkg/msg/T{i % 2}", "ros2msg", 2) for i in range(5)]
    data = _mcap_bytes(tmp_path, channels=chans)
    patched = _patch_stats_counts(data, schema_count=1, channel_count=2)
    got = read_summary_targeted(PreadRecorder(patched), len(patched))
    assert got == _baseline(patched)
    assert len(got.channels) == 5


def test_statistics_overreport_still_falls_back(tmp_path):
    # The dangerous direction: parsing FEWER records than Statistics claims
    # means a missing/truncated group — must stay TargetedUnavailable.
    data = _mcap_bytes(tmp_path, channels=[("/a", "p/msg/A", "ros2msg", 1),
                                           ("/b", "p/msg/B", "ros2msg", 1)])
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(
            PreadRecorder(_patch_stats_counts(data, channel_count=7)),
            len(data))
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(
            PreadRecorder(_patch_stats_counts(data, schema_count=9)),
            len(data))
