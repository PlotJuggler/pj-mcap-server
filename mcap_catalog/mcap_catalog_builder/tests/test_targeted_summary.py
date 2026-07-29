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
