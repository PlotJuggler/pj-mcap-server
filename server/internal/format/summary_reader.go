// summary_reader.go — the WAN-aware read path for MCAP footer/summary parsing.
//
// The naive storage.ReaderAt turns EVERY ReadAt into one ranged GET; foxglove's
// Info() then walks the summary in 4KiB reads and the message-index counting
// adds 2 GETs per (chunk x channel). Against a real bucket (~tens of ms RTT per
// GET) that made one OpenSession plan-build take MINUTES per file (measured
// 3m24s on the Dexory staging bucket) versus a 10s client timeout, and a 34-file
// warm scan take ~6 minutes. summarySource collapses the whole footer/summary
// phase into at most THREE ranged GETs (trailing probe, summary section, magic
// prefix) and serves everything else from memory; reads outside the cached
// windows (e.g. Metadata records) fall through to one GET each.
package format

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"

	"pj-cloud/server/internal/storage"
)

const (
	mcapMagicLen = 8
	// footer record framing: 1-byte opcode + 8-byte length + 20-byte body
	// (summary_start, summary_offset_start, summary_crc).
	footerBodyLen   = 20
	footerRecordLen = recordHeaderLen + footerBodyLen
	// tailProbeLen reads the footer (+ any small summary) in the first GET; a
	// larger summary then needs ONE exact [summary_start, EOF) read. Kept small
	// (NOT sized to cover big summaries) because over-reading is a net loss on a
	// bandwidth-bound WAN link — fetching the exact summary in a second GET
	// transfers fewer bytes than one oversized probe.
	tailProbeLen = 64 * 1024
	// headProbeLen is prefetched at offset 0 so mcap.NewReader's magic + header
	// reads (a handful of tiny reads at the file START) are served from memory
	// instead of one ranged GET each (3 reads ~= 200 ms of pure WAN latency for
	// ~40 bytes). The MCAP header record is tiny; 4 KiB is ample.
	headProbeLen = 4 * 1024
)

// span is one cached byte range of the object.
type span struct {
	off  int64
	data []byte
}

func (s span) contains(off, n int64) bool {
	return off >= s.off && off+n <= s.off+int64(len(s.data))
}

// summarySource is an io.ReadSeeker/io.ReaderAt over a blob that serves the
// MCAP footer/summary region from prefetched memory. It intentionally mirrors
// storage.ReaderAt's seek semantics so mcap.NewReader treats them identically.
type summarySource struct {
	ctx   context.Context
	bs    storage.BlobStore
	key   string
	size  int64
	pos   int64
	spans []span // cached windows, checked in order
}

// newSummarySource prefetches the footer (+ summary section) of the MCAP object
// at key. It fails fast on files without a summary section instead of letting
// the reader degrade to a full linear scan over ranged reads.
func newSummarySource(ctx context.Context, bs storage.BlobStore, key string, size int64) (*summarySource, error) {
	if size < int64(footerRecordLen+2*mcapMagicLen) {
		return nil, fmt.Errorf("format: %q too small to be an mcap file (%d bytes)", key, size)
	}

	tailLen := int64(tailProbeLen)
	if tailLen > size {
		tailLen = size
	}
	tailOff := size - tailLen
	tail, err := bs.GetRange(ctx, key, tailOff, tailLen)
	if err != nil {
		return nil, fmt.Errorf("format: read tail of %q: %w", key, err)
	}
	if int64(len(tail)) != tailLen {
		return nil, fmt.Errorf("format: short tail read of %q: got %d want %d", key, len(tail), tailLen)
	}

	// Validate the trailing magic, then parse the footer record right before it.
	magic := tail[len(tail)-mcapMagicLen:]
	if string(magic) != "\x89MCAP0\r\n" {
		return nil, fmt.Errorf("format: %q has no trailing MCAP magic", key)
	}
	footerHdr := len(tail) - mcapMagicLen - footerRecordLen
	if tail[footerHdr] != byte(mcap.OpFooter) {
		return nil, fmt.Errorf("format: %q footer record not found (op=0x%02x)", key, tail[footerHdr])
	}
	if got := binary.LittleEndian.Uint64(tail[footerHdr+1 : footerHdr+recordHeaderLen]); got != footerBodyLen {
		return nil, fmt.Errorf("format: %q footer record has unexpected length %d", key, got)
	}
	summaryStart := int64(binary.LittleEndian.Uint64(tail[footerHdr+recordHeaderLen : footerHdr+recordHeaderLen+8]))
	if summaryStart == 0 {
		// No summary section: reject here — same contract as the indexed reader,
		// but without risking a ranged-read linear scan of the whole object.
		return nil, fmt.Errorf("format: mcap %q has no summary section (not summarized)", key)
	}
	if summaryStart < 0 || summaryStart >= size {
		return nil, fmt.Errorf("format: mcap %q has invalid summary_start %d (size %d)", key, summaryStart, size)
	}

	src := &summarySource{ctx: ctx, bs: bs, key: key, size: size}
	if summaryStart >= tailOff {
		// The probe already covers the whole summary section.
		src.spans = append(src.spans, span{off: tailOff, data: tail})
	} else {
		// ONE more GET for the full [summary_start, EOF) window.
		full, err := bs.GetRange(ctx, key, summaryStart, size-summaryStart)
		if err != nil {
			return nil, fmt.Errorf("format: read summary section of %q: %w", key, err)
		}
		if int64(len(full)) != size-summaryStart {
			return nil, fmt.Errorf("format: short summary read of %q: got %d want %d", key, len(full), size-summaryStart)
		}
		src.spans = append(src.spans, span{off: summaryStart, data: full})
	}

	// Prefetch the file HEAD so mcap.NewReader's magic + header reads (tiny
	// reads at offset 0) hit memory instead of one ranged GET each. Skip when
	// the tail probe already reached offset 0 (small file).
	if tailOff > 0 {
		headLen := int64(headProbeLen)
		if headLen > tailOff {
			headLen = tailOff
		}
		head, err := bs.GetRange(ctx, key, 0, headLen)
		if err != nil {
			return nil, fmt.Errorf("format: read head of %q: %w", key, err)
		}
		// Prepend so it is checked first (NewReader reads offset 0 immediately).
		src.spans = append([]span{{off: 0, data: head}}, src.spans...)
	}
	return src, nil
}

func (s *summarySource) ReadAt(p []byte, off int64) (int, error) {
	if off >= s.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > s.size {
		want = s.size - off
	}
	for _, sp := range s.spans {
		if sp.contains(off, want) {
			n := copy(p, sp.data[off-sp.off:off-sp.off+want])
			if int64(n) < int64(len(p)) {
				return n, io.EOF
			}
			return n, nil
		}
	}
	// Outside the cached windows (header magic, Metadata records, ...): one GET.
	data, err := s.bs.GetRange(s.ctx, s.key, off, want)
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	if int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

func (s *summarySource) Read(p []byte) (int, error) {
	n, err := s.ReadAt(p, s.pos)
	s.pos += int64(n)
	return n, err
}

func (s *summarySource) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.pos + offset
	case io.SeekEnd:
		abs = s.size + offset
	default:
		return 0, fmt.Errorf("format: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("format: negative position")
	}
	s.pos = abs
	return abs, nil
}
