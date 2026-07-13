// Command mcapdiff is the round-trip correctness gate for the PJ Cloud
// Connector: it compares an ORIGINAL MCAP against a RECONSTRUCTED MCAP at the
// level of message-logical equality, NOT container-byte equality.
//
// Logical equality (per design-spec §11 line 833 / unified-plan §6 L3 / Plan C
// Task 4): for every (topic, log_time) record — sorted — the two files must
// agree on payload bytes, publish_time, and the originating schema's
// name/encoding/data. MCAP writers may legitimately differ on chunking, summary
// ordering, and chunk compression, so a byte diff of the containers would give
// false negatives; we compare what the wire protocol actually promises.
//
// Because the connector forwards RAW MCAP message records to the client (no
// decode on the wire) and the client's writer re-serializes them verbatim, a
// faithful logical round-trip is the natural outcome. A mismatch means a
// wire-format / protocol / decode bug between server and client.
//
// Subset support. For topic-subset or time-range downloads the reconstructed
// file legitimately contains FEWER records than the original. Pass the SAME
// --topics / --time-range selection used for the download and mcapdiff filters
// the ORIGINAL down to that selection before comparing — otherwise a correct
// subset reads as a length mismatch. It ALSO asserts the reconstructed file
// over-delivered NOTHING outside the requested selection (the affirmative half
// of the filtering gate: a mis-sliced or fabricated batch-body record).
//
// Stitched multi-file. The LAST positional is the rebuilt file; all preceding
// positionals are originals (one or more). When several originals are given they
// are MERGED — their records are concatenated and globally re-sorted by
// (topic, log_time) — into one logical set before the compare. This is exactly
// what a stitched reconstruction (Slice 7: N consecutive MCAPs presented as one
// continuous logical session) must equal. A single-original invocation is
// byte-identical to the prior two-arg behavior (merge of one file == that file).
//
// Usage:
//
//	mcapdiff [--topics a,b] [--time-range startNs,endNs] ORIGINAL... REBUILT.mcap
//
// Exit codes:
//
//	0  logically equal (within the requested selection) and no over-delivery
//	1  one or more mismatches (printed to stdout) and/or over-delivery
//	2  usage / I/O error (printed to stderr)
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		topicsCSV = flag.String("topics", "", "comma-separated topic subset the download requested (empty = all topics)")
		timeRange = flag.String("time-range", "", "optional 'startNs,endNs' half-open window the download requested (empty = no time filter)")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"usage: mcapdiff [--topics a,b] [--time-range startNs,endNs] ORIGINAL... REBUILT.mcap\n\n"+
				"Logical-equality diff (topic,log_time -> payload,publish_time,schema) between one or\n"+
				"more original MCAPs and a reconstructed one. Multiple originals are merged (records\n"+
				"concatenated + re-sorted by topic,log_time) into one logical set before compare — the\n"+
				"stitched-reconstruction gate. For subset downloads pass the same --topics/--time-range;\n"+
				"the merged original is filtered to that selection before compare and the rebuilt file\n"+
				"is checked for over-delivery outside the selection.\n\n"+
				"exit: 0 clean, 1 mismatches/over-delivery, 2 usage/IO error\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		return 2
	}
	// The LAST positional is the rebuilt file; everything before it is an
	// original (one or more).
	rebuiltPath := flag.Arg(flag.NArg() - 1)
	origPaths := flag.Args()[:flag.NArg()-1]

	topics, err := parseTopics(*topicsCSV)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcapdiff: %v\n", err)
		return 2
	}
	startNs, endNs, err := parseTimeRange(*timeRange)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcapdiff: %v\n", err)
		return 2
	}

	// Read + MERGE all originals into one logical set: concatenate every file's
	// records, then re-sort globally by (topic, log_time) — the stitched logical
	// set a correct multi-file reconstruction must equal.
	var origAll []Record
	for _, p := range origPaths {
		recs, err := collectFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mcapdiff: read original %q: %v\n", p, err)
			return 2
		}
		origAll = append(origAll, recs...)
	}
	sortRecords(origAll)

	rebuilt, err := collectFile(rebuiltPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcapdiff: read rebuilt %q: %v\n", rebuiltPath, err)
		return 2
	}

	// Filter the merged ORIGINAL set to the requested selection so a correct
	// subset download compares clean against the filtered original.
	orig := filter(origAll, topics, startNs, endNs)

	mism := compare(orig, rebuilt)
	over := overDelivered(rebuilt, topics, startNs, endNs)

	selDesc := selectionDesc(*topicsCSV, topics, startNs, endNs)
	fmt.Printf("mcapdiff: original(merged)=%d file(s) %q rebuilt=%q\n", len(origPaths), origPaths, rebuiltPath)
	fmt.Printf("  selection: %s\n", selDesc)
	fmt.Printf("  records: original(merged,filtered)=%d rebuilt=%d\n", len(orig), len(rebuilt))

	if len(mism) == 0 && len(over) == 0 {
		fmt.Println("  OK: logically equal, no over-delivery")
		return 0
	}

	if len(mism) > 0 {
		fmt.Printf("  MISMATCHES (%d):\n", len(mism))
		const showMax = 50
		for i, m := range mism {
			if i >= showMax {
				fmt.Printf("    ... and %d more\n", len(mism)-showMax)
				break
			}
			fmt.Printf("    [%d] %s\n", m.Index, describe(m))
		}
	}
	if len(over) > 0 {
		fmt.Printf("  OVER-DELIVERY (%d records outside the requested selection):\n", len(over))
		const showMax = 50
		for i, r := range over {
			if i >= showMax {
				fmt.Printf("    ... and %d more\n", len(over)-showMax)
				break
			}
			fmt.Printf("    topic=%q log_time=%d (payload %d bytes)\n", r.Topic, r.LogTime, len(r.Data))
		}
	}
	return 1
}

// Record is one MCAP message reduced to the fields the wire protocol promises
// to preserve. Records are compared after sorting by (Topic, LogTime).
type Record struct {
	Topic       string
	LogTime     uint64
	PublishTime uint64
	Data        []byte
	SchemaName  string
	SchemaEnc   string
	SchemaData  []byte
}

func collectFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return collect(f)
}

// collect reads every message from an MCAP and returns them sorted by
// (Topic, LogTime). Uses NextInto(nil) so each Record owns its payload copy.
func collect(rs io.ReadSeeker) ([]Record, error) {
	rdr, err := mcap.NewReader(rs)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	it, err := rdr.Messages()
	if err != nil {
		return nil, err
	}
	var out []Record
	for {
		sch, ch, msg, err := it.NextInto(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("iterate messages: %w", err)
		}
		if msg == nil {
			break
		}
		rec := Record{
			Topic:       ch.Topic,
			LogTime:     msg.LogTime,
			PublishTime: msg.PublishTime,
			Data:        append([]byte(nil), msg.Data...),
		}
		if sch != nil {
			rec.SchemaName = sch.Name
			rec.SchemaEnc = sch.Encoding
			rec.SchemaData = append([]byte(nil), sch.Data...)
		}
		out = append(out, rec)
	}
	sortRecords(out)
	return out, nil
}

// sortRecords orders records by (Topic, LogTime), stable so two records with an
// identical (topic, log_time) keep their input order on both sides of a compare.
// Used by collect() and by run() after merging multiple originals.
func sortRecords(recs []Record) {
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].Topic != recs[j].Topic {
			return recs[i].Topic < recs[j].Topic
		}
		return recs[i].LogTime < recs[j].LogTime
	})
}

// Mismatch is one disagreement between the two record slices at the given index
// (index into the sorted, aligned slices).
type Mismatch struct {
	Index int
	What  string
	Want  Record
	Got   Record
}

func compare(orig, rebuilt []Record) []Mismatch {
	var miss []Mismatch
	n := len(orig)
	if len(rebuilt) < n {
		n = len(rebuilt)
	}
	for i := 0; i < n; i++ {
		o, r := orig[i], rebuilt[i]
		if o.Topic != r.Topic {
			// A topic skew desynchronizes positional alignment; report and move
			// on (every subsequent index would otherwise be noise).
			miss = append(miss, Mismatch{i, "topic", o, r})
			continue
		}
		if o.LogTime != r.LogTime {
			miss = append(miss, Mismatch{i, "log_time", o, r})
		}
		if o.PublishTime != r.PublishTime {
			miss = append(miss, Mismatch{i, "publish_time", o, r})
		}
		if !bytes.Equal(o.Data, r.Data) {
			miss = append(miss, Mismatch{i, "payload", o, r})
		}
		if o.SchemaName != r.SchemaName {
			miss = append(miss, Mismatch{i, "schema_name", o, r})
		}
		if o.SchemaEnc != r.SchemaEnc {
			miss = append(miss, Mismatch{i, "schema_encoding", o, r})
		}
		if !bytes.Equal(o.SchemaData, r.SchemaData) {
			miss = append(miss, Mismatch{i, "schema_data", o, r})
		}
	}
	if len(orig) != len(rebuilt) {
		miss = append(miss, Mismatch{
			Index: n,
			What:  fmt.Sprintf("length: original(filtered)=%d rebuilt=%d", len(orig), len(rebuilt)),
		})
	}
	return miss
}

// filter narrows a record slice to a requested (topics, [startNs,endNs))
// selection. topics==nil disables topic filtering; endNs<=startNs disables time
// filtering (so the default "no flags" case is a no-op pass-through).
func filter(in []Record, topics map[string]bool, startNs, endNs uint64) []Record {
	if topics == nil && endNs <= startNs {
		return in
	}
	var out []Record
	for _, r := range in {
		if topics != nil && !topics[r.Topic] {
			continue
		}
		if endNs > startNs && (r.LogTime < startNs || r.LogTime >= endNs) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// overDelivered returns any reconstructed record OUTSIDE the requested
// selection — bytes the server should have filtered out. Must be empty.
func overDelivered(rebuilt []Record, topics map[string]bool, startNs, endNs uint64) []Record {
	var bad []Record
	for _, r := range rebuilt {
		if topics != nil && !topics[r.Topic] {
			bad = append(bad, r)
			continue
		}
		if endNs > startNs && (r.LogTime < startNs || r.LogTime >= endNs) {
			bad = append(bad, r)
		}
	}
	return bad
}

func describe(m Mismatch) string {
	switch m.What {
	case "topic":
		return fmt.Sprintf("topic: want %q got %q (log_time want=%d got=%d)", m.Want.Topic, m.Got.Topic, m.Want.LogTime, m.Got.LogTime)
	case "log_time":
		return fmt.Sprintf("log_time on topic %q: want %d got %d", m.Want.Topic, m.Want.LogTime, m.Got.LogTime)
	case "publish_time":
		return fmt.Sprintf("publish_time on topic %q log_time %d: want %d got %d", m.Want.Topic, m.Want.LogTime, m.Want.PublishTime, m.Got.PublishTime)
	case "payload":
		return fmt.Sprintf("payload on topic %q log_time %d: want %d bytes got %d bytes", m.Want.Topic, m.Want.LogTime, len(m.Want.Data), len(m.Got.Data))
	case "schema_name":
		return fmt.Sprintf("schema_name on topic %q: want %q got %q", m.Want.Topic, m.Want.SchemaName, m.Got.SchemaName)
	case "schema_encoding":
		return fmt.Sprintf("schema_encoding on topic %q: want %q got %q", m.Want.Topic, m.Want.SchemaEnc, m.Got.SchemaEnc)
	case "schema_data":
		return fmt.Sprintf("schema_data on topic %q: want %d bytes got %d bytes", m.Want.Topic, len(m.Want.SchemaData), len(m.Got.SchemaData))
	default:
		return m.What
	}
}

func parseTopics(csv string) (map[string]bool, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil, nil
	}
	m := map[string]bool{}
	for _, t := range strings.Split(csv, ",") {
		if t = strings.TrimSpace(t); t != "" {
			m[t] = true
		}
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// parseTimeRange parses "startNs,endNs" into a half-open [start,end) window.
// Empty string -> (0,0) which disables time filtering.
func parseTimeRange(csv string) (uint64, uint64, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return 0, 0, nil
	}
	parts := strings.SplitN(csv, ",", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("time-range must be 'startNs,endNs', got %q", csv)
	}
	start, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("time-range start: %w", err)
	}
	end, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("time-range end: %w", err)
	}
	if end < start {
		return 0, 0, fmt.Errorf("time-range end %d < start %d", end, start)
	}
	return start, end, nil
}

func selectionDesc(topicsCSV string, topics map[string]bool, startNs, endNs uint64) string {
	var b strings.Builder
	if topics == nil {
		b.WriteString("topics=all")
	} else {
		names := make([]string, 0, len(topics))
		for t := range topics {
			names = append(names, t)
		}
		sort.Strings(names)
		fmt.Fprintf(&b, "topics=[%s]", strings.Join(names, ","))
	}
	if endNs > startNs {
		fmt.Fprintf(&b, " time=[%d,%d)", startNs, endNs)
	} else {
		b.WriteString(" time=all")
	}
	return b.String()
}
