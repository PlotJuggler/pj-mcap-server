// Command mcaptopics reads an on-disk MCAP file and prints per-topic message
// counts (and schema name/encoding) as JSON, plus the total message count and
// topic count. It is a Slice-9 verification aid: ground truth for the
// "GetFile on a scale-bucket copy == the original's topic counts" assertion.
//
// It uses the same foxglove/mcap reader as mcapdiff, counting messages by
// channel topic so the numbers are derived from the actual records (not the
// summary statistics), making it a strong cross-check of the server's catalog
// extraction.
//
// Usage:
//
//	mcaptopics [--monotonic] FILE.mcap
//
// With --monotonic it additionally asserts the messages are non-decreasing in
// log_time when read in FILE order (as written), which proves a stitched
// multi-file reconstruction wrote a globally time-ordered stream across the
// seams (the matrix m5 monotonic check). It exits 1 if a non-monotonic step is
// found, 0 otherwise.
//
// Exit codes: 0 ok, 1 non-monotonic (with --monotonic), 2 usage / I/O error.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
)

type topicCount struct {
	Name         string `json:"name"`
	SchemaName   string `json:"schema_name"`
	SchemaEnc    string `json:"schema_encoding"`
	MessageCount uint64 `json:"message_count"`
}

type report struct {
	Path         string       `json:"path"`
	TopicCount   int          `json:"topic_count"`
	MessageCount uint64       `json:"message_count"`
	Topics       []topicCount `json:"topics"`
}

func main() {
	os.Exit(run())
}

func run() int {
	monotonic := flag.Bool("monotonic", false, "also assert log_time is non-decreasing in file (as-written) order; exit 1 if not")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, "usage: mcaptopics [--monotonic] FILE.mcap") }
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return 2
	}
	path := flag.Arg(0)
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcaptopics: open %q: %v\n", path, err)
		return 2
	}
	defer f.Close()

	rdr, err := mcap.NewReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcaptopics: reader %q: %v\n", path, err)
		return 2
	}
	defer rdr.Close()
	// Default Order is FileOrder (as written) — exactly what the monotonic check
	// needs to validate the reconstruction's native write order across seams.
	it, err := rdr.Messages()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcaptopics: messages %q: %v\n", path, err)
		return 2
	}

	counts := map[string]*topicCount{}
	var total uint64
	var lastLogTime uint64
	var haveLast bool
	nonMonotonic := false
	for {
		sch, ch, msg, err := it.NextInto(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "mcaptopics: iterate %q: %v\n", path, err)
			return 2
		}
		if msg == nil {
			break
		}
		if *monotonic {
			if haveLast && msg.LogTime < lastLogTime {
				fmt.Fprintf(os.Stderr, "mcaptopics: non-monotonic log_time at record %d: %d < %d (topic %q)\n",
					total, msg.LogTime, lastLogTime, ch.Topic)
				nonMonotonic = true
			}
			lastLogTime = msg.LogTime
			haveLast = true
		}
		tc := counts[ch.Topic]
		if tc == nil {
			tc = &topicCount{Name: ch.Topic}
			if sch != nil {
				tc.SchemaName = sch.Name
				tc.SchemaEnc = sch.Encoding
			}
			counts[ch.Topic] = tc
		}
		tc.MessageCount++
		total++
	}
	if *monotonic && nonMonotonic {
		return 1
	}

	rep := report{Path: path, MessageCount: total}
	for _, tc := range counts {
		rep.Topics = append(rep.Topics, *tc)
	}
	sort.Slice(rep.Topics, func(i, j int) bool { return rep.Topics[i].Name < rep.Topics[j].Name })
	rep.TopicCount = len(rep.Topics)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintf(os.Stderr, "mcaptopics: encode: %v\n", err)
		return 2
	}
	return 0
}
