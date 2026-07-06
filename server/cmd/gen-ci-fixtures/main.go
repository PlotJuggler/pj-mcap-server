// Command gen-ci-fixtures writes the deterministic synthetic MCAP fixtures (see
// internal/genmcap) to an output directory, for the CI integration legs to seed
// into the Minio (S3) / fake-gcs (GCS) emulator buckets. It is a thin disk
// wrapper over internal/genmcap so the on-disk bytes and the harness's in-memory
// expectations come from the SAME FileSpec list — they cannot drift.
//
// Usage:
//
//	gen-ci-fixtures -out <dir>          # write the pinned fixture set
//	gen-ci-fixtures -out <dir> -manifest   # also print a JSON manifest to stdout
//
// The bytes are byte-identical across runs (no time.Now / no randomness), so a
// re-upload is a no-op for the indexer change-detect path — which the warm-start
// (0 re-extracts) assertion in the CI integration test relies on.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"pj-cloud/server/internal/genmcap"
)

// hiveKeyFor lays a spec out under a deterministic Hive-partitioned key
// (catalog-migration plan §5.1: dev == prod key shape). The dimensions are
// derived from the spec index so the set is byte-stable across runs AND exercises
// multiple robots/dates — enough for the auryn builder's dimension extraction and
// the cross-language read proof. The layout:
//
//	customer=test/customer_site=lab/robot=r{1|2}/source=synthetic/date=2026-06-2{2|3}/<spec.Key>
//
// 1 customer, 1 site, 2 robots, 1 source, 2 dates — strict hierarchy, varied leaves.
func hiveKeyFor(spec genmcap.FileSpec, i int) string {
	robot := fmt.Sprintf("r%d", i%2+1)
	date := "2026-06-22"
	if i%2 == 1 {
		date = "2026-06-23"
	}
	return filepath.Join(
		"customer=test",
		"customer_site=lab",
		"robot="+robot,
		"source=synthetic",
		"date="+date,
		spec.Key,
	)
}

// bigSpec is a single ADDITIONAL fixture (never part of DefaultSpecs — those
// are shared, pinned CI/ci-integration ground truth; perturbing their counts
// would ripple into unrelated gates). It exists for callers (the catalog-
// migration smoke harness) that need a file with enough VOLUME to force
// multiple WS session batches — DefaultSpecs' files are all well under the
// default 512KiB max_batch_bytes threshold, so a resume/reconnect test that
// forces a drop "after batch N>1" never finds enough batches to drop from.
// 3000 messages * 2048 bytes ≈ 6MiB, comfortably >10 size-triggered batches.
// Time-disjoint from every DefaultSpecs file (StartNs=8000s, past f_lz4's
// 6000.29s end) and from the 3D fixture (StartNs=1.7e18s).
func bigSpec() genmcap.FileSpec {
	const sec = 1_000_000_000
	return genmcap.FileSpec{
		Key:          "ci_synth_big.mcap",
		StartNs:      8000 * sec,
		StepNs:       1_000_000, // 1ms
		PayloadBytes: 2048,
		ChunkSize:    256 * 1024,
		Topics: []genmcap.TopicSpec{
			{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 500},
			{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 500},
			{Topic: "/imu", SchemaName: "sensor_msgs/msg/Imu", SchemaEnc: "ros2msg", MessageCount: 2000},
		},
	}
}

// bigHiveKey is bigSpec's FIXED Hive key — a distinct robot/date from every
// hiveKeyFor(spec, i) output so it never collides with the alternating
// DefaultSpecs layout.
const bigHiveKey = "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap"

func main() {
	out := flag.String("out", "", "output directory for the synthetic MCAP fixtures (required)")
	manifest := flag.Bool("manifest", false, "print a JSON manifest of the fixtures to stdout")
	hive := flag.Bool("hive", false, "lay fixtures out under Hive-partitioned keys "+
		"(customer=/customer_site=/robot=/source=/date=/<name>.mcap) so the auryn builder "+
		"extracts dimensions; default is the flat layout")
	hiveBig := flag.Bool("hive-big", false, "requires -hive: ALSO write bigSpec(), a "+
		"high-volume (~6MiB) fixture at a fixed Hive key for tests that need multiple "+
		"WS session batches (e.g. reconnect-resume). No-op without -hive.")
	flag.Parse()

	if *out == "" {
		fmt.Fprintln(os.Stderr, "gen-ci-fixtures: -out <dir> is required")
		os.Exit(2)
	}
	// S5: validate every flag combination BEFORE writing a single fixture (or
	// even creating -out) — a flag error must never leave a partially
	// populated (or newly created, empty) output directory behind.
	if *hiveBig && !*hive {
		fmt.Fprintln(os.Stderr, "gen-ci-fixtures: -hive-big requires -hive")
		os.Exit(2)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "gen-ci-fixtures: mkdir %q: %v\n", *out, err)
		os.Exit(1)
	}

	specs := genmcap.DefaultSpecs()
	type fileManifest struct {
		Key           string `json:"key"`
		StartNs       int64  `json:"start_ns"`
		EndNs         int64  `json:"end_ns"`
		TotalMessages uint64 `json:"total_messages"`
		Topics        int    `json:"topics"`
	}
	var mf []fileManifest

	writeOne := func(spec genmcap.FileSpec, rel string) {
		path := filepath.Join(*out, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "gen-ci-fixtures: mkdir %q: %v\n", filepath.Dir(path), err)
			os.Exit(1)
		}
		f, err := os.Create(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gen-ci-fixtures: create %q: %v\n", path, err)
			os.Exit(1)
		}
		if err := genmcap.Write(f, spec); err != nil {
			_ = f.Close()
			fmt.Fprintf(os.Stderr, "gen-ci-fixtures: write %q: %v\n", path, err)
			os.Exit(1)
		}
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "gen-ci-fixtures: close %q: %v\n", path, err)
			os.Exit(1)
		}
		mf = append(mf, fileManifest{
			Key:           rel,
			StartNs:       spec.StartNs,
			EndNs:         spec.EndNs(),
			TotalMessages: spec.TotalMessages(),
			Topics:        len(spec.Topics),
		})
		fmt.Fprintf(os.Stderr, "wrote %s (%d msgs, %d topics)\n", path, spec.TotalMessages(), len(spec.Topics))
	}

	for i, spec := range specs {
		rel := spec.Key
		if *hive {
			rel = hiveKeyFor(spec, i)
		}
		writeOne(spec, rel)
	}

	if *hiveBig {
		writeOne(bigSpec(), bigHiveKey)
	}

	if *manifest {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(mf); err != nil {
			fmt.Fprintf(os.Stderr, "gen-ci-fixtures: encode manifest: %v\n", err)
			os.Exit(1)
		}
	}
}
