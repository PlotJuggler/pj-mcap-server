package format

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/storage"
)

// fileStore is a tiny on-disk storage.BlobStore for the test: it serves ranged
// reads from a local file so Extract exercises the real ranged-read path
// (storage.ReaderAt -> GetRange) without needing Minio.
type fileStore struct{ root string }

func (f fileStore) path(key string) string { return filepath.Join(f.root, key) }

func (f fileStore) GetRange(_ context.Context, key string, off, length int64) ([]byte, error) {
	fh, err := os.Open(f.path(key))
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	if _, err := fh.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	if length <= 0 {
		return io.ReadAll(fh)
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(fh, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	return buf[:n], err
}

func (f fileStore) Head(_ context.Context, key string) (storage.ObjectInfo, error) {
	st, err := os.Stat(f.path(key))
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	return storage.ObjectInfo{
		Key:            key,
		ETag:           "local",
		Size:           st.Size(),
		LastModifiedNs: st.ModTime().UnixNano(),
	}, nil
}

func (f fileStore) List(context.Context, string, string) ([]storage.ObjectInfo, string, error) {
	return nil, "", nil
}

// Ground truth from /home/gn/ws/jkk_dataset02/baginfo.txt for
// nissan_zala_50_zeg_1_0.mcap (Oct 6 2023, ROS2 CDR).
func TestExtractNissanZalaZeg1(t *testing.T) {
	const key = "nissan_zala_50_zeg_1_0.mcap"
	bs := fileStore{root: "testdata"}
	codec, err := NewCodec("mcap")
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	fs, err := codec.Extract(context.Background(), bs, key)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	// 6 topics.
	if fs.TopicCount != 6 || len(fs.Topics) != 6 {
		t.Errorf("topic count: got TopicCount=%d len=%d want 6", fs.TopicCount, len(fs.Topics))
	}

	// Expected topic names present.
	want := map[string]bool{
		"/nissan/gps/duro/current_pose":  false,
		"/nissan/gps/duro/imu":           false,
		"/nissan/gps/duro/mag":           false,
		"/nissan/gps/duro/status_string": false,
		"/nissan/vehicle_speed":          false,
		"/nissan/vehicle_steering":       false,
	}
	for _, tp := range fs.Topics {
		if _, ok := want[tp.Name]; ok {
			want[tp.Name] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("expected topic %q not found", name)
		}
	}

	// Total message count = 33670 (baginfo sum).
	if fs.MessageCount != 33670 {
		t.Errorf("message_count: got %d want 33670", fs.MessageCount)
	}

	// CDR-family schema encoding. MCAP records the schema encoding; for these
	// ROS2 files it is "ros2msg" (the .msg type-definition language), with the
	// channel message encoding being "cdr".
	for _, tp := range fs.Topics {
		if tp.SchemaEncoding != "ros2msg" {
			t.Errorf("topic %q: schema_encoding got %q want ros2msg", tp.Name, tp.SchemaEncoding)
		}
	}

	// Per-topic counts spot-check against baginfo.
	wantCounts := map[string]uint64{
		"/nissan/gps/duro/imu":     14904,
		"/nissan/vehicle_speed":    4513,
		"/nissan/vehicle_steering": 4513,
	}
	for _, tp := range fs.Topics {
		if c, ok := wantCounts[tp.Name]; ok && tp.MessageCount != c {
			t.Errorf("topic %q: message_count got %d want %d", tp.Name, tp.MessageCount, c)
		}
	}

	// Start time ~ 1696577416664xxxxxx ns (Oct 6 2023 09:30:16.664).
	const wantStartPrefix = int64(1696577416664)
	if fs.StartNs/1_000_000 != wantStartPrefix {
		t.Errorf("start_ns: got %d (ms=%d) want ms-prefix %d",
			fs.StartNs, fs.StartNs/1_000_000, wantStartPrefix)
	}
	if fs.EndNs <= fs.StartNs {
		t.Errorf("end_ns %d should be > start_ns %d", fs.EndNs, fs.StartNs)
	}
}
