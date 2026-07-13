// Command gen-3d-fixture writes ONE synthetic MCAP whose payloads are REAL
// ROS2 CDR (tf2_msgs/TFMessage + sensor_msgs/PointCloud2 + std_msgs/Float32)
// with real concatenated .msg schema text, so the full pipeline — server →
// toolbox parser-ingest → parser_ros — classifies /tf as frame_transforms and
// /points as point_cloud (3D-scene draggable AND renderable). Deterministic.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"pj-cloud/server/internal/genmcap"
)

// cdr is a minimal XCDR1 little-endian writer; alignment is relative to the
// body start (after the 4-byte encapsulation header) — the rule rosx/FastCDR
// deserializers apply. Mirrors the C++ CdrWriter the real-parser_ros
// integration test validated (toolbox_parser_ingest_real_ros_test.cpp).
type cdr struct{ b []byte }

func newCdr() *cdr       { return &cdr{b: []byte{0x00, 0x01, 0x00, 0x00}} }
func (c *cdr) body() int { return len(c.b) - 4 }
func (c *cdr) align(n int) {
	for c.body()%n != 0 {
		c.b = append(c.b, 0)
	}
}
func (c *cdr) u8(v uint8)    { c.b = append(c.b, v) }
func (c *cdr) u32(v uint32)  { c.align(4); c.b = binary.LittleEndian.AppendUint32(c.b, v) }
func (c *cdr) i32(v int32)   { c.u32(uint32(v)) }
func (c *cdr) f32(v float32) { c.u32(math.Float32bits(v)) }
func (c *cdr) f64(v float64) {
	c.align(8)
	c.b = binary.LittleEndian.AppendUint64(c.b, math.Float64bits(v))
}
func (c *cdr) str(s string) {
	c.u32(uint32(len(s) + 1))
	c.b = append(c.b, s...)
	c.b = append(c.b, 0)
}
func (c *cdr) bytes(p []byte)                              { c.u32(uint32(len(p))); c.b = append(c.b, p...) }
func (c *cdr) header(sec int32, nsec uint32, frame string) { c.i32(sec); c.u32(nsec); c.str(frame) }

// tfMessage encodes one tf2_msgs/TFMessage with a single TransformStamped.
// Factored so the cross-validation test can build the EXACT message the C++
// reference writer (toolbox_parser_ingest_real_ros_test.cpp makeTfPayload)
// emits and assert byte equality.
func tfMessage(sec int32, nsec uint32, frame, child string, t [3]float64, q [4]float64) []byte {
	w := newCdr()
	w.u32(1) // transforms[] sequence length
	w.header(sec, nsec, frame)
	w.str(child)
	for _, v := range t {
		w.f64(v) // transform.translation x/y/z
	}
	for _, v := range q {
		w.f64(v) // transform.rotation x/y/z/w
	}
	return w.b
}

func tfPayload(idx int) []byte {
	// A robot driving in x with the identity quaternion: map -> base_link.
	return tfMessage(int32(idx), 0, "map", "base_link",
		[3]float64{float64(idx) * 0.1, 0, 0},
		[4]float64{0, 0, 0, 1})
}

// pointCloudPayload: 64 points on a spinning ring, fields x/y/z float32,
// point_step 12, height 1.
func pointCloudPayload(idx int) []byte {
	const n = 64
	data := make([]byte, 0, n*12)
	for i := 0; i < n; i++ {
		ang := float64(i)/float64(n)*2*math.Pi + float64(idx)*0.1
		for _, v := range []float64{2 * math.Cos(ang), 2 * math.Sin(ang), 0.2 * float64(idx%10)} {
			data = binary.LittleEndian.AppendUint32(data, math.Float32bits(float32(v)))
		}
	}
	w := newCdr()
	w.header(int32(idx), 0, "base_link")
	w.u32(1) // height
	w.u32(n) // width
	w.u32(3) // fields[]
	for fi, name := range []string{"x", "y", "z"} {
		w.str(name)
		w.u32(uint32(fi * 4)) // offset
		w.u8(7)               // datatype FLOAT32
		w.u32(1)              // count
	}
	w.u8(0)       // is_bigendian
	w.u32(12)     // point_step
	w.u32(12 * n) // row_step
	w.bytes(data) // data[]
	w.u8(1)       // is_dense
	return w.b
}

func float32Payload(idx int) []byte {
	w := newCdr()
	w.f32(float32(idx) * 0.5)
	return w.b
}

// kTfSchema is the concatenated ros2msg schema text exactly as a rosbag2 MCAP
// embeds it — copied VERBATIM from the empirically-validated C++ integration
// test (PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp,
// kTfSchema), where the REAL parser_ros classified it kFrameTransforms.
const kTfSchema = `geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
Transform transform
================================================================================
MSG: geometry_msgs/Transform
Vector3 translation
Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

// kPointCloudSchema is the concatenated sensor_msgs/msg/PointCloud2 schema.
const kPointCloudSchema = `std_msgs/Header header
uint32 height
uint32 width
PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
================================================================================
MSG: sensor_msgs/PointField
uint8 INT8=1
uint8 UINT8=2
uint8 INT16=3
uint8 UINT16=4
uint8 INT32=5
uint8 UINT32=6
uint8 FLOAT32=7
uint8 FLOAT64=8
string name
uint32 offset
uint8 datatype
uint32 count
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

// kFloat32Schema is the trivial std_msgs/msg/Float32 schema.
const kFloat32Schema = "float32 data\n"

// spec returns the single pinned 3D fixture: one MCAP, three real-CDR topics.
func spec() genmcap.FileSpec {
	return genmcap.FileSpec{
		Key:     "synthetic_3d_0.mcap",
		StartNs: 1_700_000_000_000_000_000,
		StepNs:  100_000_000, // 100ms
		Topics: []genmcap.TopicSpec{
			{
				Topic: "/tf", SchemaName: "tf2_msgs/msg/TFMessage", SchemaEnc: "ros2msg",
				MessageCount: 50, SchemaData: []byte(kTfSchema), PayloadFn: tfPayload,
			},
			{
				Topic: "/points", SchemaName: "sensor_msgs/msg/PointCloud2", SchemaEnc: "ros2msg",
				MessageCount: 20, SchemaData: []byte(kPointCloudSchema), PayloadFn: pointCloudPayload,
			},
			{
				Topic: "/speed", SchemaName: "std_msgs/msg/Float32", SchemaEnc: "ros2msg",
				MessageCount: 100, SchemaData: []byte(kFloat32Schema), PayloadFn: float32Payload,
			},
		},
	}
}

func main() {
	out := flag.String("out", "", "output directory for the 3D fixture MCAP (required)")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "gen-3d-fixture: -out <dir> is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "gen-3d-fixture: mkdir %q: %v\n", *out, err)
		os.Exit(1)
	}

	s := spec()
	path := filepath.Join(*out, s.Key)
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gen-3d-fixture: create %q: %v\n", path, err)
		os.Exit(1)
	}
	if err := genmcap.Write(f, s); err != nil {
		_ = f.Close()
		fmt.Fprintf(os.Stderr, "gen-3d-fixture: write %q: %v\n", path, err)
		os.Exit(1)
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "gen-3d-fixture: close %q: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s (%d msgs, %d topics)\n", path, s.TotalMessages(), len(s.Topics))
}
