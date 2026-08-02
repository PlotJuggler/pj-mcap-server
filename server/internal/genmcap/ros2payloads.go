// Real ROS2 CDR payloads + concatenated .msg schema text for the synthetic
// corpus's ros2msg topics, so the fixtures decode through the REAL parser
// pipeline (mcap-loader -> host parser delegation -> parser_ros), not only
// through count-asserting harnesses.
//
// WHY (stage-5 layout-import E2E, 2026-08-01): the corpus previously wrote
// schema data = the bare type-name string and deterministic garbage bytes as
// payloads. Every count/round-trip assertion in the Go/CLI harnesses is
// payload-agnostic, so that was invisible — until the live gui-test E2E put
// the REAL PJ4 parser stack behind the plugin and every channel failed with
// "failed to parse ROS schema: Missing ROSType in library" (mcap-loader then
// refuses the whole file: zero channels bound). The frozen E2E descriptor
// vectors pin ci_synth_* s3_keys, so the fixtures must decode.
//
// CONTRACT KEPT: keys, topic sets, message counts, log times, chunking and
// compression are untouched — every runtime-derived oracle (mcaptopics
// counts, catalog dimensions/time ranges, stitch sums) is unchanged. Only the
// message BYTES (and schema records) change, deterministically: payloads are
// pure functions of (topic, per-topic index, target size). Messages with a
// Header are padded via frame_id toward the spec's PayloadBytes so volume
// properties survive (ci_synth_big stays multi-batch for the resume legs).
//
// The CDR writer mirrors cmd/gen-3d-fixture's (XCDR1 little-endian, alignment
// relative to the body start after the 4-byte encapsulation header), which was
// itself cross-validated against the C++ reference writer in
// PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp.
package genmcap

import (
	"encoding/binary"
	"math"
	"strings"
)

// cdr is a minimal XCDR1 little-endian writer; alignment is relative to the
// body start (after the 4-byte encapsulation header).
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
func (c *cdr) header(sec int32, nsec uint32, frame string) { c.i32(sec); c.u32(nsec); c.str(frame) }

// padFrame returns a frame_id padded with 'x' so the finished message lands
// close to targetBytes. `natural` is the encoded size with the UNPADDED
// frame; a targetBytes at or below it returns the base frame unchanged.
//
// BEST-EFFORT, and the pad length is NOT the size delta: the field after
// frame_id is 8-aligned (float64 / a nested Header), so growing the string
// shifts that alignment and the finished message can absorb some pad bytes or
// overshoot by up to 7. Measured worst case across all four padded types is 7
// bytes, i.e. the finished size lands within ±8 of targetBytes (e.g.
// tfPayload(0, 302) -> 300, imuPayload(0, 4096) -> 4100,
// odometryPayload(0, 4096) -> 4092). Callers must treat PayloadBytes as a
// volume knob, never as an exact size — genmcap_test's golden lengths pin the
// UNPADDED sizes for exactly that reason.
func padFrame(base string, natural, targetBytes int) string {
	if targetBytes <= natural {
		return base
	}
	return base + strings.Repeat("x", targetBytes-natural)
}

// ---------------------------------------------------------------------------
// Schema text (concatenated .msg, rosbag2 style — the exact format the
// empirically-validated kTfSchema in cmd/gen-3d-fixture uses).
// ---------------------------------------------------------------------------

const kSep = "================================================================================\n"

const kHeaderDeps = kSep + `MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
` + kSep + `MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

const kClockSchema = `builtin_interfaces/Time clock
` + kSep + `MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

const kImuSchema = `std_msgs/Header header
geometry_msgs/Quaternion orientation
float64[9] orientation_covariance
geometry_msgs/Vector3 angular_velocity
float64[9] angular_velocity_covariance
geometry_msgs/Vector3 linear_acceleration
float64[9] linear_acceleration_covariance
` + kSep + `MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
` + kSep + `MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
` + kHeaderDeps

const kOdometrySchema = `std_msgs/Header header
string child_frame_id
geometry_msgs/PoseWithCovariance pose
geometry_msgs/TwistWithCovariance twist
` + kSep + `MSG: geometry_msgs/PoseWithCovariance
Pose pose
float64[36] covariance
` + kSep + `MSG: geometry_msgs/Pose
Point position
Quaternion orientation
` + kSep + `MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
` + kSep + `MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
` + kSep + `MSG: geometry_msgs/TwistWithCovariance
Twist twist
float64[36] covariance
` + kSep + `MSG: geometry_msgs/Twist
Vector3 linear
Vector3 angular
` + kSep + `MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
` + kHeaderDeps

const kLaserScanSchema = `std_msgs/Header header
float32 angle_min
float32 angle_max
float32 angle_increment
float32 time_increment
float32 scan_time
float32 range_min
float32 range_max
float32[] ranges
float32[] intensities
` + kHeaderDeps

const kTfSchema = `geometry_msgs/TransformStamped[] transforms
` + kSep + `MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
Transform transform
` + kSep + `MSG: geometry_msgs/Transform
Vector3 translation
Quaternion rotation
` + kSep + `MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
` + kSep + `MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
` + kHeaderDeps

// ---------------------------------------------------------------------------
// Payload encoders — deterministic in (idx); padded toward targetBytes where
// the type carries a Header.
// ---------------------------------------------------------------------------

func clockPayload(idx, _ int) []byte {
	w := newCdr()
	w.i32(int32(idx))         // clock.sec
	w.u32(uint32(idx) * 1000) // clock.nanosec
	return w.b
}

func imuBody(idx int, frame string) []byte {
	w := newCdr()
	w.header(int32(idx), 0, frame)
	for _, v := range [4]float64{0, 0, 0, 1} { // orientation (identity)
		w.f64(v)
	}
	for i := 0; i < 9; i++ { // orientation_covariance
		w.f64(0)
	}
	// angular_velocity
	w.f64(0.01 * float64(idx))
	w.f64(-0.01 * float64(idx))
	w.f64(0.5)
	for i := 0; i < 9; i++ { // angular_velocity_covariance
		w.f64(0)
	}
	// linear_acceleration
	w.f64(0.1 * float64(idx))
	w.f64(math.Sin(0.1 * float64(idx)))
	w.f64(9.81)
	for i := 0; i < 9; i++ { // linear_acceleration_covariance
		w.f64(0)
	}
	return w.b
}

func imuPayload(idx, targetBytes int) []byte {
	natural := imuBody(idx, "imu_link")
	return imuBody(idx, padFrame("imu_link", len(natural), targetBytes))
}

func odometryBody(idx int, frame string) []byte {
	w := newCdr()
	w.header(int32(idx), 0, frame)
	w.str("base_link") // child_frame_id
	// pose.pose: position + orientation
	w.f64(0.1 * float64(idx))
	w.f64(0.05 * float64(idx))
	w.f64(0)
	for _, v := range [4]float64{0, 0, 0, 1} {
		w.f64(v)
	}
	for i := 0; i < 36; i++ { // pose.covariance
		w.f64(0)
	}
	// twist.twist: linear + angular
	w.f64(1.5)
	w.f64(0)
	w.f64(0)
	w.f64(0)
	w.f64(0)
	w.f64(0.02 * float64(idx))
	for i := 0; i < 36; i++ { // twist.covariance
		w.f64(0)
	}
	return w.b
}

func odometryPayload(idx, targetBytes int) []byte {
	natural := odometryBody(idx, "odom")
	return odometryBody(idx, padFrame("odom", len(natural), targetBytes))
}

func laserScanBody(idx int, frame string) []byte {
	w := newCdr()
	w.header(int32(idx), 0, frame)
	w.f32(-1.57) // angle_min
	w.f32(1.57)  // angle_max
	w.f32(0.1)   // angle_increment
	w.f32(0)     // time_increment
	w.f32(0.1)   // scan_time
	w.f32(0.2)   // range_min
	w.f32(30)    // range_max
	const n = 8
	w.u32(n) // ranges[]
	for i := 0; i < n; i++ {
		w.f32(float32(1 + i%4 + idx%10))
	}
	w.u32(n) // intensities[]
	for i := 0; i < n; i++ {
		w.f32(float32(100 + i))
	}
	return w.b
}

func laserScanPayload(idx, targetBytes int) []byte {
	natural := laserScanBody(idx, "laser")
	return laserScanBody(idx, padFrame("laser", len(natural), targetBytes))
}

func tfBody(idx int, frame string) []byte {
	w := newCdr()
	w.u32(1) // transforms[] length
	w.header(int32(idx), 0, frame)
	w.str("base_link")
	w.f64(0.1 * float64(idx)) // translation
	w.f64(0)
	w.f64(0)
	for _, v := range [4]float64{0, 0, 0, 1} { // rotation
		w.f64(v)
	}
	return w.b
}

func tfPayload(idx, targetBytes int) []byte {
	natural := tfBody(idx, "map")
	return tfBody(idx, padFrame("map", len(natural), targetBytes))
}

// realRos2Payload maps a TopicSpec's SchemaName to its real schema text +
// payload encoder. Returns ok=false for types this table does not know —
// Write() then falls back to the legacy synthetic bytes (explicit SchemaData /
// PayloadFn overrides always win; see Write).
func realRos2Payload(schemaName string) (schema []byte, fn func(idx, targetBytes int) []byte, ok bool) {
	switch schemaName {
	case "rosgraph_msgs/msg/Clock":
		return []byte(kClockSchema), clockPayload, true
	case "sensor_msgs/msg/Imu":
		return []byte(kImuSchema), imuPayload, true
	case "nav_msgs/msg/Odometry":
		return []byte(kOdometrySchema), odometryPayload, true
	case "sensor_msgs/msg/LaserScan":
		return []byte(kLaserScanSchema), laserScanPayload, true
	case "tf2_msgs/msg/TFMessage":
		return []byte(kTfSchema), tfPayload, true
	}
	return nil, nil, false
}
