package main

import (
	"encoding/hex"
	"testing"
)

// TestTfCdrMatchesCppReference cross-validates the Go CDR writer against the
// C++ CdrWriter that the REAL-parser_ros integration test empirically
// validated (PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp,
// makeTfPayload): one TFMessage with one TransformStamped —
// stamp{sec=7, nsec=500}, frame "map", child "base_link", t=(1,2,3),
// q=(0,0,0,1) — must produce byte-identical CDR.
//
// The expected bytes are derived BY HAND from the C++ CdrWriter rules
// (alignment relative to the body start, i.e. after the 4-byte encapsulation
// header):
//
//	off(file) off(body) bytes                 field
//	0         -         00 01 00 00           encapsulation {CDR_LE, options=0}
//	4         0         01 00 00 00           u32 transforms[] length = 1
//	8         4         07 00 00 00           i32 header.stamp.sec = 7
//	12        8         f4 01 00 00           u32 header.stamp.nanosec = 500 (0x1f4)
//	16        12        04 00 00 00           u32 strlen("map")+1 = 4
//	20        16        6d 61 70 00           "map\0"
//	24        20        0a 00 00 00           u32 strlen("base_link")+1 = 10
//	                                          (body off 20 already 4-aligned: no pad)
//	28        24        62 61 73 65 5f 6c     "base_link\0"
//	          ...       69 6e 6b 00
//	38        34        00 x6                 pad: align(8) for f64 (body 34 -> 40)
//	44        40        00 00 00 00 00 00 f0 3f  f64 translation.x = 1.0
//	52        48        00 00 00 00 00 00 00 40  f64 translation.y = 2.0
//	60        56        00 00 00 00 00 00 08 40  f64 translation.z = 3.0
//	68        64        00 x8                 f64 rotation.x = 0.0
//	76        72        00 x8                 f64 rotation.y = 0.0
//	84        80        00 x8                 f64 rotation.z = 0.0
//	92        88        00 00 00 00 00 00 f0 3f  f64 rotation.w = 1.0
//	100 bytes total
const cppReferenceTfHex = "00010000" + // encapsulation
	"01000000" + // transforms len
	"07000000" + // sec
	"f4010000" + // nsec
	"04000000" + "6d617000" + // "map"
	"0a000000" + "626173655f6c696e6b00" + // "base_link"
	"000000000000" + // align(8) pad
	"000000000000f03f" + // tx = 1.0
	"0000000000000040" + // ty = 2.0
	"0000000000000840" + // tz = 3.0
	"0000000000000000" + // qx = 0.0
	"0000000000000000" + // qy = 0.0
	"0000000000000000" + // qz = 0.0
	"000000000000f03f" // qw = 1.0

func TestTfCdrMatchesCppReference(t *testing.T) {
	got := tfMessage(7, 500, "map", "base_link",
		[3]float64{1, 2, 3}, [4]float64{0, 0, 0, 1})
	if gotHex := hex.EncodeToString(got); gotHex != cppReferenceTfHex {
		t.Fatalf("Go tf CDR diverges from the C++ reference writer\n got: %s\nwant: %s", gotHex, cppReferenceTfHex)
	}
	if len(got) != 100 {
		t.Fatalf("tf CDR length = %d, want 100", len(got))
	}
}

// TestPayloadsDeterministic pins the determinism contract genmcap relies on
// (byte-identical re-runs => indexer change-detect no-op).
func TestPayloadsDeterministic(t *testing.T) {
	for idx := 0; idx < 3; idx++ {
		for name, fn := range map[string]func(int) []byte{
			"tf": tfPayload, "points": pointCloudPayload, "speed": float32Payload,
		} {
			a, b := fn(idx), fn(idx)
			if hex.EncodeToString(a) != hex.EncodeToString(b) {
				t.Fatalf("%s payload idx=%d not deterministic", name, idx)
			}
		}
	}
}
