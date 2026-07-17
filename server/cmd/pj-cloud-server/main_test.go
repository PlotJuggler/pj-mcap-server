package main

import "testing"

// The anonymous-mode env opt-in must be STRICT ("1"/"true" only), mirroring the
// PJ_CLOUD_EXTERNAL_BUILDER parse: a fail-closed feature must not be disabled
// by PJ_CLOUD_ALLOW_ANONYMOUS=0 or =false (any-non-empty would fail OPEN).
func TestEnvTruthy(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"no", false},
		{"off", false},
		{" 1", false}, // no trimming: exact match only, like the external-builder parse
		{"1", true},
		{"true", true},
	}
	for _, c := range cases {
		if got := envTruthy(c.in); got != c.want {
			t.Errorf("envTruthy(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
