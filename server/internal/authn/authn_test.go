package authn

import (
	"context"
	"errors"
	"testing"
)

// TestBearerToken_CorrectThenPrincipal (Plan A Task 24a Step 1): the matching
// token verifies and yields principal "shared".
func TestBearerToken_CorrectThenPrincipal(t *testing.T) {
	a := NewBearerToken("correct-secret")
	principal, err := a.Verify(context.Background(), "correct-secret", "1.2.3.4:5678")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if principal != "shared" {
		t.Errorf("principal: got %q want \"shared\"", principal)
	}
}

// TestBearerToken_WrongFails (Plan A Task 24a Step 1): empty, wrong, and the two
// off-by-one (truncated / extra-char) tokens all fail with ErrAuthFailed.
func TestBearerToken_WrongFails(t *testing.T) {
	a := NewBearerToken("correct-secret")
	for _, tok := range []string{"", "wrong", "correct-secre", "correct-secrett"} {
		_, err := a.Verify(context.Background(), tok, "1.2.3.4:0")
		if err == nil {
			t.Errorf("token %q should fail", tok)
			continue
		}
		if !errors.Is(err, ErrAuthFailed) {
			t.Errorf("token %q: want ErrAuthFailed, got %v", tok, err)
		}
	}
}
