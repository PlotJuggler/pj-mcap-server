// Package authn is the client↔server auth seam (unified-plan §3.2 seam 3,
// Plan A Task 24a). v1 has one impl: a shared bearer token with a constant-time
// compare. The principal is always "shared" in v1, which makes spec §8.2 resume
// re-auth a vacuous no-op (hijack protection is deferred to a future OIDC
// ClientAuthenticator impl).
package authn

import (
	"context"
	"crypto/subtle"
	"errors"
)

// ErrAuthFailed is returned by Verify when the credential does not match.
var ErrAuthFailed = errors.New("authn: invalid token")

// ClientAuthenticator verifies a client credential and returns its principal.
// Called once per WS connection in the Hello handler.
type ClientAuthenticator interface {
	Verify(ctx context.Context, token, remoteAddr string) (principal string, err error)
}

type bearerToken struct{ secret []byte }

// NewBearerToken returns a ClientAuthenticator that constant-time-compares the
// presented token against secret and yields principal "shared" on success.
func NewBearerToken(secret string) ClientAuthenticator {
	return &bearerToken{secret: []byte(secret)}
}

func (b *bearerToken) Verify(_ context.Context, token, _ string) (string, error) {
	if subtle.ConstantTimeCompare([]byte(token), b.secret) != 1 {
		return "", ErrAuthFailed
	}
	return "shared", nil
}
