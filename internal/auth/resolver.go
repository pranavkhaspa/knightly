package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// ErrIdentityRejected indicates that request evidence did not verify into a
// trusted identity. Callers must fail closed and never fall back to request
// supplied scope or approver values.
var ErrIdentityRejected = errors.New("request identity could not be verified")

// IdentityResolver derives a trusted Identity from request evidence only. A
// resolver replaces the deployment-wide static identity: it must verify OIDC
// tokens or trusted-proxy signatures and must never read scope or approver
// values that a client can set without proof.
type IdentityResolver interface {
	Resolve(ctx context.Context, request *http.Request) (Identity, error)
}

// newIdentity normalizes and validates verified identity values.
func newIdentity(tenantID, userID, approverID string) (Identity, error) {
	value := Identity{
		Scope: identity.Scope{
			TenantID: identity.NormalizeID(tenantID),
			UserID:   identity.NormalizeID(userID),
		},
		ApproverID: identity.NormalizeID(approverID),
	}
	if err := value.Scope.Validate(); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrIdentityRejected, err)
	}
	if value.ApproverID == "" {
		value.ApproverID = value.Scope.UserID
	}
	if err := identity.ValidateID("approver ID", value.ApproverID); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrIdentityRejected, err)
	}
	return value, nil
}
