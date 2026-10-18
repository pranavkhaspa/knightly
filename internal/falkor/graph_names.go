package falkor

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
)

// UserGraphName creates a stable, non-identifying FalkorDB graph name. It is
// safe to use for multi-user deployments because display names and credentials
// never become part of the graph name.
func UserGraphName(tenantID, userID string) string {
	value := strings.TrimSpace(tenantID) + "\x00" + strings.TrimSpace(userID)
	sum := sha256.Sum256([]byte(value))
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
	return "nightshift_u_" + strings.ToLower(encoded[:16])
}
