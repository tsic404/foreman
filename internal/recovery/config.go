package recovery

import (
	"os"
	"path/filepath"
	"strings"
)

// Environment variable names from the contract config table (§5.1).
const EnvStateRoot = "FOREMAN_STATE_ROOT"

// DefaultStateRoot is the contract default node root (§5.1).
const DefaultStateRoot = "/var/lib/foreman"

// PendingReportsDir resolves the durable queue directory:
// FOREMAN_STATE_ROOT/pending-reports (contract §4).
func PendingReportsDir(getenv func(string) string) string {
	if getenv == nil {
		getenv = os.Getenv
	}
	root := strings.TrimSpace(getenv(EnvStateRoot))
	if root == "" {
		root = DefaultStateRoot
	}
	return filepath.Join(root, "pending-reports")
}
