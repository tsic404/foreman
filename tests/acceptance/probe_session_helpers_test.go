// Shared construction of a catalog-probe session for the probe cases
// (TC-tech-catalog-11, TC-tech-auth-07): the token's only adjudication face is
// the session row, so both are seeded as one step (§6.3).
package acceptance

import (
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/registry"
)

// probeSeed is the identity one probe session is constructed under: the
// probe's Job name, its request id, its runtime ids and the workspace the
// session and the token are pinned to.
type probeSeed struct {
	jobName     string
	requestID   string
	jobRuntime  string
	realRuntime string
	workspaceID string
}

// seedProbeSession issues the probe's own Job Token and inserts the session
// row the token is adjudicated against (§6.3). Only workspaceID tells a
// session pointing at a served workspace from one pointing past the
// deployment's discovery set.
func seedProbeSession(t *testing.T, issuer *auth.Issuer, probes *registry.ProbeRegistry, seed probeSeed) (string, registry.ProbeSession) {
	t.Helper()
	token, err := issuer.IssueProbe(seed.jobName, seed.requestID, seed.workspaceID, 10*time.Minute)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	session := registry.ProbeSession{
		JobName:       seed.jobName,
		Mode:          registry.ProbeCommandDriven,
		RequestID:     seed.requestID,
		TokenHash:     issuer.Hash(token),
		JobRuntime:    seed.jobRuntime,
		RealRuntimeID: seed.realRuntime,
		WorkspaceID:   seed.workspaceID,
		Deadline:      time.Now().Add(time.Minute),
	}
	if err := probes.PutProbe(session); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	return token, session
}
