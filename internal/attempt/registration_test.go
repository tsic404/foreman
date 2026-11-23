package attempt

import "testing"

// TestVerifyRegistrationComparesOnlyTheDaemonCLI pins the version rule: the
// supervisor verifies the daemon's Multica CLI version against the frozen
// Multica artifact. A legitimately different Agent runtime version must pass,
// and the Agent version must never be accepted as the daemon's.
func TestVerifyRegistrationComparesOnlyTheDaemonCLI(t *testing.T) {
	binding := Binding{DaemonID: "fm-task-1", JobRuntimeID: "runtime-1"}
	prepared := []Artifact{
		{Kind: KindMultica, Version: "0.11.1"},
		{Kind: KindAgent, Version: "19.0.1"},
	}
	registration := Registration{
		DaemonID: "fm-task-1", RuntimeID: "runtime-1",
		Version: "0.11.1", AgentVersion: "19.0.1",
	}
	if err := verifyRegistration(binding, prepared, registration); err != nil {
		t.Fatalf("verifyRegistration rejected a legitimate daemon/agent version difference: %v", err)
	}

	registration.Version = "19.0.1"
	if err := verifyRegistration(binding, prepared, registration); err == nil {
		t.Fatal("verifyRegistration accepted the Agent version as the daemon CLI version")
	}
}
