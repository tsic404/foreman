package scheduler

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/attempt"
	"github.com/tsic404/foreman/internal/registry"
)

// fixedAttemptSpec freezes the two-CLI/environment binding the fixed Job
// consumes. It reports fixed=false when the binder answered only the legacy
// ids, in which case the template path stays in force. A binder that names an
// environment but cannot supply its complete placement becomes a claim
// refusal: the fixed path never guesses an endpoint or an artifact.
func fixedAttemptSpec(e registry.TaskEntry, binding EnvironmentBinding) ([]byte, bool, error) {
	if binding.Endpoint == "" && binding.TLSRef == "" && binding.StateRoot == "" &&
		binding.EnvironmentGen == "" && binding.Platform == "" && binding.AgentBackend == "" &&
		binding.MulticaVersion == "" && binding.MulticaDigest == "" &&
		binding.AgentVersion == "" && binding.AgentDigest == "" && binding.ConfigRevision == 0 {
		// A legacy binder answered only the reservation ids.
		return nil, false, nil
	}
	required := map[string]string{
		"environment endpoint":   binding.Endpoint,
		"environment TLS ref":    binding.TLSRef,
		"environment state root": binding.StateRoot,
		"environment generation": binding.EnvironmentGen,
		"platform":               binding.Platform,
		"agent backend":          binding.AgentBackend,
		"multica CLI version":    binding.MulticaVersion,
		"multica CLI digest":     binding.MulticaDigest,
		"agent CLI version":      binding.AgentVersion,
		"agent CLI digest":       binding.AgentDigest,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return nil, true, fmt.Errorf("fixed placement has no %s", name)
		}
	}
	attemptID := registry.AttemptID(e.TaskID, e.Attempt)
	config := attempt.Config{
		Revision: binding.ConfigRevision,
		Digest:   binding.ConfigDigest,
	}
	if binding.ConfigRevision > 0 {
		switch {
		case binding.ConfigSecretRef == "":
			return nil, true, fmt.Errorf("config revision %d names no Secret reference", binding.ConfigRevision)
		case binding.ConfigFileName == "":
			return nil, true, fmt.Errorf("config revision %d names no file", binding.ConfigRevision)
		}
		config.SecretRef = binding.ConfigSecretRef
		config.SecretName = agentconfig.SecretNameForAttempt(attemptID)
		config.FileName = binding.ConfigFileName
		config.MountPath = agentconfig.MountDir + "/" + binding.ConfigFileName
	}
	spec := attempt.Binding{
		AttemptID: attemptID, TaskID: e.TaskID, SourceID: e.SourceID,
		WorkspaceID: e.WorkspaceID, AgentID: e.AgentID, DaemonID: e.DaemonID,
		JobRuntimeID: e.JobRuntimeID, EnvironmentID: binding.EnvironmentID,
		Endpoint: binding.Endpoint, TLSRef: binding.TLSRef, StateRoot: binding.StateRoot,
		EnvironmentGen: binding.EnvironmentGen, ResumeSessionID: e.PriorSessionID,
		SlotID: binding.SlotID, Config: config,
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: binding.MulticaVersion, Platform: binding.Platform, Digest: binding.MulticaDigest},
			{Kind: attempt.KindAgent, Backend: binding.AgentBackend, Version: binding.AgentVersion, Platform: binding.Platform, Digest: binding.AgentDigest},
		},
	}
	if err := spec.Validate(); err != nil {
		return nil, true, err
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, true, fmt.Errorf("encode the fixed attempt binding: %w", err)
	}
	return raw, true, nil
}
