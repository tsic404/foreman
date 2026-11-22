package scheduler

import (
	"encoding/json"
	"fmt"
	"net/url"
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
func fixedAttemptSpec(e registry.TaskEntry, binding EnvironmentBinding, jobImage, proxyURL string, cliBudget int64) ([]byte, bool, error) {
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
	image, imageDigest, ok := splitImageDigest(jobImage)
	if !ok {
		return nil, true, fmt.Errorf("job image %q carries no verified digest; the carrier must be frozen per attempt", jobImage)
	}
	platformOS, platformArch, ok := splitPlatform(binding.Platform)
	if !ok {
		return nil, true, fmt.Errorf("client platform %q is not os/arch", binding.Platform)
	}
	multicaDigest := normalizeDigest(binding.MulticaDigest)
	agentDigest := normalizeDigest(binding.AgentDigest)
	if !attempt.ValidDigest(multicaDigest) || !attempt.ValidDigest(agentDigest) {
		return nil, true, fmt.Errorf("the frozen CLI digests are not sha256 digests")
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
		Image: image, ImageDigest: imageDigest, Platform: binding.Platform,
		ProxyURL: proxyURL, CLIBudgetBytes: cliBudget,
		CLIs: []attempt.CLI{
			{Kind: attempt.KindMultica, Version: binding.MulticaVersion, Platform: binding.Platform, Digest: multicaDigest,
				DownloadURL: cliDownloadAddress(proxyURL, attempt.KindMultica, "", binding.MulticaVersion, platformOS, platformArch, multicaDigest)},
			{Kind: attempt.KindAgent, Backend: binding.AgentBackend, Version: binding.AgentVersion, Platform: binding.Platform, Digest: agentDigest,
				DownloadURL: cliDownloadAddress(proxyURL, attempt.KindAgent, binding.AgentBackend, binding.AgentVersion, platformOS, platformArch, agentDigest)},
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

func splitImageDigest(reference string) (string, string, bool) {
	reference = strings.TrimSpace(reference)
	repo, digest, found := strings.Cut(reference, "@")
	if !found || strings.TrimSpace(repo) == "" || !attempt.ValidDigest(digest) {
		return "", "", false
	}
	return repo, normalizeDigest(digest), true
}

func splitPlatform(platform string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(platform), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func normalizeDigest(digest string) string {
	return "sha256:" + attempt.DigestHex(digest)
}

func cliDownloadAddress(proxy, kind, backend, version, platformOS, platformArch, digest string) string {
	address := strings.TrimRight(proxy, "/") + "/cli/" + kind + "/" +
		url.PathEscape(version) + "/" + url.PathEscape(platformOS) + "/" + url.PathEscape(platformArch)
	query := url.Values{"digest": {normalizeDigest(digest)}}
	if backend != "" {
		query.Set("backend", backend)
	}
	return address + "?" + query.Encode()
}
