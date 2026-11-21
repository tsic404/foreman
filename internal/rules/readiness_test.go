package rules

import (
	"context"
	"errors"
	"testing"

	"github.com/tsic404/foreman/internal/agentconfig"
	"github.com/tsic404/foreman/internal/catalog"
)

type countingSessions struct {
	records int
}

func (c *countingSessions) EnvironmentForSession(context.Context, string, string) (string, bool) {
	return "", false
}

func (c *countingSessions) RecordSession(context.Context, string, string, string) {
	c.records++
}

func readinessConfig(envs *fakeEnvironments, directory catalog.DirectoryView) Config {
	return Config{
		Environments: envs,
		Catalog:      &fakeCatalog{view: directory},
		Issues:       fakeIssues{},
		Releases:     fakeReleases{digests: map[string]string{"linux/amd64": "sha256:cli"}},
		Configs:      fakeConfigs{},
		Sessions:     &countingSessions{},
		Platform:     "linux/amd64",
		JobNamespace: "foreman",
	}
}

func checkByKey(t *testing.T, report ReadinessReport, key string) Check {
	t.Helper()
	for _, check := range report.Checks {
		if check.Key == key {
			return check
		}
	}
	t.Fatalf("readiness report has no %q check: %+v", key, report.Checks)
	return Check{}
}

func TestReadinessReadyIsReadOnlyAndItemized(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	cfg := readinessConfig(envs, testDirectory())
	sessions := cfg.Sessions.(*countingSessions)
	service := newTestServiceWithConfig(t, cfg)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeReady || report.RuleID != "a5" ||
		report.Environment == nil || report.Environment.ID != "build-a" {
		t.Fatalf("readiness = %+v, want ready on a5/build-a", report.Resolution)
	}
	if report.Primary != nil {
		t.Fatalf("ready report has primary finding %+v", report.Primary)
	}
	if len(report.Checks) != 10 {
		t.Fatalf("checks = %d, want 10: %+v", len(report.Checks), report.Checks)
	}
	for _, check := range report.Checks {
		if check.Status != OutcomeReady {
			t.Fatalf("check %q = %q, want ready (%+v)", check.Key, check.Status, check)
		}
		if check.EvidenceAt == "" {
			t.Fatalf("check %q has no evidence time", check.Key)
		}
	}
	if envs.acquires != 0 || envs.releases != 0 {
		t.Fatalf("readiness reserved/released slots: %d/%d", envs.acquires, envs.releases)
	}
	if sessions.records != 0 {
		t.Fatalf("readiness recorded %d sessions", sessions.records)
	}
}

func TestReadinessUnmanagedStopsDependentChecks(t *testing.T) {
	directory := testDirectory()
	directory.Agents[0].Management = string(catalog.ManagedUnmanaged)
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, directory)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeUnmanaged {
		t.Fatalf("outcome = %q, want unmanaged", report.Outcome)
	}
	if got := checkByKey(t, report, "agent").Status; got != OutcomeUnmanaged {
		t.Fatalf("agent check = %q, want unmanaged", got)
	}
	if got := checkByKey(t, report, "project").Status; got != OutcomeUnknown {
		t.Fatalf("project check = %q, want unknown (not evaluated)", got)
	}
	if report.Primary == nil || report.Primary.Code != "unmanaged_runtime" {
		t.Fatalf("primary = %+v, want unmanaged_runtime", report.Primary)
	}
	if envs.acquires != 0 {
		t.Fatalf("readiness reserved %d slots", envs.acquires)
	}
}

func TestReadinessOfflineSourceIsUnknown(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	cfg := readinessConfig(envs, testDirectory())
	cfg.Catalog = &fakeCatalog{err: errors.New("source offline")}
	service := newTestServiceWithConfig(t, cfg)

	report, err := service.Readiness(context.Background(), Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want unknown", report.Outcome)
	}
	if got := checkByKey(t, report, "source").Status; got != OutcomeUnknown {
		t.Fatalf("source check = %q, want unknown", got)
	}
	if got := checkByKey(t, report, "rules").Status; got != OutcomeUnknown {
		t.Fatalf("rules check = %q, want unknown (source unreadable)", got)
	}
	if report.Primary == nil || report.Primary.Code != "source_unknown" {
		t.Fatalf("primary = %+v, want source_unknown", report.Primary)
	}
	if envs.acquires != 0 {
		t.Fatalf("readiness reserved %d slots", envs.acquires)
	}
}

func TestReadinessMissingCLIArchIsConfigBlocked(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	cfg := readinessConfig(envs, testDirectory())
	cfg.Releases = fakeReleases{digests: map[string]string{"linux/arm64": "sha256:cli"}}
	service := newTestServiceWithConfig(t, cfg)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeConfigBlocked {
		t.Fatalf("outcome = %q, want config_blocked", report.Outcome)
	}
	for _, key := range []string{"cli_multica", "cli_agent"} {
		check := checkByKey(t, report, key)
		if check.Status != OutcomeConfigBlocked {
			t.Fatalf("%s check = %q, want config_blocked", key, check.Status)
		}
		if check.Remediation == nil || check.Remediation.Path != "/admin/cli" {
			t.Fatalf("%s remediation = %+v, want /admin/cli", key, check.Remediation)
		}
	}
	if got := checkByKey(t, report, "rules").Status; got != OutcomeReady {
		t.Fatalf("rules check = %q, want ready (preview still hits the rule)", got)
	}
	if report.Primary == nil || report.Primary.Code != "multica_cli_missing" {
		t.Fatalf("primary = %+v, want multica_cli_missing", report.Primary)
	}
}

func TestReadinessMissingSecretIsConfigBlocked(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	cfg := readinessConfig(envs, testDirectory())
	cfg.Configs = fakeConfigs{found: true, revision: agentconfig.Revision{Revision: 4}}
	service := newTestServiceWithConfig(t, cfg)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeConfigBlocked {
		t.Fatalf("outcome = %q, want config_blocked", report.Outcome)
	}
	check := checkByKey(t, report, "config")
	if check.Status != OutcomeConfigBlocked || check.Remediation == nil ||
		check.Remediation.Path != "/admin/agent-configs" {
		t.Fatalf("config check = %+v, want blocked with /admin/agent-configs", check)
	}
	if report.Primary == nil || report.Primary.Code != "config_secret_missing" {
		t.Fatalf("primary = %+v, want config_secret_missing", report.Primary)
	}
}

func TestReadinessUnverifiedSecretIsUnknownWhilePreviewStillHits(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 1))
	cfg := readinessConfig(envs, testDirectory())
	cfg.Configs = fakeConfigs{found: true, revision: agentconfig.Revision{
		Revision: 4, SecretRef: "secret/agent-config", FileName: "config.json", Digest: "sha256:cfg",
	}}
	service := newTestServiceWithConfig(t, cfg)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	preview, err := service.Preview(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview.Outcome != OutcomeReady || preview.RuleID != "a5" {
		t.Fatalf("preview = %q/%q, want the rule still hits", preview.Outcome, preview.RuleID)
	}

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want unknown", report.Outcome)
	}
	if report.RuleID != "a5" {
		t.Fatalf("readiness rule = %q, want a5", report.RuleID)
	}
	check := checkByKey(t, report, "config")
	if check.Status != OutcomeUnknown || check.Remediation == nil ||
		check.Remediation.Path != "/admin/agent-configs" {
		t.Fatalf("config check = %+v, want unknown with /admin/agent-configs", check)
	}
	if report.Primary == nil || report.Primary.Code != "config_unverified" {
		t.Fatalf("primary = %+v, want config_unverified", report.Primary)
	}
	if envs.acquires != 0 {
		t.Fatalf("readiness reserved %d slots", envs.acquires)
	}
}

func TestReadinessSaturatedStillReportsIndependentFacts(t *testing.T) {
	envs := newFakeEnvironments(envView("build-a", 0))
	service := newTestService(t, envs, testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeTemporarilySaturated {
		t.Fatalf("outcome = %q, want temporarily_saturated", report.Outcome)
	}
	for _, key := range []string{"environment", "capacity"} {
		if got := checkByKey(t, report, key).Status; got != OutcomeTemporarilySaturated {
			t.Fatalf("%s check = %q, want temporarily_saturated", key, got)
		}
	}
	for _, key := range []string{"rules", "cli_multica", "cli_agent", "config"} {
		if got := checkByKey(t, report, key).Status; got != OutcomeReady {
			t.Fatalf("%s check = %q, want ready (independent fact)", key, got)
		}
	}
	if report.Primary == nil || report.Primary.Code != "environment_saturated" {
		t.Fatalf("primary = %+v, want environment_saturated", report.Primary)
	}
	if envs.acquires != 0 {
		t.Fatalf("readiness reserved %d slots", envs.acquires)
	}
}

func TestReadinessRequiresAnIssue(t *testing.T) {
	service := newTestService(t, newFakeEnvironments(envView("build-a", 1)), testDirectory())
	report, err := service.Readiness(context.Background(), Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeConfigBlocked {
		t.Fatalf("outcome = %q, want config_blocked", report.Outcome)
	}
	if len(report.Checks) == 0 || report.Checks[0].Key != "request" ||
		report.Checks[0].Status != OutcomeConfigBlocked {
		t.Fatalf("first check = %+v, want a blocked request item", report.Checks)
	}
	if report.Primary == nil || report.Primary.Code != "request_incomplete" {
		t.Fatalf("primary = %+v, want request_incomplete", report.Primary)
	}
}

func TestReadinessRequiresTheWholeIssueIdentity(t *testing.T) {
	service := newTestService(t, newFakeEnvironments(envView("build-a", 1)), testDirectory())
	report, err := service.Readiness(context.Background(), Request{
		ServerID: "atlas", WorkspaceID: "core", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeConfigBlocked {
		t.Fatalf("outcome = %q, want config_blocked", report.Outcome)
	}
	check := checkByKey(t, report, "request")
	if check.Status != OutcomeConfigBlocked {
		t.Fatalf("request check = %q, want config_blocked", check.Status)
	}
	if report.Primary == nil || report.Primary.Code != "request_incomplete" {
		t.Fatalf("primary = %+v, want request_incomplete", report.Primary)
	}
}

func TestReadinessTakesTheProjectFromTheIssue(t *testing.T) {
	service := newTestService(t, newFakeEnvironments(envView("build-a", 1)), testDirectory())
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)
	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev",
		IssueID: "issue-1", ProjectID: "a-caller-supplied-project",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeReady || report.ProjectID != "mobile" || report.RuleID != "a5" {
		t.Fatalf("readiness = %+v, want the issue's mobile project and rule a5", report.Resolution)
	}
}

func TestUnknownProjectBlocksEvenWhenAnAgentRuleMatches(t *testing.T) {
	directory := testDirectory()
	directory.Projects = nil
	envs := newFakeEnvironments(envView("build-b", 1))
	service := newTestService(t, envs, directory)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{agentRule()}, 0)

	preview, err := service.Preview(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview.Outcome != OutcomeUnknown || preview.RuleID != "a3" || preview.Environment != nil {
		t.Fatalf("preview = %+v, want unknown with the agent rule shown but no environment", preview)
	}

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want unknown", report.Outcome)
	}
	if got := checkByKey(t, report, "rules").Status; got != OutcomeUnknown {
		t.Fatalf("rules check = %q, want unknown", got)
	}
	if report.Primary == nil || report.Primary.Code != "project_unknown" {
		t.Fatalf("primary = %+v, want project_unknown", report.Primary)
	}
	if _, err := service.Bind(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
		AttemptID: "attempt-1",
	}); !errors.Is(err, ErrEnvironmentUnknown) {
		t.Fatalf("Bind err = %v, want ErrEnvironmentUnknown", err)
	}
	if envs.acquires != 0 {
		t.Fatalf("unknown project reserved %d slots", envs.acquires)
	}
}

func TestReadinessUnknownProviderIsUnknown(t *testing.T) {
	directory := testDirectory()
	directory.Runtimes[0].Provider = ""
	envs := newFakeEnvironments(envView("build-a", 1))
	service := newTestService(t, envs, directory)
	ctx := context.Background()
	draftAndPublish(t, service, []Rule{combinedRule()}, 0)

	report, err := service.Readiness(ctx, Request{
		ServerID: "atlas", WorkspaceID: "core", AgentID: "dev", IssueID: "issue-1",
	}, catalog.Grant{})
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if report.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want unknown", report.Outcome)
	}
	if got := checkByKey(t, report, "runtime").Status; got != OutcomeUnknown {
		t.Fatalf("runtime check = %q, want unknown", got)
	}
	if got := checkByKey(t, report, "cli_agent").Status; got != OutcomeUnknown {
		t.Fatalf("agent CLI check = %q, want unknown", got)
	}
	if report.Primary == nil || report.Primary.Code != "provider_unknown" {
		t.Fatalf("primary = %+v, want provider_unknown", report.Primary)
	}
}
