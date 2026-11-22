package rules

import (
	"context"
	"fmt"
	"time"

	"github.com/tsic404/foreman/internal/catalog"
)

// Check is one item in a readiness report. The five status values are the same
// five outcomes the report can carry, so an item and the whole answer never use
// different vocabularies (02-contracts V2-C01).
type Check struct {
	Key         string       `json:"key"`
	Label       string       `json:"label"`
	Status      Outcome      `json:"status"`
	Detail      string       `json:"detail"`
	EvidenceAt  string       `json:"evidence_at,omitempty"`
	Remediation *Remediation `json:"remediation,omitempty"`
}

// ReadinessReport is the read-only answer for one Issue. It carries the shared
// resolution (rule, environment and CLI/config facts) plus the per-item
// verdicts the console shows.
type ReadinessReport struct {
	Resolution
	Checks  []Check  `json:"checks"`
	Primary *Finding `json:"primary,omitempty"`
}

// Readiness checks one Issue without reserving a slot, recording a session,
// reading a Secret or touching the environment registry. It is the same
// resolver the real dispatch uses, limited to evidence reads (02-contracts
// V2-C01).
func (s *Service) Readiness(ctx context.Context, req Request, grant catalog.Grant) (ReadinessReport, error) {
	req.Reserve = false
	req.PriorSessionID = ""
	req.AttemptID = ""
	if req.IssueID == "" {
		res := s.blankResolution(req)
		res.Outcome = OutcomeConfigBlocked
		res.Findings = append(res.Findings, Finding{
			Code: "request_incomplete", Message: "an issue is required",
			Remediation: remediationAgents,
		})
		return ReadinessReport{
			Resolution: res,
			Checks:     s.readinessChecks(res),
			Primary:    primaryFinding(res, OutcomeConfigBlocked),
		}, nil
	}
	// The Issue is authoritative for the project identity; a caller-supplied
	// project would let the check answer for a different Issue.
	req.ProjectID = ""
	res, err := s.resolve(ctx, req, grant)
	if err != nil {
		return ReadinessReport{}, err
	}
	if res.RuntimeID != "" && res.Provider != "" &&
		(res.CLIs.Platform == "" || res.Config.Readability == "") {
		s.resolveCLIAndConfig(ctx, &res, req, res.Provider)
	}
	if s.cfg.Configs == nil && res.Provider != "" && !hasFinding(res, "config_unknown") {
		res.Findings = append(res.Findings, Finding{
			Code: "config_unknown", Message: "the Agent configuration source is not wired",
			Remediation: remediationConfigs,
		})
	}
	checks := s.readinessChecks(res)
	outcome := readinessOutcome(checks)
	res.Outcome = outcome
	return ReadinessReport{
		Resolution: res,
		Checks:     checks,
		Primary:    primaryFinding(res, outcome),
	}, nil
}

func (s *Service) blankResolution(req Request) Resolution {
	return Resolution{
		ServerID: req.ServerID, WorkspaceID: req.WorkspaceID, AgentID: req.AgentID,
		IssueID: req.IssueID, ProjectID: req.ProjectID,
		RuleDecisions:        []RuleDecision{},
		EnvironmentDecisions: []EnvironmentDecision{},
		Findings:             []Finding{},
		CheckedAt:            s.now(),
		Template: TemplateView{
			Name:      "foreman-job",
			Namespace: s.cfg.JobNamespace,
			Note:      "the fixed template is rendered by the Job builder; a rule never changes it",
		},
	}
}

func (s *Service) readinessChecks(res Resolution) []Check {
	checkedAt := res.CheckedAt.UTC().Format(time.RFC3339)
	sourceAt := res.SourceSyncedAt
	if sourceAt == "" {
		sourceAt = checkedAt
	}
	checks := make([]Check, 0, 10)
	add := func(key, label string, status Outcome, detail, evidenceAt string, remediation *Remediation) {
		checks = append(checks, Check{
			Key: key, Label: label, Status: status, Detail: detail,
			EvidenceAt: evidenceAt, Remediation: remediation,
		})
	}
	agents := remediationAgents
	servers := remediationServers
	environments := remediationEnvironments
	rules := remediationRules
	clis := remediationCLIs
	configs := remediationConfigs

	if hasFinding(res, "request_incomplete") {
		add("request", "Issue 输入", OutcomeConfigBlocked,
			firstFindingMessage(res, "request_incomplete"), checkedAt, &agents)
	}

	switch {
	case hasFinding(res, "source_unknown", "source_unsynced", "source_stale"):
		add("source", "来源目录", OutcomeUnknown, firstFindingMessage(res,
			"source_unknown", "source_unsynced", "source_stale"), sourceAt, &servers)
	case !res.sourceKnown:
		add("source", "来源目录", OutcomeUnknown, "来源目录未读取", sourceAt, &servers)
	default:
		detail := "目录代际可读"
		if res.SourceGeneration > 0 {
			detail = fmt.Sprintf("目录代际 %d 可读", res.SourceGeneration)
		}
		add("source", "来源目录", OutcomeReady, detail, sourceAt, nil)
	}

	switch {
	case hasFinding(res, "agent_unknown"):
		add("agent", "Agent 实际绑定", OutcomeUnknown,
			firstFindingMessage(res, "agent_unknown"), checkedAt, &agents)
	case !res.agentKnown:
		add("agent", "Agent 实际绑定", OutcomeUnknown, "Agent 实际绑定未读取", checkedAt, &agents)
	case hasFinding(res, "unmanaged_runtime"):
		add("agent", "Agent 实际绑定", OutcomeUnmanaged,
			firstFindingMessage(res, "unmanaged_runtime"), checkedAt, &agents)
	case hasFinding(res, "runtime_unbound"):
		add("agent", "Agent 实际绑定", OutcomeConfigBlocked,
			firstFindingMessage(res, "runtime_unbound"), checkedAt, &agents)
	default:
		add("agent", "Agent 实际绑定", OutcomeReady,
			fmt.Sprintf("管理归属 %s", res.Management), checkedAt, nil)
	}

	switch {
	case hasFinding(res, "runtime_unbound"):
		add("runtime", "实际 runtime", OutcomeConfigBlocked,
			firstFindingMessage(res, "runtime_unbound"), checkedAt, &agents)
	case !res.bindingKnown:
		add("runtime", "实际 runtime", OutcomeUnknown, "实际 runtime 未确定", checkedAt, &agents)
	case res.RuntimeID == "":
		add("runtime", "实际 runtime", OutcomeUnknown, "runtime 未确定", checkedAt, &agents)
	case res.Provider == "":
		add("runtime", "实际 runtime", OutcomeUnknown,
			fmt.Sprintf("runtime %s 没有已知 provider", res.RuntimeID), checkedAt, &agents)
	default:
		add("runtime", "实际 runtime", OutcomeReady,
			fmt.Sprintf("runtime %s (%s)", res.RuntimeID, res.Provider), checkedAt, nil)
	}

	switch {
	case !res.bindingKnown:
		add("project", "Issue 项目", OutcomeUnknown, "项目身份未读取", checkedAt, &servers)
	case hasFinding(res, "project_unknown"):
		add("project", "Issue 项目", OutcomeUnknown,
			firstFindingMessage(res, "project_unknown"), checkedAt, &servers)
	case res.ProjectID == "":
		add("project", "Issue 项目", OutcomeReady, "Issue 无项目归属", checkedAt, nil)
	default:
		add("project", "Issue 项目", OutcomeReady,
			fmt.Sprintf("project %s", res.ProjectID), checkedAt, nil)
	}

	switch {
	case !res.rulesKnown:
		add("rules", "发布规则", OutcomeUnknown, "发布规则未读取", checkedAt, &rules)
	case hasFinding(res, "project_unknown"):
		add("rules", "发布规则", OutcomeUnknown,
			"项目身份未知，规则命中未判定", checkedAt, &rules)
	case hasFinding(res, "rule_conflict"):
		add("rules", "发布规则", OutcomeConfigBlocked,
			firstFindingMessage(res, "rule_conflict"), checkedAt, &rules)
	case hasFinding(res, "continuation_environment_mismatch"):
		add("rules", "发布规则", OutcomeConfigBlocked,
			firstFindingMessage(res, "continuation_environment_mismatch"), checkedAt, &rules)
	case hasFinding(res, "no_matching_rule"):
		status := OutcomeConfigBlocked
		detail := firstFindingMessage(res, "no_matching_rule")
		if hasFinding(res, "project_unknown") {
			status = OutcomeUnknown
			detail = "项目身份未知，无法判定项目规则是否命中"
		}
		add("rules", "发布规则", status, detail, checkedAt, &rules)
	case res.RuleID == "":
		add("rules", "发布规则", OutcomeUnknown, "规则选择未完成", checkedAt, &rules)
	default:
		add("rules", "发布规则", OutcomeReady,
			fmt.Sprintf("规则 %s (%s)，发布修订 %d", res.RuleID, res.RuleLevel, res.RulesRevision),
			checkedAt, nil)
	}

	switch {
	case hasFinding(res, "environment_registry_unreadable"):
		add("environment", "运行环境", OutcomeUnknown,
			firstFindingMessage(res, "environment_registry_unreadable"), checkedAt, &environments)
	case !res.environmentKnown:
		add("environment", "运行环境", OutcomeUnknown, "环境注册表未读取", checkedAt, &environments)
	case hasFinding(res, "environment_missing", "no_environment"):
		add("environment", "运行环境", OutcomeConfigBlocked,
			firstFindingMessage(res, "environment_missing", "no_environment"), checkedAt, &environments)
	case hasFinding(res, "environment_saturated"):
		add("environment", "运行环境", OutcomeTemporarilySaturated,
			firstFindingMessage(res, "environment_saturated"), checkedAt, &environments)
	case res.Environment == nil:
		add("environment", "运行环境", OutcomeUnknown, "未选择环境", checkedAt, &environments)
	default:
		add("environment", "运行环境", OutcomeReady,
			fmt.Sprintf("环境 %s，快照空闲 %d/%d", res.Environment.ID, res.Environment.FreeSlots, res.Environment.Concurrency),
			checkedAt, nil)
	}

	switch {
	case hasFinding(res, "environment_saturated"):
		add("capacity", "当前槽位", OutcomeTemporarilySaturated,
			firstFindingMessage(res, "environment_saturated"), checkedAt, &environments)
	case res.Environment == nil:
		add("capacity", "当前槽位", OutcomeUnknown, "环境未确定，无法读取容量", checkedAt, &environments)
	case res.Environment.FreeSlots <= 0:
		add("capacity", "当前槽位", OutcomeTemporarilySaturated,
			fmt.Sprintf("环境 %s 快照空闲 0/%d", res.Environment.ID, res.Environment.Concurrency),
			checkedAt, &environments)
	default:
		add("capacity", "当前槽位", OutcomeReady,
			fmt.Sprintf("环境 %s 快照空闲 %d/%d", res.Environment.ID, res.Environment.FreeSlots, res.Environment.Concurrency),
			checkedAt, nil)
	}

	switch {
	case !res.cliKnown:
		add("cli_multica", "Multica CLI", OutcomeUnknown, "Multica CLI 发布事实未读取", checkedAt, &clis)
	case hasFinding(res, "multica_cli_missing"):
		add("cli_multica", "Multica CLI", OutcomeConfigBlocked,
			firstFindingMessage(res, "multica_cli_missing"), checkedAt, &clis)
	case hasFinding(res, "cli_unknown"):
		add("cli_multica", "Multica CLI", OutcomeUnknown,
			firstFindingMessage(res, "cli_unknown"), checkedAt, &clis)
	case res.CLIs.MulticaDigest == "":
		add("cli_multica", "Multica CLI", OutcomeUnknown, "Multica CLI 发布事实未读取", checkedAt, &clis)
	default:
		add("cli_multica", "Multica CLI", OutcomeReady,
			fmt.Sprintf("%s %s", res.CLIs.MulticaVersion, res.CLIs.MulticaDigest), checkedAt, nil)
	}

	switch {
	case !res.cliKnown:
		add("cli_agent", "Agent CLI", OutcomeUnknown, "Agent CLI 发布事实未读取", checkedAt, &clis)
	case res.Provider == "":
		add("cli_agent", "Agent CLI", OutcomeUnknown, "provider 未确定", checkedAt, &clis)
	case hasFinding(res, "agent_cli_missing"):
		add("cli_agent", "Agent CLI", OutcomeConfigBlocked,
			firstFindingMessage(res, "agent_cli_missing"), checkedAt, &clis)
	case hasFinding(res, "cli_unknown"):
		add("cli_agent", "Agent CLI", OutcomeUnknown,
			firstFindingMessage(res, "cli_unknown"), checkedAt, &clis)
	case res.CLIs.AgentDigest == "":
		add("cli_agent", "Agent CLI", OutcomeUnknown, "Agent CLI 发布事实未读取", checkedAt, &clis)
	default:
		add("cli_agent", "Agent CLI", OutcomeReady,
			fmt.Sprintf("%s %s", res.CLIs.AgentVersion, res.CLIs.AgentDigest), checkedAt, nil)
	}

	switch {
	case hasFinding(res, "config_secret_missing"):
		add("config", "Agent 配置 Secret", OutcomeConfigBlocked,
			firstFindingMessage(res, "config_secret_missing"), checkedAt, &configs)
	case hasFinding(res, "config_unknown", "config_unverified"):
		add("config", "Agent 配置 Secret", OutcomeUnknown,
			firstFindingMessage(res, "config_unknown", "config_unverified"), checkedAt, &configs)
	case res.Config.Readability == "":
		add("config", "Agent 配置 Secret", OutcomeUnknown, "配置事实未读取", checkedAt, &configs)
	case res.Config.Known && res.Config.Readability == "not_required":
		add("config", "Agent 配置 Secret", OutcomeReady, "所选配置无需 Secret", checkedAt, nil)
	case res.Config.Known && res.Config.Readability == "unverified":
		add("config", "Agent 配置 Secret", OutcomeUnknown,
			"配置 Secret 缺少投影身份可读性验证事实", checkedAt, &configs)
	default:
		add("config", "Agent 配置 Secret", OutcomeUnknown, "配置事实未读取", checkedAt, &configs)
	}
	return checks
}

func readinessOutcome(checks []Check) Outcome {
	worst := OutcomeReady
	for _, check := range checks {
		if checkPriority(check.Status) > checkPriority(worst) {
			worst = check.Status
		}
	}
	return worst
}

func checkPriority(outcome Outcome) int {
	switch outcome {
	case OutcomeUnmanaged:
		return 4
	case OutcomeConfigBlocked:
		return 3
	case OutcomeUnknown:
		return 2
	case OutcomeTemporarilySaturated:
		return 1
	default:
		return 0
	}
}

func primaryFinding(res Resolution, outcome Outcome) *Finding {
	if outcome == OutcomeReady {
		return nil
	}
	codes := map[Outcome][]string{
		OutcomeUnmanaged:            {"unmanaged_runtime"},
		OutcomeConfigBlocked:        {"request_incomplete", "runtime_unbound", "rule_conflict", "no_matching_rule", "environment_missing", "no_environment", "continuation_environment_mismatch", "multica_cli_missing", "agent_cli_missing", "config_secret_missing", "environment_unavailable"},
		OutcomeUnknown:              {"source_unknown", "source_unsynced", "source_stale", "agent_unknown", "project_unknown", "environment_registry_unreadable", "provider_unknown", "cli_unknown", "config_unknown", "config_unverified"},
		OutcomeTemporarilySaturated: {"environment_saturated"},
	}
	for _, finding := range res.Findings {
		for _, code := range codes[outcome] {
			if finding.Code == code {
				copy := finding
				return &copy
			}
		}
	}
	if len(res.Findings) > 0 {
		copy := res.Findings[0]
		return &copy
	}
	return nil
}

func hasFinding(res Resolution, codes ...string) bool {
	for _, finding := range res.Findings {
		for _, code := range codes {
			if finding.Code == code {
				return true
			}
		}
	}
	return false
}

func firstFindingMessage(res Resolution, codes ...string) string {
	for _, finding := range res.Findings {
		for _, code := range codes {
			if finding.Code == code {
				return finding.Message
			}
		}
	}
	return "未判定"
}
