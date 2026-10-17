package jobbuilder

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

// TemplatePath is the fixed template carrier location (03-contracts.md §5.4):
// the Deployment mounts the required ConfigMap foreman-job-template here with
// key job-overlay.yaml, as a non-optional read-only volume. There is no env
// switch — the file's presence is the deployment's contract, so a setting
// cannot point at a file the Deployment never mounted.
const TemplatePath = "/etc/foreman/job-template/job-overlay.yaml"

// The startup log carries fixed literals from two places in 03-contracts.md
// §5.4: the risk declaration (§5.4 风险告知) and the hash line plus effect
// statement (§5.4 运维口径①). The declaration and the hash line are greped
// verbatim by AC-29 / TC-tech-job-07; the effect statement rests on design text
// only — no AC or TC protects it. They stay constants so call sites cannot
// drift from the contract text.

// templateRiskDeclaration is the statement the startup log always carries
// (03-contracts.md §5.4 风险告知): the template is not a security boundary,
// and Foreman does not check it.
const templateRiskDeclaration = "Foreman 不对 Job 模板做任何校验，模板内容不构成安全边界"

// templateHashLinePrefix starts the hash line, whose fixed literal is
// `模板内容哈希 sha256=<64 lowercase hex>` over the template file's bytes. The
// line is a message, not an attr: an attr key carrying this prefix would
// render `"模板内容哈希 sha256":"<hex>"`, breaking the `=` form the greps fix.
// It is printed only when the file was readable — the missing and unreadable
// paths say the hash is out of reach instead.
const templateHashLinePrefix = "模板内容哈希 sha256="

// templateEffectStatement is the 生效状态声明 (§5.4 运维口径①): the file is
// read once at startup, so a ConfigMap edit takes effect only after a restart.
const templateEffectStatement = "运行中 CM 变更不生效"

// strictJobSerializer rejects unknown fields instead of dropping them: a
// misspelled "contianers" must fail loudly rather than render a Job without
// containers (03-contracts §5.4 启动期判定).
var strictJobSerializer = jsonserializer.NewSerializerWithOptions(
	jsonserializer.DefaultMetaFactory, scheme.Scheme, scheme.Scheme,
	jsonserializer.SerializerOptions{Yaml: false, Strict: true},
)

// LoadJobTemplate is the startup gate (called by NewBuilder): it reads the
// required Job template and parses it. The template is a required deployment
// artifact, so every failure refuses startup — there is no fallback and no
// default template (ADR-030). Four failure classes, each logged on its own
// line: the file does not exist, the file cannot be read (the hash is out of
// reach — the line says so), the content is not a batch/v1 Job (a YAML parse
// error, an unknown field under strict decoding, or an apiVersion/kind that is
// not the Job's — the hash line is printed here, the bytes being readable), or
// metadata.name is not a legal name format (ADR-034 decision 3: the hash line
// is printed here too). Nothing else about the template is judged.
func LoadJobTemplate(cfg Config) (map[string]any, error) {
	path := cfg.TemplatePath
	if path == "" {
		path = TemplatePath
	}
	log := cfg.logger()

	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		log.Error("job template file missing: 模板文件不存在，拒绝启动",
			"template", path, "declaration", templateRiskDeclaration)
		return nil, fmt.Errorf("job template %s does not exist: %w", path, err)
	case err != nil:
		log.Error("job template file unreadable: 文件不可读，模板内容哈希不可得",
			"template", path, "err", err.Error(), "declaration", templateRiskDeclaration)
		return nil, fmt.Errorf("read job template %s: %w", path, err)
	}

	sum := sha256.Sum256(data)
	hashLine := templateHashLinePrefix + hex.EncodeToString(sum[:])

	template, err := parseJobTemplate(path, data)
	if err != nil {
		log.Error("job template rejected: refusing to start",
			"template", path, "err", err.Error(), "declaration", templateRiskDeclaration)
		log.Error(hashLine, "template", path)
		return nil, err
	}
	// Failure class 4 (ADR-034 decision 3): metadata.name is the name format
	// string, and a non-empty one that leaves the character-level closure —
	// an unregistered variable, a missing unique task segment, an over-long
	// worst case, a minimum rendering that is not DNS-1123 — refuses startup.
	// Falling back silently would hide the deployer's format.
	if err := validateNameFormat(templateNameFormat(template)); err != nil {
		log.Error("job template name format rejected: 名字格式非法，拒绝启动",
			"template", path, "err", err.Error(), "declaration", templateRiskDeclaration)
		log.Error(hashLine, "template", path)
		return nil, err
	}

	log.Info("job template accepted: used verbatim apart from Foreman's own writes",
		"template", path, "declaration", templateRiskDeclaration,
		"effect", templateEffectStatement)
	log.Info(hashLine, "template", path)
	return template, nil
}

// parseJobTemplate decodes the template file into the raw map each Build
// copies. It decides the two rules of failure class 3 the file can break: the
// document must declare the Job's apiVersion/kind (a bare manifest fragment is
// not a template), and strict decoding must accept every field. Numbers are
// kept as json.Number: the map is written back per Build, and a float64 round
// trip would silently rewrite int64 fields above 2^53.
func parseJobTemplate(path string, data []byte) (map[string]any, error) {
	jsonBytes, err := utilyaml.ToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("job template %s is not parseable as YAML: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.UseNumber()
	var template map[string]any
	if err := decoder.Decode(&template); err != nil {
		return nil, fmt.Errorf("job template %s must be a Job manifest object: %w", path, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("job template %s must be a single Job manifest object", path)
	}
	if template == nil {
		// An empty document (only comments) is not a Job; refusing it catches a
		// ConfigMap key created empty by mistake.
		return nil, fmt.Errorf("job template %s must be a Job manifest object, got an empty document", path)
	}
	if apiVersion, _ := template["apiVersion"].(string); apiVersion != "batch/v1" {
		return nil, fmt.Errorf("job template %s must declare apiVersion batch/v1, got %v", path, template["apiVersion"])
	}
	if kind, _ := template["kind"].(string); kind != "Job" {
		return nil, fmt.Errorf("job template %s must declare kind Job, got %v", path, template["kind"])
	}
	if _, _, err := strictJobSerializer.Decode(jsonBytes, nil, &batchv1.Job{}); err != nil {
		return nil, fmt.Errorf("job template %s rejected by strict decoding: %w", path, err)
	}
	return template, nil
}

// resolvePreferredNode settles the node-reuse item on the rendered Job
// (03-contracts §5.4「`<preferred node>` 条件块」): every values entry reading
// exactly "<preferred node>" at
// affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution[*]
// .preference.matchExpressions[*].values takes the resolved node's name, or —
// when no node resolved — its whole item is dropped and the structures the
// deletion empties are collapsed. The literal elsewhere is untouched: no
// replacement, no deletion, no error. An item dropped here covers both the
// reuse conditions (no session, unknown node, reuse off, saturated node) and
// the nodeSelector axis's suppression (ADR-036).
func resolvePreferredNode(job *batchv1.Job, node string) {
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		return
	}
	preferred := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	kept := make([]corev1.PreferredSchedulingTerm, 0, len(preferred))
	dropped := false
	for _, item := range preferred {
		switch {
		case node != "" && nameRecognitionValues(&item, node):
			// The item carries the recognition element and now names the node.
			kept = append(kept, item)
		case node == "" && hasRecognitionValue(&item):
			dropped = true
		default:
			kept = append(kept, item)
		}
	}
	if !dropped {
		return
	}
	if len(kept) > 0 {
		affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = kept
		return
	}
	// The empty-structure collapse is part of「删除该项」: the item, its list,
	// nodeAffinity, affinity — until something non-empty is left.
	affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = nil
	if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.NodeAffinity = nil
	}
	if affinity.NodeAffinity == nil && affinity.PodAffinity == nil && affinity.PodAntiAffinity == nil {
		job.Spec.Template.Spec.Affinity = nil
	}
}

// nameRecognitionValues rewrites every recognition element of one preferred
// item to the resolved node, reporting whether the item carried one.
func nameRecognitionValues(item *corev1.PreferredSchedulingTerm, node string) bool {
	found := false
	for i := range item.Preference.MatchExpressions {
		expression := &item.Preference.MatchExpressions[i]
		for j, value := range expression.Values {
			if value == placeholderPreferredNode {
				expression.Values[j] = node
				found = true
			}
		}
	}
	return found
}

// hasRecognitionValue reports whether one preferred item carries a recognition
// element. An entry whose value merely contains the literal is not a
// recognition element — the design pins an exact value — so it is left to the
// apiserver's own judgment.
func hasRecognitionValue(item *corev1.PreferredSchedulingTerm) bool {
	for i := range item.Preference.MatchExpressions {
		for _, value := range item.Preference.MatchExpressions[i].Values {
			if value == placeholderPreferredNode {
				return true
			}
		}
	}
	return false
}

// Registered template placeholders (05-modules/job-template.md 占位符与回归基线).
// Anything else written as <...> stays verbatim — an unregistered literal is
// never guessed at, so a typo cannot silently become a value.
// placeholderPreferredNode is registered too, but as the substitution set's one
// exception: it is consumed by its recognition site, never by the generic walk.
const (
	placeholderTaskID          = "<task_id>"
	placeholderAgentID         = "<agent_id>"
	placeholderWorkspaceID     = "<workspace_id>"
	placeholderIssueID         = "<issue_id>"
	placeholderIssueIdentifier = "<issue-identifier>"
	placeholderAutopilotRunID  = "<autopilot_run_id>"
	placeholderChatSessionID   = "<chat_session_id>"
	placeholderJobRuntimeID    = "<job runtime uuid>"
	placeholderClaimedAt       = "<RFC3339>"
	placeholderJobImage        = "<job-image>"
	placeholderJobName         = "<job-name>"
	placeholderDaemonID        = "<daemon-id>"
	placeholderAgentName       = "<agent-name>"
	placeholderPreferredNode   = "<preferred node>"
)

// placeholderRegistry is the substitution set in one place, so the walk and the
// documented table cannot drift. <preferred node> is deliberately absent: the
// design registers it as the substitution set's one exception (03-contracts
// §5.4「占位符替换」第 1 条) — only its recognition site takes a value or drops
// its item (resolvePreferredNode), and anywhere else the literal stays verbatim
// like any other unregistered one. The name-face variables (<issue-segment>,
// <agent-segment>, <task-key>) are absent for the same reason: they are valid
// inside metadata.name only (ADR-034 decision 6).
var placeholderRegistry = []string{
	placeholderTaskID, placeholderAgentID, placeholderWorkspaceID,
	placeholderIssueID, placeholderIssueIdentifier, placeholderAutopilotRunID,
	placeholderChatSessionID, placeholderJobRuntimeID, placeholderClaimedAt,
	placeholderJobImage, placeholderJobName, placeholderDaemonID, placeholderAgentName,
}

// placeholderValues resolves each registered placeholder for one task.
type placeholderValues struct {
	taskID          string
	agentID         string
	workspaceID     string
	issueID         string
	issueIdentifier string
	autopilotRunID  string
	chatSessionID   string
	jobRuntimeID    string
	claimedAt       string
	jobImage        string
	// jobName is the final display name (ADR-034): the name composition step
	// runs before the substitution, so a template can reference it outside
	// metadata.name — where the name itself is already the final value.
	jobName string
	// daemonID is the identity name fm-<task_id>, independently of the display
	// name (ADR-034 decision 8).
	daemonID string
	// agentName is the payload's raw agent.name (may carry uppercase, spaces
	// or non-ASCII); only the name composition normalizes it.
	agentName string
	// preferredNode is empty when reuse is off, no history exists, or the node
	// is saturated; the affinity item carrying the placeholder is then dropped
	// instead of rendered empty.
	preferredNode string
}

func (v placeholderValues) replacement(placeholder string) string {
	switch placeholder {
	case placeholderTaskID:
		return v.taskID
	case placeholderAgentID:
		return v.agentID
	case placeholderWorkspaceID:
		return v.workspaceID
	case placeholderIssueID:
		return v.issueID
	case placeholderIssueIdentifier:
		return v.issueIdentifier
	case placeholderAutopilotRunID:
		return v.autopilotRunID
	case placeholderChatSessionID:
		return v.chatSessionID
	case placeholderJobRuntimeID:
		return v.jobRuntimeID
	case placeholderClaimedAt:
		return v.claimedAt
	case placeholderJobImage:
		return v.jobImage
	case placeholderJobName:
		return v.jobName
	case placeholderDaemonID:
		return v.daemonID
	case placeholderAgentName:
		return v.agentName
	default:
		return ""
	}
}

// substituteValue rewrites the registered placeholders in every string value
// of the template — map values and list elements alike (§5.4「占位符替换」
// 第 2/3 条: substring matching over the whole object's string values).
func substituteValue(node any, v placeholderValues) any {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			value[key] = substituteValue(child, v)
		}
		return value
	case []any:
		for i, child := range value {
			value[i] = substituteValue(child, v)
		}
		return value
	case string:
		return substituteString(value, v)
	default:
		return node
	}
}

// substituteString rewrites the registered placeholders inside one string
// value. The node-reuse literal is not among them: it is handled at its
// recognition site only, so a template that writes it elsewhere keeps it
// verbatim — the apiserver, not this walk, is what refuses such a position
// (§5.4「占位符替换」第 4 条).
func substituteString(value string, v placeholderValues) string {
	if !strings.Contains(value, "<") {
		return value
	}
	for _, placeholder := range placeholderRegistry {
		if strings.Contains(value, placeholder) {
			value = strings.ReplaceAll(value, placeholder, v.replacement(placeholder))
		}
	}
	return value
}

// deepCopyTemplate clones the parsed template so Build never mutates the
// startup-parsed object (P-006 §A1.3).
func deepCopyTemplate(template map[string]any) map[string]any {
	out := make(map[string]any, len(template))
	for key, value := range template {
		out[key] = deepCopyValue(value)
	}
	return out
}

func deepCopyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return deepCopyTemplate(typed)
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = deepCopyValue(child)
		}
		return out
	default:
		return value
	}
}
