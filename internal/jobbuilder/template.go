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
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

// TemplatePath is the fixed template carrier location (03-contracts.md §5.4):
// the Deployment mounts the optional ConfigMap foreman-job-template here with
// key job-overlay.yaml. Enablement is file existence only — there is no env
// switch, so a setting cannot point at a file the Deployment never mounted.
// Path and key keep their names for deployment compatibility; the content is
// the complete Job template, not a merge fragment (P-006 §A4).
const TemplatePath = "/etc/foreman/job-template/job-overlay.yaml"

// The startup log carries three fixed literals from two places in
// 03-contracts.md §5.4: the risk declaration (§5.4 风险告知) and the hash line
// plus effect statement (§5.4 运维口径①). The declaration and the hash line are
// greped verbatim by AC-29 / TC-tech-job-07; the effect statement rests on
// design text only — no AC or TC protects it. They stay constants so call
// sites cannot drift from the contract text.

// templateRiskDeclaration is the statement the startup log carries on both
// paths — template present and absent (03-contracts.md §5.4 风险告知, restated
// in ADR-016): the template is not a security boundary, and Foreman does not
// check it.
const templateRiskDeclaration = "Foreman 不对 Job 模板做任何校验，模板内容不构成安全边界"

// templateHashLinePrefix starts the hash line, whose fixed literal is
// `模板内容哈希 sha256=<64 lowercase hex>` over the template file's bytes. The
// line is a message, not an attr: an attr key carrying this prefix would
// render `"模板内容哈希 sha256":"<hex>"`, breaking the `=` form the greps fix.
const templateHashLinePrefix = "模板内容哈希 sha256="

// templateEffectStatement is the 生效状态声明 (§5.4 运维口径①): the file is
// read once at startup, so a ConfigMap edit takes effect only after a restart.
const templateEffectStatement = "运行中 CM 变更不生效"

// strictJobSerializer rejects unknown fields instead of dropping them: a
// misspelled "contianers" must fail loudly rather than render a Job without
// containers (P-006 §A4).
var strictJobSerializer = jsonserializer.NewSerializerWithOptions(
	jsonserializer.DefaultMetaFactory, scheme.Scheme, scheme.Scheme,
	jsonserializer.SerializerOptions{Yaml: false, Strict: true},
)

// LoadJobTemplate is the startup gate (called by NewBuilder): it reads the
// optional Job template and parses it. A missing file means present=false —
// the built-in default template (AC-20 regression), never an error. Only two
// failures exist (P-006 §A4): the file cannot be read, or its content is not a
// batch/v1 Job — a YAML parse error, an unknown field under strict decoding,
// or an apiVersion/kind that is not the Job's.
func LoadJobTemplate(cfg Config) (map[string]any, bool, error) {
	path := cfg.TemplatePath
	if path == "" {
		path = TemplatePath
	}
	log := cfg.logger()

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Warn("job template absent: using the built-in default template",
			"template", path, "declaration", templateRiskDeclaration,
			"effect", templateEffectStatement)
		return nil, false, nil
	}
	if err != nil {
		// Failure class 1: the file cannot be read, so the hash is out of
		// reach — the line says so instead of carrying a hash line.
		log.Error("job template file unreadable: 文件不可读，模板内容哈希不可得",
			"template", path, "err", err.Error(), "declaration", templateRiskDeclaration)
		return nil, false, fmt.Errorf("read job template %s: %w", path, err)
	}

	sum := sha256.Sum256(data)
	hashLine := templateHashLinePrefix + hex.EncodeToString(sum[:])

	template, err := parseJobTemplate(path, data)
	if err != nil {
		log.Error("job template rejected: refusing to start",
			"template", path, "err", err.Error(), "declaration", templateRiskDeclaration)
		log.Error(hashLine, "template", path)
		return nil, true, err
	}

	log.Info("job template accepted: used verbatim apart from Foreman's own writes",
		"template", path, "declaration", templateRiskDeclaration,
		"effect", templateEffectStatement)
	log.Info(hashLine, "template", path)
	return template, true, nil
}

// parseJobTemplate decodes the template file into the raw map each Build
// copies. It decides the two class-2 rules the file can break: the document
// must declare the Job's apiVersion/kind (a bare manifest fragment is not a
// template), and strict decoding must accept every field. Numbers are kept as
// json.Number: the map is written back per Build, and a float64 round trip
// would silently rewrite int64 fields above 2^53.
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

// renderTemplate renders one task from the startup-parsed template: a fresh
// deep copy per Build (P-006 §A1.3), the placeholder substitution, then the
// typed Job. Reusing the parsed object would carry task A's per-task values —
// above all the fm-<task_id>-cred Secret reference — into task B.
func renderTemplate(template map[string]any, v placeholderValues) (*batchv1.Job, error) {
	rendered := deepCopyTemplate(template)
	dropPreferredAffinityItems(rendered, v.preferredNode)
	if _, err := substituteValue(rendered, v); err != nil {
		return nil, err
	}

	raw, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("render job template: %w", err)
	}
	var job batchv1.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("render job template: %w", err)
	}
	return &job, nil
}

// Registered template placeholders (05-modules/job-template.md 占位符与回归基线).
// Anything else written as <...> stays verbatim — an unregistered literal is
// never guessed at, so a typo cannot silently become a value.
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
	placeholderPreferredNode   = "<preferred node>"
	placeholderSessionNode     = "<session node>"
)

// preferredNodePlaceholders are the two spellings of the one conditional block
// the design corpus carries for the node-reuse affinity item: P-006 §A1.2 and
// 03-contracts §5.4 write <preferred node>, the built-in default template and
// the placeholder table in 05-modules/job-template.md write <session node>.
// Both name the same decision and drop their item together when none resolves.
var preferredNodePlaceholders = []string{placeholderPreferredNode, placeholderSessionNode}

// placeholderRegistry is the substitution set in one place, so the walk and the
// documented table cannot drift.
var placeholderRegistry = []string{
	placeholderTaskID, placeholderAgentID, placeholderWorkspaceID,
	placeholderIssueID, placeholderIssueIdentifier, placeholderAutopilotRunID,
	placeholderChatSessionID, placeholderJobRuntimeID, placeholderClaimedAt,
	placeholderJobImage, placeholderPreferredNode, placeholderSessionNode,
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
	case placeholderPreferredNode, placeholderSessionNode:
		return v.preferredNode
	default:
		return ""
	}
}

// substituteValue rewrites the registered placeholders in every string value
// of the template — map values and list elements alike (P-006 §A1.2 作用域).
// An unresolved node-reuse placeholder outside the preferred affinity item is
// an error rather than a deletion: P-006 §A1.2 的条件删除只覆盖那个 affinity
// 项，别处的占位符若靠删除承载键来「解决」，args/env 整条会静默消失。
func substituteValue(node any, v placeholderValues) (any, error) {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			replaced, err := substituteValue(child, v)
			if err != nil {
				return nil, err
			}
			value[key] = replaced
		}
		return value, nil
	case []any:
		for i, child := range value {
			replaced, err := substituteValue(child, v)
			if err != nil {
				return nil, err
			}
			value[i] = replaced
		}
		return value, nil
	case string:
		return substituteString(value, v)
	default:
		return node, nil
	}
}

// substituteString rewrites the registered placeholders inside one string
// value. A node-reuse placeholder with no resolved node has no value to write:
// dropPreferredAffinityItems has already removed the items it may delete, so
// reaching here means the template put it somewhere else — fail closed.
func substituteString(value string, v placeholderValues) (string, error) {
	if v.preferredNode == "" && containsPreferredNode(value) {
		return "", fmt.Errorf("job template: %s (%s) outside a preferred-affinity item has no value: no soft-affinity node was resolved for this task",
			placeholderPreferredNode, placeholderSessionNode)
	}
	if !strings.Contains(value, "<") {
		return value, nil
	}
	for _, placeholder := range placeholderRegistry {
		if strings.Contains(value, placeholder) {
			value = strings.ReplaceAll(value, placeholder, v.replacement(placeholder))
		}
	}
	return value, nil
}

// dropPreferredAffinityItems removes the preferred-affinity items carrying a
// node-reuse placeholder when no node was resolved. The placeholder's home is
// the whole item: dropping only the placeholder string would leave an "In"
// term with an empty values list, which the apiserver rejects. Structures the
// deletion empties are pruned — the item, its list, nodeAffinity, affinity —
// so a template whose only affinity was that item ends up without one, exactly
// like the built-in default template's conditional block.
func dropPreferredAffinityItems(template map[string]any, node string) {
	if node != "" {
		return
	}
	spec := nestedMap(template, "spec", "template", "spec")
	affinity, _ := spec["affinity"].(map[string]any)
	nodeAffinity, _ := affinity["nodeAffinity"].(map[string]any)
	items, _ := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]any)

	kept := make([]any, 0, len(items))
	dropped := false
	for _, item := range items {
		if containsPreferredNode(item) {
			dropped = true
			continue
		}
		kept = append(kept, item)
	}
	if !dropped {
		return
	}
	if len(kept) > 0 {
		nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"] = kept
		return
	}
	delete(nodeAffinity, "preferredDuringSchedulingIgnoredDuringExecution")
	if len(nodeAffinity) == 0 {
		delete(affinity, "nodeAffinity")
	}
	if len(affinity) == 0 {
		delete(spec, "affinity")
	}
}

// containsPreferredNode reports whether node is, or contains, either spelling
// of the node-reuse placeholder.
func containsPreferredNode(node any) bool {
	switch value := node.(type) {
	case map[string]any:
		for _, child := range value {
			if containsPreferredNode(child) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if containsPreferredNode(child) {
				return true
			}
		}
	case string:
		for _, placeholder := range preferredNodePlaceholders {
			if strings.Contains(value, placeholder) {
				return true
			}
		}
	}
	return false
}

// nestedMap walks a chain of map keys, returning nil when any step is absent
// or is not a map.
func nestedMap(root map[string]any, keys ...string) map[string]any {
	current := root
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current
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
