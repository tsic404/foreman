package jobbuilder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// PolicyPath is the fixed policy carrier location (02-contracts.md §8.5):
// the Deployment mounts the optional ConfigMap foreman-job-policy here with key
// policy.yaml, as an optional read-only volume. The file's absence is a legal
// default — all three axes stay None — so the path is a constant and not an env
// switch, exactly like the template carrier's.
const PolicyPath = "/etc/foreman/policy/policy.yaml"

// Policy-side startup literals (§8.7). The declaration is always printed — a
// missing policy file is a legal default, not a silent one — and the hash
// line only when the file was readable. They are independent of the template's
// own two lines and never replace them (AC-29 / AC-44).
const (
	policyEffectStatement = "策略文件在启动期一次性加载，运行中变更不生效"
	policyHashLinePrefix  = "策略文件内容哈希 sha256="
)

// AxisPolicy is one axis's declared tier: where that axis reads its definition
// from (ADR-035 decision 1). The four literals are the deployment interface.
type AxisPolicy string

const (
	AxisNone             AxisPolicy = "None"
	AxisProject          AxisPolicy = "project"
	AxisAgent            AxisPolicy = "agent"
	AxisProjectWithAgent AxisPolicy = "projectWithAgent"
)

// appliedValue renders the tier the way policy.applied carries it
// (§9.1: none / project / agent / project_with_agent).
func (a AxisPolicy) appliedValue() string {
	switch a {
	case AxisProject:
		return "project"
	case AxisAgent:
		return "agent"
	case AxisProjectWithAgent:
		return "project_with_agent"
	default:
		return "none"
	}
}

// Resolution conclusions (ADR-035 decision 4); mirrored in the
// policy.name_resolution event.
type NameConclusion string

const (
	NameResolved    NameConclusion = "resolved"
	NameAbsent      NameConclusion = "absent"
	NameAmbiguous   NameConclusion = "ambiguous"
	NameUnavailable NameConclusion = "unavailable"
)

// PolicyNameResolver is the platform-side seam ADR-035 leaves to the platform
// (02-contracts.md §8.5: the query's shape is a platform contract).
// Foreman calls it once per key at startup, never again: a nil resolver means
// the platform offers no such query, so every key concludes unavailable and
// startup continues (the documented degradation).
type PolicyNameResolver interface {
	// ResolveName reports what the platform's authority says about a policy
	// key. An error means unreachable or no such query — the caller maps it to
	// unavailable; it never becomes a startup refusal.
	ResolveName(ctx context.Context, kind, name string) (NameConclusion, error)
}

// Axis sources: the evaluation outcome of one axis
// (§9.1: the policy.applied event's *_source field).
const (
	SourceDirect       = "direct"
	SourceNoDefinition = "no_definition"
	SourceNoIdentity   = "no_identity"
)

// Project identity sources (02-contracts.md §4.1 项目身份).
const (
	ProjectSourcePayload = "payload"
	ProjectSourceAPI     = "api"
	ProjectSourceNone    = "none"
)

// Soft-affinity suppression causes (ADR-036 decision 5).
const (
	SuppressionPolicy     = "policy"
	SuppressionSaturation = "saturation"
	SuppressionNone       = "none"
)

// Policy is the startup-parsed policy table: read-only, shared by every Build,
// never reloaded while the process lives (ADR-035 decision 7).
type Policy struct {
	agents   map[string]agentEntry
	projects map[string]projectEntry
}

// agentEntry is one agent's row: the three tiers plus that agent's own
// definitions.
type agentEntry struct {
	resourcePolicy      AxisPolicy
	capPolicy           AxisPolicy
	nodeSelectorPolicy  AxisPolicy
	resources           *resourceDefinition
	capabilities        *capabilityDefinition
	nodeSelectorDefined map[string]string
}

// projectEntry is one project's row: three definitions, no tiers.
type projectEntry struct {
	resources           *resourceDefinition
	capabilities        *capabilityDefinition
	nodeSelectorDefined map[string]string
}

// resourceDefinition is the resource axis's definition body: the k8s-native
// requests/limits pair, each optional.
type resourceDefinition struct {
	requests corev1.ResourceList
	limits   corev1.ResourceList
}

// capabilityDefinition is the capability axis's definition body. drop is not a
// definition field: writing it refuses startup, so it never reaches here.
type capabilityDefinition struct {
	add []string
}

// empty reports whether the definition carries no value at all; an empty body
// is judged exactly like an absent one (ADR-035 decision 5).
func (d *resourceDefinition) empty() bool {
	return d == nil || (len(d.requests) == 0 && len(d.limits) == 0)
}

func (d *capabilityDefinition) empty() bool { return d == nil || len(d.add) == 0 }

func emptyNodeSelector(d map[string]string) bool { return len(d) == 0 }

// Evaluation is one task's three-axis outcome, before and after the merge:
// the fields map one-to-one onto the policy.applied event
// (§9.1), so the event is a rendering of this struct.
type Evaluation struct {
	resourcePolicy     AxisPolicy
	capPolicy          AxisPolicy
	nodeSelectorPolicy AxisPolicy

	resourceSource     string
	capSource          string
	nodeSelectorSource string

	// Resolved values, nil/empty when the axis is not injected.
	resources     *corev1.ResourceRequirements
	capabilityAdd []string
	nodeSelector  map[string]string

	projectSource string
	nameSource    string

	overriddenKeys []string
	// suppressed is the soft-affinity decision's cause (ADR-036).
	suppressed string
}

// writtenFaces lists the axis faces this evaluation actually writes, in the
// fixed order policy.applied documents (resources / capabilities_add /
// node_selector).
func (ev Evaluation) writtenFaces() []string {
	faces := make([]string, 0, 3)
	if ev.resources != nil {
		faces = append(faces, "resources")
	}
	if len(ev.capabilityAdd) > 0 {
		faces = append(faces, "capabilities_add")
	}
	if len(ev.nodeSelector) > 0 {
		faces = append(faces, "node_selector")
	}
	return faces
}

// filePolicy is the strict decoding target of policy.yaml. Unknown keys — including
// a misspelled axis or definition field — are refused, so a policy that does not
// mean what it says never loads.
type filePolicy struct {
	Agents   map[string]fileAgent   `json:"agents"`
	Projects map[string]fileProject `json:"projects"`
}

type fileAgent struct {
	ResourcePolicy     *string           `json:"resourcePolicy"`
	CapPolicy          *string           `json:"capPolicy"`
	NodeSelectorPolicy *string           `json:"nodeSelectorPolicy"`
	Resources          *fileResources    `json:"resources"`
	Capabilities       *fileCapabilities `json:"capabilities"`
	NodeSelector       map[string]string `json:"nodeSelector"`
}

type fileProject struct {
	Resources    *fileResources    `json:"resources"`
	Capabilities *fileCapabilities `json:"capabilities"`
	NodeSelector map[string]string `json:"nodeSelector"`
}

type fileResources struct {
	Requests map[string]string `json:"requests"`
	Limits   map[string]string `json:"limits"`
}

type fileCapabilities struct {
	Add []string `json:"add"`
	// Drop is present only so an explicit drop key is decodable and can be
	// refused with a precise reason: silently ignoring it would swallow a
	// security intent (§8.5, loading-and-failure class 3).
	Drop []string `json:"drop"`
}

// LoadPolicy is the policy startup gate (§8.5, alongside the template gate of
// §8.1). A missing file is a legal default: all three axes read None, a warn
// records it, and the fixed declaration sentence is still printed. A file
// that is unreadable, unparseable, carries an explicit drop, or whose keys the
// platform's authority reports absent or ambiguous
// refuses startup; an unreachable platform only downgrades the conclusions to
// unavailable.
func LoadPolicy(cfg Config) (Policy, error) {
	log := cfg.logger()
	path := cfg.policyPath()

	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		log.Warn("job policy file missing: 策略文件缺席，三轴按 None 读（合法默认）",
			"policy", path, "declaration", policyEffectStatement)
		return Policy{agents: map[string]agentEntry{}, projects: map[string]projectEntry{}}, nil
	case err != nil:
		log.Error("job policy file unreadable: 文件不可读，策略文件内容哈希不可得",
			"policy", path, "err", err.Error(), "declaration", policyEffectStatement)
		return Policy{}, fmt.Errorf("read job policy %s: %w", path, err)
	}

	sum := sha256.Sum256(data)
	hashLine := policyHashLinePrefix + hex.EncodeToString(sum[:])
	// The hash line and the effect statement are printed on every readable
	// path, the accepting one included (AC-29 / AC-44): the file's bytes are
	// the evidence of what this process loaded.
	log.Info(hashLine, "policy", path, "declaration", policyEffectStatement, "effect", policyEffectStatement)

	file, err := parsePolicyFile(path, data)
	if err != nil {
		log.Error("job policy rejected: refusing to start",
			"policy", path, "err", err.Error(), "declaration", policyEffectStatement)
		return Policy{}, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	policy, err := buildPolicy(file)
	if err != nil {
		log.Error("job policy rejected: refusing to start",
			"policy", path, "err", err.Error(), "declaration", policyEffectStatement)
		return Policy{}, err
	}

	if err := resolvePolicyNames(cfg, policy, log); err != nil {
		log.Error("job policy name resolution refused startup",
			"policy", path, "err", err.Error(), "declaration", policyEffectStatement)
		return Policy{}, err
	}
	logDeadDefinitions(policy, log)
	return policy, nil
}

// parsePolicyFile decodes policy.yaml strictly. An empty document (the shipped
// skeleton is all comments) reads as an empty policy: it is not a broken file,
// and refusing it would crash-loop a deployment that shipped the skeleton
// unedited.
func parsePolicyFile(path string, data []byte) (filePolicy, error) {
	documents, err := countYAMLDocuments(data)
	if err != nil {
		return filePolicy{}, fmt.Errorf("job policy %s is not parseable as YAML: %w", path, err)
	}
	if documents > 1 {
		return filePolicy{}, fmt.Errorf("job policy %s must hold one YAML document, got %d", path, documents)
	}
	jsonBytes, err := utilyaml.ToJSON(data)
	if err != nil {
		return filePolicy{}, fmt.Errorf("job policy %s is not parseable as YAML: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.DisallowUnknownFields()
	var file filePolicy
	if err := decoder.Decode(&file); err != nil {
		return filePolicy{}, fmt.Errorf("job policy %s rejected by strict decoding: %w", path, err)
	}
	return file, nil
}

// buildPolicy validates the decoded file and freezes it into the read-only
// Policy: tiers spell the four documented literals, resource quantities parse,
// capability names are non-empty, and no definition rewrites drop.
func buildPolicy(file filePolicy) (Policy, error) {
	policy := Policy{
		agents:   make(map[string]agentEntry, len(file.Agents)),
		projects: make(map[string]projectEntry, len(file.Projects)),
	}
	for name, row := range file.Projects {
		if name == "" {
			return Policy{}, fmt.Errorf("%w: a projects key is empty", ErrInvalidPolicy)
		}
		entry, err := buildProjectEntry(name, row)
		if err != nil {
			return Policy{}, err
		}
		policy.projects[name] = entry
	}
	for name, row := range file.Agents {
		if name == "" {
			return Policy{}, fmt.Errorf("%w: an agents key is empty", ErrInvalidPolicy)
		}
		entry, err := buildAgentEntry(name, row)
		if err != nil {
			return Policy{}, err
		}
		policy.agents[name] = entry
	}
	return policy, nil
}

// ErrInvalidPolicy marks a policy file Foreman refuses to serve with.
var ErrInvalidPolicy = errors.New("invalid job policy")

func buildAgentEntry(name string, row fileAgent) (agentEntry, error) {
	resourcePolicy, err := parseAxis("agents."+name+".resourcePolicy", row.ResourcePolicy)
	if err != nil {
		return agentEntry{}, err
	}
	capPolicy, err := parseAxis("agents."+name+".capPolicy", row.CapPolicy)
	if err != nil {
		return agentEntry{}, err
	}
	nodeSelectorPolicy, err := parseAxis("agents."+name+".nodeSelectorPolicy", row.NodeSelectorPolicy)
	if err != nil {
		return agentEntry{}, err
	}
	resources, err := parseResources("agents."+name+".resources", row.Resources)
	if err != nil {
		return agentEntry{}, err
	}
	capabilities, err := parseCapabilities("agents."+name+".capabilities", row.Capabilities)
	if err != nil {
		return agentEntry{}, err
	}
	return agentEntry{
		resourcePolicy:      resourcePolicy,
		capPolicy:           capPolicy,
		nodeSelectorPolicy:  nodeSelectorPolicy,
		resources:           resources,
		capabilities:        capabilities,
		nodeSelectorDefined: row.NodeSelector,
	}, nil
}

func buildProjectEntry(name string, row fileProject) (projectEntry, error) {
	resources, err := parseResources("projects."+name+".resources", row.Resources)
	if err != nil {
		return projectEntry{}, err
	}
	capabilities, err := parseCapabilities("projects."+name+".capabilities", row.Capabilities)
	if err != nil {
		return projectEntry{}, err
	}
	return projectEntry{
		resources:           resources,
		capabilities:        capabilities,
		nodeSelectorDefined: row.NodeSelector,
	}, nil
}

// parseAxis reads one tier: absent (the field is not written) means None — the
// same reading as an unlisted agent, and never a startup refusal.
func parseAxis(field string, raw *string) (AxisPolicy, error) {
	if raw == nil {
		return AxisNone, nil
	}
	switch AxisPolicy(*raw) {
	case AxisNone, AxisProject, AxisAgent, AxisProjectWithAgent:
		return AxisPolicy(*raw), nil
	default:
		return AxisNone, fmt.Errorf("%w: %s = %q is not one of None/project/agent/projectWithAgent",
			ErrInvalidPolicy, field, *raw)
	}
}

// parseResources reads a resource definition body: the two optional k8s-native
// sub maps. Quantities are parsed here, at startup, so a typo refuses the boot
// instead of failing one task's Job create.
func parseResources(field string, raw *fileResources) (*resourceDefinition, error) {
	if raw == nil {
		return nil, nil
	}
	requests, err := parseResourceList(field+".requests", raw.Requests)
	if err != nil {
		return nil, err
	}
	limits, err := parseResourceList(field+".limits", raw.Limits)
	if err != nil {
		return nil, err
	}
	return &resourceDefinition{requests: requests, limits: limits}, nil
}

func parseResourceList(field string, raw map[string]string) (corev1.ResourceList, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	list := make(corev1.ResourceList, len(raw))
	for key, value := range raw {
		if key == "" {
			return nil, fmt.Errorf("%w: %s carries an empty resource key", ErrInvalidPolicy, field)
		}
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("%w: %s.%s = %q is not a valid quantity: %v", ErrInvalidPolicy, field, key, value, err)
		}
		list[corev1.ResourceName(key)] = quantity
	}
	return list, nil
}

// parseCapabilities reads a capability definition body. A written drop refuses
// startup: Foreman is the one writing capabilities into the container, so a
// silently ignored drop would mean a security intent that never took effect.
func parseCapabilities(field string, raw *fileCapabilities) (*capabilityDefinition, error) {
	if raw == nil {
		return nil, nil
	}
	if raw.Drop != nil {
		return nil, fmt.Errorf("%w: %s.drop is not writable — the capability axis appends add and keeps the template's drop",
			ErrInvalidPolicy, field)
	}
	for _, name := range raw.Add {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%w: %s.add carries an empty capability", ErrInvalidPolicy, field)
		}
	}
	return &capabilityDefinition{add: append([]string(nil), raw.Add...)}, nil
}

// resolvePolicyNames performs the startup name resolution (ADR-035 decision 4):
// one attempt per key, inside one bounded window, with the conclusion of every
// key recorded as a policy.name_resolution event. absent and ambiguous refuse
// startup; unavailable only warns — the runtime comparison stays exact-match on
// names, and nothing is cached or refreshed.
func resolvePolicyNames(cfg Config, policy Policy, log logger) error {
	keys := sortedPolicyKeys(policy)
	if len(keys) == 0 {
		return nil
	}
	resolver := cfg.NameResolver
	deadline := time.Now().Add(cfg.policyNameResolutionWindow())
	var refusals []string
	for _, key := range keys {
		conclusion, err := resolveOneName(cfg, resolver, deadline, key)
		fields := []any{"kind", key.kind, "name", key.name, "conclusion", string(conclusion)}
		if conclusion == NameUnavailable {
			log.Warn("policy.name_resolution", fields...)
		} else {
			log.Info("policy.name_resolution", fields...)
		}
		if err != nil {
			// The window is exhausted: every remaining key reads unavailable
			// too, and the resolver is not called again (no retry, no
			// extension).
			resolver = nil
		}
		switch conclusion {
		case NameAbsent, NameAmbiguous:
			refusals = append(refusals, fmt.Sprintf("%s %q: %s", key.kind, key.name, conclusion))
		}
	}
	if len(refusals) > 0 {
		return fmt.Errorf("%w: platform name resolution refused: %s",
			ErrInvalidPolicy, strings.Join(refusals, "; "))
	}
	return nil
}

// policyKey is one key of the policy table's two sections.
type policyKey struct {
	kind string // agent | project
	name string
}

// sortedPolicyKeys lists every key of both sections in a stable order, so the
// per-key events of a boot are reproducible.
func sortedPolicyKeys(policy Policy) []policyKey {
	keys := make([]policyKey, 0, len(policy.agents)+len(policy.projects))
	for name := range policy.agents {
		keys = append(keys, policyKey{kind: "agent", name: name})
	}
	for name := range policy.projects {
		keys = append(keys, policyKey{kind: "project", name: name})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].name < keys[j].name
	})
	return keys
}

// resolveOneName resolves one key inside the batch window; a nil resolver (no
// platform query, or a window already exhausted) concludes unavailable without
// an error.
func resolveOneName(cfg Config, resolver PolicyNameResolver, deadline time.Time, key policyKey) (NameConclusion, error) {
	if resolver == nil {
		return NameUnavailable, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return NameUnavailable, errors.New("policy name resolution window exhausted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	conclusion, err := resolver.ResolveName(ctx, key.kind, key.name)
	if err != nil {
		cfg.logger().Warn("policy name resolution unavailable",
			"kind", key.kind, "name", key.name, "err", err.Error())
		return NameUnavailable, nil
	}
	switch conclusion {
	case NameResolved, NameAbsent, NameAmbiguous, NameUnavailable:
		return conclusion, nil
	default:
		return NameUnavailable, nil
	}
}

// logDeadDefinitions warns about definitions no policy references (ADR-035
// decision 8/3): visible, never blocking.
func logDeadDefinitions(policy Policy, log logger) {
	var dead []string
	for name, entry := range policy.agents {
		if !reachesAgentDefinition(entry.resourcePolicy) && !entry.resources.empty() {
			dead = append(dead, "agents."+name+".resources")
		}
		if !reachesAgentDefinition(entry.capPolicy) && !entry.capabilities.empty() {
			dead = append(dead, "agents."+name+".capabilities")
		}
		if !reachesAgentDefinition(entry.nodeSelectorPolicy) && !emptyNodeSelector(entry.nodeSelectorDefined) {
			dead = append(dead, "agents."+name+".nodeSelector")
		}
	}
	projectReachable := reachesProjectDefinition(policy)
	for name, entry := range policy.projects {
		if !projectReachable && !entry.resources.empty() {
			dead = append(dead, "projects."+name+".resources")
		}
		if !projectReachable && !entry.capabilities.empty() {
			dead = append(dead, "projects."+name+".capabilities")
		}
		if !projectReachable && !emptyNodeSelector(entry.nodeSelectorDefined) {
			dead = append(dead, "projects."+name+".nodeSelector")
		}
	}
	if len(dead) == 0 {
		return
	}
	sort.Strings(dead)
	log.Warn("job policy carries definitions no axis references",
		"dead_definitions", strings.Join(dead, ","))
}

// reachesAgentDefinition reports whether a tier can read the agent's own
// definition.
func reachesAgentDefinition(tier AxisPolicy) bool {
	return tier == AxisAgent || tier == AxisProjectWithAgent
}

// reachesProjectDefinition reports whether any agent tier can read a project
// definition.
func reachesProjectDefinition(policy Policy) bool {
	for _, entry := range policy.agents {
		for _, tier := range []AxisPolicy{entry.resourcePolicy, entry.capPolicy, entry.nodeSelectorPolicy} {
			if tier == AxisProject || tier == AxisProjectWithAgent {
				return true
			}
		}
	}
	return false
}

// Evaluate resolves the three axes for one task (ADR-035 decisions 1/2/5/6):
// each axis selects its definition source by its own tier, compares names by
// equality, merges projectWithAgent pairs, and reports a source even when
// nothing is injected — the policy.applied event is the sole observable face of
// the silent degradation cases.
func (p Policy) Evaluate(e TaskEntry) Evaluation {
	agent, agentListed := p.agents[e.AgentName]
	project := p.projects[e.ProjectName]

	ev := Evaluation{
		projectSource: e.ProjectSource,
		nameSource:    e.NameSource,
		suppressed:    SuppressionNone,
	}
	if ev.projectSource == "" {
		ev.projectSource = ProjectSourceNone
	}
	if ev.nameSource == "" {
		ev.nameSource = "template"
	}

	ev.resourcePolicy = tierFor(agentListed, agent.resourcePolicy)
	ev.capPolicy = tierFor(agentListed, agent.capPolicy)
	ev.nodeSelectorPolicy = tierFor(agentListed, agent.nodeSelectorPolicy)

	identity := axisIdentity{
		agentName:   e.AgentName,
		projectName: e.ProjectName,
	}

	// An empty agent name leaves the agent's row unreachable, so no axis
	// declares a tier; the reason nothing was read is the missing identity,
	// not a missing definition (§8.5: an absent agent name reads no_identity).
	// An agent that is simply unlisted keeps no_definition.
	if e.AgentName == "" {
		ev.resourceSource, ev.capSource, ev.nodeSelectorSource = SourceNoIdentity, SourceNoIdentity, SourceNoIdentity
		return ev
	}

	var capOverrides, selectorOverrides []string
	ev.resourceSource, ev.resources, ev.overriddenKeys = resolveResourceAxis(
		ev.resourcePolicy, agent.resources, project.resources, identity)
	ev.capSource, ev.capabilityAdd, capOverrides = resolveCapabilityAxis(
		ev.capPolicy, agent.capabilities, project.capabilities, identity)
	ev.nodeSelectorSource, ev.nodeSelector, selectorOverrides = resolveNodeSelectorAxis(
		ev.nodeSelectorPolicy, agent.nodeSelectorDefined, project.nodeSelectorDefined, identity)
	ev.overriddenKeys = append(ev.overriddenKeys, capOverrides...)
	ev.overriddenKeys = append(ev.overriddenKeys, selectorOverrides...)
	sort.Strings(ev.overriddenKeys)

	if len(ev.nodeSelector) > 0 {
		ev.suppressed = SuppressionPolicy
	}
	return ev
}

// axisIdentity is what one axis may read from the task: the two identity names
// its tiers key on. An absent name is the axis's identity-unavailable face.
type axisIdentity struct {
	agentName   string
	projectName string
}

// tierFor reads a listed agent's tier; an agent that is absent from the table
// (or whose row omits the axis field) reads None.
func tierFor(listed bool, tier AxisPolicy) AxisPolicy {
	if !listed {
		return AxisNone
	}
	return tier
}

// resolveResourceAxis applies the resource axis's tiers and merge. The resolved
// value is an atomic replacement block for containers[0].resources, so it is
// written only when the axis has a usable definition.
func resolveResourceAxis(tier AxisPolicy, agentDef, projectDef *resourceDefinition, id axisIdentity) (
	source string, value *corev1.ResourceRequirements, overridden []string) {
	switch tier {
	case AxisProject:
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		if projectDef.empty() {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, projectResources(projectDef), nil
	case AxisAgent:
		if id.agentName == "" {
			return SourceNoIdentity, nil, nil
		}
		if agentDef.empty() {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, agentResources(agentDef), nil
	case AxisProjectWithAgent:
		// Identity unavailability outranks single-side use (ADR-035
		// decision 5): without the project name the project tier cannot be
		// honoured, so the axis degrades instead of reading the agent's side.
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		merged, keys := mergeResources(projectDef, agentDef)
		if merged == nil {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, merged, keys
	default:
		return SourceNoDefinition, nil, nil
	}
}

// resolveCapabilityAxis applies the capability axis's tiers and merge. The
// resolved value is appended to the template's own add list.
func resolveCapabilityAxis(tier AxisPolicy, agentDef, projectDef *capabilityDefinition, id axisIdentity) (
	source string, add []string, overridden []string) {
	switch tier {
	case AxisProject:
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		if projectDef.empty() {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, append([]string(nil), projectDef.add...), nil
	case AxisAgent:
		if id.agentName == "" {
			return SourceNoIdentity, nil, nil
		}
		if agentDef.empty() {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, append([]string(nil), agentDef.add...), nil
	case AxisProjectWithAgent:
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		merged, keys := mergeCapabilities(projectDef, agentDef)
		if len(merged) == 0 {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, merged, keys
	default:
		return SourceNoDefinition, nil, nil
	}
}

// resolveNodeSelectorAxis applies the nodeSelector axis's tiers and merge. The
// resolved value is merged key-by-key into the pod template's nodeSelector, so
// the template's other keys survive.
func resolveNodeSelectorAxis(tier AxisPolicy, agentDef, projectDef map[string]string, id axisIdentity) (
	source string, value map[string]string, overridden []string) {
	switch tier {
	case AxisProject:
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		if emptyNodeSelector(projectDef) {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, copyNodeSelector(projectDef), nil
	case AxisAgent:
		if id.agentName == "" {
			return SourceNoIdentity, nil, nil
		}
		if emptyNodeSelector(agentDef) {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, copyNodeSelector(agentDef), nil
	case AxisProjectWithAgent:
		if id.projectName == "" {
			return SourceNoIdentity, nil, nil
		}
		merged, keys := mergeNodeSelector(projectDef, agentDef)
		if emptyNodeSelector(merged) {
			return SourceNoDefinition, nil, nil
		}
		return SourceDirect, merged, keys
	default:
		return SourceNoDefinition, nil, nil
	}
}

// projectResources renders one project definition as the replacement block.
func projectResources(def *resourceDefinition) *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{Requests: copyResourceList(def.requests), Limits: copyResourceList(def.limits)}
}

func agentResources(def *resourceDefinition) *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{Requests: copyResourceList(def.requests), Limits: copyResourceList(def.limits)}
}

func copyResourceList(list corev1.ResourceList) corev1.ResourceList {
	if len(list) == 0 {
		return nil
	}
	out := make(corev1.ResourceList, len(list))
	for key, value := range list {
		out[key] = value
	}
	return out
}

// mergeResources merges the project and agent definitions key by key, the agent
// winning on a shared key. The overridden keys are the shared ones — the
// refutable face of the merge (AC-42).
func mergeResources(projectDef, agentDef *resourceDefinition) (*corev1.ResourceRequirements, []string) {
	if projectDef.empty() && agentDef.empty() {
		return nil, nil
	}
	out := &corev1.ResourceRequirements{
		Requests: mergedResourceList(projectDef.requestsOrNil(), agentDef.requestsOrNil()),
		Limits:   mergedResourceList(projectDef.limitsOrNil(), agentDef.limitsOrNil()),
	}
	overridden := sharedResourceKeys(projectDef.requestsOrNil(), agentDef.requestsOrNil(), "resources.requests")
	overridden = append(overridden, sharedResourceKeys(projectDef.limitsOrNil(), agentDef.limitsOrNil(), "resources.limits")...)
	if len(out.Requests) == 0 && len(out.Limits) == 0 {
		return nil, nil
	}
	return out, overridden
}

func (d *resourceDefinition) requestsOrNil() corev1.ResourceList {
	if d == nil {
		return nil
	}
	return d.requests
}

func (d *resourceDefinition) limitsOrNil() corev1.ResourceList {
	if d == nil {
		return nil
	}
	return d.limits
}

// mergedResourceList merges one side of the resource block, key by key.
func mergedResourceList(projectList, agentList corev1.ResourceList) corev1.ResourceList {
	if len(projectList) == 0 && len(agentList) == 0 {
		return nil
	}
	out := make(corev1.ResourceList, len(projectList)+len(agentList))
	for key, value := range projectList {
		out[key] = value
	}
	for key, value := range agentList {
		out[key] = value
	}
	return out
}

// sharedResourceKeys lists the keys both sides define, formed as axis:key.
func sharedResourceKeys(projectList, agentList corev1.ResourceList, axis string) []string {
	var keys []string
	for key := range agentList {
		if _, ok := projectList[key]; ok {
			keys = append(keys, axis+":"+string(key))
		}
	}
	return keys
}

// mergeCapabilities merges two add lists into one deduplicated set. The order
// of the result is not part of any assertion (AC-43): only the set is. Either
// side may be absent — a missing definition is an empty list, never a panic.
func mergeCapabilities(projectDef, agentDef *capabilityDefinition) ([]string, []string) {
	projectAdd, agentAdd := projectDef.addOrNil(), agentDef.addOrNil()
	if len(projectAdd) == 0 && len(agentAdd) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	var merged []string
	for _, list := range [][]string{projectAdd, agentAdd} {
		for _, name := range list {
			if seen[name] {
				continue
			}
			seen[name] = true
			merged = append(merged, name)
		}
	}
	var overridden []string
	for name := range seen {
		if contains(projectAdd, name) && contains(agentAdd, name) {
			overridden = append(overridden, "capabilities:"+name)
		}
	}
	return merged, overridden
}

// addOrNil is the capability definition's list, nil when the side is absent.
func (d *capabilityDefinition) addOrNil() []string {
	if d == nil {
		return nil
	}
	return d.add
}

func contains(list []string, value string) bool {
	for _, entry := range list {
		if entry == value {
			return true
		}
	}
	return false
}

// mergeNodeSelector merges two selector definitions key by key, the agent
// winning on a shared key.
func mergeNodeSelector(projectDef, agentDef map[string]string) (map[string]string, []string) {
	if emptyNodeSelector(projectDef) && emptyNodeSelector(agentDef) {
		return nil, nil
	}
	out := copyNodeSelector(projectDef)
	if out == nil {
		out = map[string]string{}
	}
	var overridden []string
	for key, value := range agentDef {
		if _, shared := projectDef[key]; shared {
			overridden = append(overridden, "node_selector:"+key)
		}
		out[key] = value
	}
	return out, overridden
}

func copyNodeSelector(def map[string]string) map[string]string {
	if len(def) == 0 {
		return nil
	}
	out := make(map[string]string, len(def))
	for key, value := range def {
		out[key] = value
	}
	return out
}

// applyPolicy writes the evaluated axes onto the rendered Job
// (§8.2 A1.5): the first container's resources as one replacement block, its
// capability add list appended, and the pod template's nodeSelector merged key
// by key. drop and every other securityContext field stay the template's, and
// initContainers are never touched.
func applyPolicy(job *batchv1.Job, ev Evaluation) {
	containers := job.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return
	}
	if ev.resources != nil {
		containers[0].Resources = *ev.resources
	}
	if len(ev.capabilityAdd) > 0 {
		injectCapabilities(&containers[0], ev.capabilityAdd)
	}
	if len(ev.nodeSelector) > 0 {
		selector := job.Spec.Template.Spec.NodeSelector
		if selector == nil {
			selector = map[string]string{}
		}
		for key, value := range ev.nodeSelector {
			selector[key] = value
		}
		job.Spec.Template.Spec.NodeSelector = selector
	}
}

// injectCapabilities appends the policy's capabilities to the container's own
// add list, keeping every existing entry (and the whole drop list) untouched —
// the axis adds capability, it never removes.
func injectCapabilities(container *corev1.Container, add []string) {
	if container.SecurityContext == nil {
		container.SecurityContext = &corev1.SecurityContext{}
	}
	if container.SecurityContext.Capabilities == nil {
		container.SecurityContext.Capabilities = &corev1.Capabilities{}
	}
	capabilities := container.SecurityContext.Capabilities
	seen := make(map[corev1.Capability]bool, len(capabilities.Add))
	for _, existing := range capabilities.Add {
		seen[existing] = true
	}
	for _, name := range add {
		capability := corev1.Capability(name)
		if seen[capability] {
			continue
		}
		seen[capability] = true
		capabilities.Add = append(capabilities.Add, capability)
	}
}

// RequiresProjectIdentity reports whether any axis this task declares reads the
// project side (project / projectWithAgent). An agent that is absent from the
// table declares nothing, so the reverse lookup stays out of the claim path
// entirely (ADR-035 decision 3: 反查只在需要时发生).
func (b *Builder) RequiresProjectIdentity(e TaskEntry) bool {
	agent, listed := b.policy.agents[e.AgentName]
	if !listed {
		return false
	}
	for _, tier := range []AxisPolicy{agent.resourcePolicy, agent.capPolicy, agent.nodeSelectorPolicy} {
		if tier == AxisProject || tier == AxisProjectWithAgent {
			return true
		}
	}
	return false
}

// logger is the minimal logging surface LoadPolicy and the render path need;
// *slog.Logger satisfies it and tests can keep it nil-free through Config.
type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// logPolicyApplied emits the per-task policy.applied event
// (§9.1): the declared tiers, the per-axis source, the project and name
// sources, the definition-side overrides, the faces actually written, and the
// soft-affinity decision's cause. It is the only observable face of a policy
// that silently does not apply; there is no policy metric
// (cardinality discipline).
func (b *Builder) logPolicyApplied(e TaskEntry, jobName string, ev Evaluation) {
	b.cfg.logger().Info("policy.applied",
		"task_id", e.TaskID,
		"job_name", jobName,
		"resource_policy", ev.resourcePolicy.appliedValue(),
		"cap_policy", ev.capPolicy.appliedValue(),
		"node_selector_policy", ev.nodeSelectorPolicy.appliedValue(),
		"resource_source", ev.resourceSource,
		"cap_source", ev.capSource,
		"node_selector_source", ev.nodeSelectorSource,
		"project_source", ev.projectSource,
		"name_source", ev.nameSource,
		"overridden_keys", ev.overriddenKeys,
		"template_written_faces", ev.writtenFaces(),
		"soft_affinity_suppressed", ev.suppressed)
}
