package jobbuilder

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// PremergeValidator is stage 1 of the two-stage overlay validation
// (03-contracts.md §5.4): it judges the overlay's own raw map, where
// "field present" is still visible. Stage 2 (InvariantChecker) then re-checks
// the merged object; the startup gate runs both.
type PremergeValidator struct{}

// serverTokenShape matches mdt_/mul_ token prefixes (F3 形态扫描, C-4).
var serverTokenShape = regexp.MustCompile(`m(dt|ul)_`)

// checkOverlayMap runs every stage-1 clause. The context key is the slice of
// raw-map paths a check walks.
func (PremergeValidator) CheckOverlayMap(raw map[string]any) []Violation {
	v := newViolations()

	// `$` 前缀键禁令: any $-prefixed key anywhere (they are invisible or
	// already applied after merging, so the raw map is the only place to see
	// them); "remove a default-template field" stays unsupported (ADR-011).
	scanForbiddenKeys(v, "", raw)

	// C-4: server token shape anywhere in the overlay text (F3).
	scanTokenShape(v, "", raw)

	// 清单 A: authoritative paths — appearing in the overlay is a violation
	// even though Foreman would overwrite them.
	checkAuthoritative(v, raw)

	// 清单 B: §5.1-env-owned paths.
	checkEnvTuned(v, raw)

	return v.list()
}

func checkAuthoritative(v *violations, raw map[string]any) {
	for _, key := range []string{"name", "namespace"} {
		if _, present := atPath(raw, "metadata", key); present {
			v.add("metadata."+key, "清单 A: 权威字段（Foreman 独占，overlay 出现即拒）")
		}
	}
	checkReservedKeys(v, raw, "metadata", "labels")
	checkReservedKeys(v, raw, "metadata", "annotations")
	for _, key := range []string{"backoffLimit", "completions", "parallelism", "suspend", "podFailurePolicy", "selector", "manualSelector"} {
		if _, present := atPath(raw, "spec", key); present {
			v.add("spec."+key, "清单 A: Job 级字段权威（Foreman 独占）")
		}
	}
	checkReservedKeys(v, raw, "spec", "template", "metadata", "labels")
	checkReservedKeys(v, raw, "spec", "template", "metadata", "annotations")
	for _, key := range []string{"restartPolicy", "serviceAccountName", "automountServiceAccountToken", "ephemeralContainers"} {
		if _, present := atPath(raw, "spec", "template", "spec", key); present {
			v.add("spec.template.spec."+key, "清单 A: Pod 级字段权威（Foreman 独占）")
		}
	}
	for _, key := range []string{"runAsUser", "runAsGroup", "fsGroup", "runAsNonRoot", "seccompProfile"} {
		if _, present := atPath(raw, "spec", "template", "spec", "securityContext", key); present {
			v.add("spec.template.spec.securityContext."+key, "清单 A: Pod securityContext 锁定键")
		}
	}
	if list, ok := listAt(raw, "spec", "template", "spec", "volumes"); ok {
		for i, entry := range list {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if name, _ := m["name"].(string); isBuiltinVolume(name) {
				v.add(fmt.Sprintf("spec.template.spec.volumes[%d]", i),
					fmt.Sprintf("清单 A: 内置卷 name=%s 的定义与来源类型锁定", name))
			}
		}
	}
	checkOverlayContainers(v, raw)
	checkOverlayInitContainers(v, raw)
}

func checkEnvTuned(v *violations, raw map[string]any) {
	for _, key := range []string{"ttlSecondsAfterFinished", "activeDeadlineSeconds"} {
		if _, present := atPath(raw, "spec", key); present {
			v.add("spec."+key, "清单 B: 由 §5.1 env 独占")
		}
	}
	for _, key := range []string{"imagePullSecrets", "nodeSelector", "tolerations"} {
		if _, present := atPath(raw, "spec", "template", "spec", key); present {
			v.add("spec.template.spec."+key, "清单 B: 由 §5.1 env 独占")
		}
	}
}

// checkReservedKeys rejects identity-label and audit keys on any metadata map
// (清单 A) — label/annotation keys of the foreman.tsic.top/ domain and the two
// app.kubernetes.io keys (C-5).
func checkReservedKeys(v *violations, raw map[string]any, path ...string) {
	meta, ok := mapAt(raw, path...)
	if !ok {
		return
	}
	base := strings.Join(path, ".")
	for _, key := range sortedKeys(meta) {
		switch {
		case strings.HasPrefix(key, foremanDomain):
			v.add(base+"["+key+"]", "清单 A: foreman.tsic.top/ 域身份标签注解（GC/reconcile 依赖）")
		case key == labelAppName || key == labelManagedBy:
			v.add(base+"["+key+"]", "清单 A: 身份标签键锁定")
		}
	}
}

func checkOverlayContainers(v *violations, raw map[string]any) {
	list, ok := listAt(raw, "spec", "template", "spec", "containers")
	if !ok {
		return
	}
	const base = "spec.template.spec.containers"
	if len(list) != 1 {
		v.add(base, "清单 A: containers 恒为 1 项（agent 单业务容器；辅助容器以 initContainers 追加表达）")
	}
	for i, entry := range list {
		path := fmt.Sprintf("%s[%d]", base, i)
		m, ok := entry.(map[string]any)
		if !ok {
			v.add(path, "清单 A: container 必须是对象")
			continue
		}
		if name, _ := m["name"].(string); name != containerAgent {
			// Without the merge key the patch cannot target the built-in
			// agent container; it would become containers[1..].
			v.add(path+".name", "清单 A: 主容器只能是 agent，且不得追加 containers[1..]")
		}
		for _, key := range []string{"command", "args", "workingDir", "tty"} {
			if _, present := m[key]; present {
				v.add(path+"."+key, "清单 A: agent 锁定子路径（ADR-004 注入点与日志可见性）")
			}
		}
		if _, present := m["image"]; present {
			v.add(path+".image", "清单 B: image 定制只走 FOREMAN_JOB_IMAGE/FOREMAN_JOB_IMAGE_DIGEST")
		}
		if _, present := m["resources"]; present {
			v.add(path+".resources", "清单 B: resources 由 §5.1 env 独占")
		}
		checkAgentSecurityContext(v, path+".securityContext", m)
		checkAgentEnv(v, path, m)
		checkAgentVolumeMounts(v, path, m)
	}
}

// checkAgentSecurityContext: the only legal overlay write into the agent
// securityContext is readOnlyRootFilesystem false → true (C-6); every other
// key is locked by 清单 A.
func checkAgentSecurityContext(v *violations, path string, container map[string]any) {
	rawSC, present := container["securityContext"]
	if !present {
		return
	}
	sc, ok := rawSC.(map[string]any)
	if !ok {
		v.add(path, "清单 A: securityContext 必须是对象")
		return
	}
	for _, key := range sortedKeys(sc) {
		if key != "readOnlyRootFilesystem" {
			v.add(path+"."+key, "清单 A: agent securityContext 键锁定（唯一例外 C-6）")
			continue
		}
		if value, ok := sc[key].(bool); !ok || !value {
			v.add(path+"."+key, "清单 C-6: readOnlyRootFilesystem 只允许单向加强为 true")
		}
	}
}

// checkAgentEnv: the 16 authoritative env keys must not be touched and new
// env entries must not use the MULTICA_ prefix.
func checkAgentEnv(v *violations, containerPath string, container map[string]any) {
	list, ok := listAt(container, "env")
	if !ok {
		return
	}
	for i, entry := range list {
		m, ok := entry.(map[string]any)
		if !ok {
			v.add(fmt.Sprintf("%s.env[%d]", containerPath, i), "清单 A: env 条目必须是对象")
			continue
		}
		name, _ := m["name"].(string)
		switch {
		case authoritativeEnvKeys[name]:
			v.add(fmt.Sprintf("%s.env[%s]", containerPath, name), "清单 A: 16 键权威 env 不得增删改（F5/§5.2）")
		case strings.HasPrefix(name, "MULTICA_"):
			v.add(fmt.Sprintf("%s.env[%s]", containerPath, name), "清单 A: 新增 env 不得使用 MULTICA_ 前缀")
		}
	}
}

// checkAgentVolumeMounts: the mounts of the built-in five volumes are locked
// (name+mountPath pairs); extra mounts stay free under C-10.
func checkAgentVolumeMounts(v *violations, containerPath string, container map[string]any) {
	list, ok := listAt(container, "volumeMounts")
	if !ok {
		return
	}
	for i, entry := range list {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := m["name"].(string); isBuiltinVolume(name) {
			v.add(fmt.Sprintf("%s.volumeMounts[%d].name", containerPath, i),
				fmt.Sprintf("清单 A: 内置卷 %s 的挂载 name+mountPath 对锁定", name))
		}
	}
}

// checkOverlayInitContainers: the built-in prepare is fully locked (any entry
// named prepare is rejected, including restartPolicy) and appended entries
// must be one of the two legal forms: native sidecar (restartPolicy: Always)
// or prepare-init (no restartPolicy at all).
func checkOverlayInitContainers(v *violations, raw map[string]any) {
	list, ok := listAt(raw, "spec", "template", "spec", "initContainers")
	if !ok {
		return
	}
	const base = "spec.template.spec.initContainers"
	for i, entry := range list {
		path := fmt.Sprintf("%s[%d]", base, i)
		m, ok := entry.(map[string]any)
		if !ok {
			v.add(path, "清单 A: initContainers 条目必须是对象")
			continue
		}
		name, _ := m["name"].(string)
		switch name {
		case "":
			v.add(path+".name", "清单 A: 追加条目必须有 name（缺 name 由 SMP ErrNoMergeKey 拒绝）")
		case containerPrepare:
			v.add(path, "清单 A: 内置 prepare 全字段锁定（含 restartPolicy），overlay 不得出现该条目")
		case containerAgent:
			v.add(path+".name", "清单 A: 追加条目不得命名为 agent")
		}
		if restartPolicy, present := m["restartPolicy"]; present {
			if value, ok := restartPolicy.(string); !ok || value != string(corev1.ContainerRestartPolicyAlways) {
				v.add(path+".restartPolicy",
					fmt.Sprintf("清单 A: 追加条目 restartPolicy 只允许缺省（prepare-init）或 Always（native sidecar），got %v", restartPolicy))
			}
		}
	}
}

// scanForbiddenKeys walks every map and list, reporting $-prefixed keys.
func scanForbiddenKeys(v *violations, path string, node any) {
	switch typed := node.(type) {
	case map[string]any:
		for _, key := range sortedKeys(typed) {
			child := joinPath(path, key)
			if strings.HasPrefix(key, "$") {
				v.add(child, "§5.4: `$` 前缀键禁令（$patch/$setElementOrder 等指令在合并后不可见）")
			}
			scanForbiddenKeys(v, child, typed[key])
		}
	case []any:
		for i, item := range typed {
			scanForbiddenKeys(v, fmt.Sprintf("%s[%d]", path, i), item)
		}
	}
}

// scanTokenShape walks every string field of the overlay looking for the
// (mdt|mul)_ shape (C-4/F3). Only the raw overlay is scanned — the merged
// object legitimately carries per-task values.
func scanTokenShape(v *violations, path string, node any) {
	switch typed := node.(type) {
	case map[string]any:
		for _, key := range sortedKeys(typed) {
			scanTokenShape(v, joinPath(path, key), typed[key])
		}
	case []any:
		for i, item := range typed {
			scanTokenShape(v, fmt.Sprintf("%s[%d]", path, i), item)
		}
	case string:
		if serverTokenShape.MatchString(typed) {
			v.add(path, "清单 C-4: overlay 原始文本含 (mdt|mul)_ server 凭据形态（F3）")
		}
	}
}

// violations collects stage-1 findings in deterministic order.
type violations struct {
	seen       map[string]bool
	violations []Violation
}

func newViolations() *violations {
	return &violations{seen: map[string]bool{}}
}

func (v *violations) add(path, rule string) {
	key := path + "\x00" + rule
	if v.seen[key] {
		return
	}
	v.seen[key] = true
	v.violations = append(v.violations, Violation{Path: path, Rule: rule})
}

func (v *violations) list() []Violation {
	if len(v.violations) == 0 {
		return nil
	}
	return v.violations
}

// atPath resolves a structural path of map keys.
func atPath(node any, keys ...string) (any, bool) {
	current := node
	for _, key := range keys {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[key]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func mapAt(node any, keys ...string) (map[string]any, bool) {
	value, ok := atPath(node, keys...)
	if !ok {
		return nil, false
	}
	m, ok := value.(map[string]any)
	return m, ok
}

// listAt resolves a structural path whose final step may be a map key.
func listAt(node any, keys ...string) ([]any, bool) {
	value, ok := atPath(node, keys...)
	if !ok {
		return nil, false
	}
	list, ok := value.([]any)
	return list, ok
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func joinPath(base, key string) string {
	if base == "" {
		return key
	}
	return base + "." + key
}
