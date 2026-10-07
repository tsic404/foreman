package jobbuilder

import (
	"encoding/json"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// mergeOverlay applies the overlay to the default template with Kubernetes
// strategic merge patch semantics (03-contracts.md §5.4 合并语义): lists merge
// by their key (containers/initContainers/env/volumes by name, volumeMounts by
// mountPath), maps by key, scalars overwrite. Merge failures are fail-closed:
// an appended initContainers entry without a name ends in ErrNoMergeKey and
// refuses startup rather than silently dropping the entry.
func mergeOverlay(base *batchv1.Job, overlay map[string]any) (*batchv1.Job, error) {
	original, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	patch, err := json.Marshal(overlay)
	if err != nil {
		return nil, err
	}
	merged, err := strategicpatch.StrategicMergePatch(original, patch, batchv1.Job{})
	if err != nil {
		return nil, err
	}
	var job batchv1.Job
	if err := json.Unmarshal(merged, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// normalizeInitContainers pins the execution order §5.4 mandates: the built-in
// prepare first, then the overlay's appended entries in overlay writing order.
// The merged list cannot be trusted for it — SMP keeps patch items ahead of
// server-only items and $setElementOrder is banned.
func normalizeInitContainers(job *batchv1.Job, overlay map[string]any) {
	inits := job.Spec.Template.Spec.InitContainers
	if len(inits) == 0 {
		return
	}
	prepare := -1
	for i, c := range inits {
		if c.Name == containerPrepare {
			prepare = i
			break
		}
	}
	if prepare < 0 {
		return // the checker rejects the missing built-in prepare
	}
	order := overlayInitOrder(overlay)
	appended := make([]corev1.Container, 0, len(inits)-1)
	for i, c := range inits {
		if i != prepare {
			appended = append(appended, c)
		}
	}
	sort.SliceStable(appended, func(i, j int) bool {
		oi, iRanked := order[appended[i].Name]
		oj, jRanked := order[appended[j].Name]
		switch {
		case iRanked && jRanked:
			return oi < oj
		case iRanked != jRanked:
			return iRanked
		default:
			return false
		}
	})
	job.Spec.Template.Spec.InitContainers = append([]corev1.Container{inits[prepare]}, appended...)
}

// overlayInitOrder maps each overlay-declared init container name to its
// position in the overlay's own list (the 「书写顺序」 §5.4 preserves).
func overlayInitOrder(overlay map[string]any) map[string]int {
	order := map[string]int{}
	list, ok := listAt(overlay, "spec", "template", "spec", "initContainers")
	if !ok {
		return order
	}
	for i, entry := range list {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if name, ok := m["name"].(string); ok && name != "" {
			if _, seen := order[name]; !seen {
				order[name] = i
			}
		}
	}
	return order
}
