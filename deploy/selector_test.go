// Selector uniqueness inside the shipped deploy set. Every one of these
// objects is addressed by labels at runtime — `kubectl logs deploy/foreman`,
// the test cluster's stop_instance, the Service's endpoints — so a key shared
// between two workloads silently makes the address ambiguous instead of
// failing. These pins are the manifest-side judgment point.

package deploy

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// workload is a pod-owning object from the shipped manifests: the labels its
// selector matches on and the labels its pod template gives the pods.
type workload struct {
	file      string
	kind      string
	name      string
	selector  map[string]string
	podLabels map[string]string
}

// service is a Service from the shipped manifests: its own labels (what a
// ServiceMonitor selects on) and the pod labels it routes to.
type service struct {
	file     string
	name     string
	labels   map[string]string
	selector map[string]string
}

// TestWorkloadSelectorsAreDisjoint holds every pair of shipped workloads to
// separate pod sets. Two selectors that one pod can satisfy make every
// name-addressed read pick an arbitrary member: the scheduler Deployment and
// the shared-state StatefulSet both keyed on name=foreman, so
// `kubectl logs deploy/foreman` also matched foreman-redis-0. The component
// key is what separates them; dropping it from either side brings the
// collision back.
func TestWorkloadSelectorsAreDisjoint(t *testing.T) {
	workloads, _ := deployedObjects(t)
	if len(workloads) < 2 {
		t.Fatalf("found %d workloads in deploy/, want at least 2", len(workloads))
	}
	for i := range workloads {
		for j := i + 1; j < len(workloads); j++ {
			a, b := workloads[i], workloads[j]
			if selectorsOverlap(a.selector, b.selector) {
				t.Errorf("%s %s/%s (%s) and %s %s/%s (%s) select overlapping pod sets: one pod would be addressed by two objects",
					a.file, a.kind, a.name, formatLabels(a.selector),
					b.file, b.kind, b.name, formatLabels(b.selector))
			}
		}
	}
}

// TestWorkloadPodTemplatesCarryTheirSelector catches the other half of a
// selector edit: a selector that no longer matches its own template leaves the
// workload managing zero pods, so `kubectl logs deploy/foreman` resolves
// nothing at all.
func TestWorkloadPodTemplatesCarryTheirSelector(t *testing.T) {
	workloads, _ := deployedObjects(t)
	for _, w := range workloads {
		for key, want := range w.selector {
			got, present := w.podLabels[key]
			if !present || got != want {
				t.Errorf("%s %s/%s: pod template label %s = %q (present: %t), selector requires %q",
					w.file, w.kind, w.name, key, got, present, want)
			}
		}
	}
}

// TestServiceSelectorsRouteToOneWorkload rejects a Service selector that
// matches pods of two workloads: its traffic and named-port resolution would
// span objects it does not own.
func TestServiceSelectorsRouteToOneWorkload(t *testing.T) {
	workloads, services := deployedObjects(t)
	for _, svc := range services {
		if len(svc.selector) == 0 {
			continue
		}
		var matched []string
		for _, w := range workloads {
			if labelsMatch(svc.selector, w.podLabels) {
				matched = append(matched, w.kind+"/"+w.name)
			}
		}
		if len(matched) > 1 {
			t.Errorf("%s Service/%s selector %s matches %v: a Service must route to one workload",
				svc.file, svc.name, formatLabels(svc.selector), matched)
		}
	}
}

// TestServiceMonitorSelectsOnlyTheSchedulerService holds the optional scrape
// object to the scheduler Service. Sharing name=foreman with the shared-state
// Service makes Prometheus target a Service that carries no http port.
func TestServiceMonitorSelectsOnlyTheSchedulerService(t *testing.T) {
	_, services := deployedObjects(t)
	selector := serviceMonitorSelector(t)

	var matched []string
	for _, svc := range services {
		if labelsMatch(selector, svc.labels) {
			matched = append(matched, svc.name)
		}
	}
	if len(matched) != 1 || matched[0] != "foreman" {
		t.Fatalf("ServiceMonitor selector %s matches Services %v, want only foreman",
			formatLabels(selector), matched)
	}
}

// deployedObjects reads every manifest under deploy/ (and optional/) and
// returns the workloads and Services it declares. Other kinds are not read
// here: they either own no pods or are covered by their own pins.
func deployedObjects(t *testing.T) ([]workload, []service) {
	t.Helper()
	paths, err := filepath.Glob("*.yaml")
	if err != nil {
		t.Fatalf("list deploy manifests: %v", err)
	}
	optional, err := filepath.Glob(filepath.Join("optional", "*.yaml"))
	if err != nil {
		t.Fatalf("list optional manifests: %v", err)
	}
	paths = append(paths, optional...)

	var workloads []workload
	var services []service
	for _, path := range paths {
		for _, doc := range manifestDocuments(t, path) {
			jsonBytes, err := utilyaml.ToJSON(doc)
			if err != nil {
				t.Fatalf("%s document is not valid YAML: %v", path, err)
			}
			var typeMeta metav1.TypeMeta
			if err := json.Unmarshal(jsonBytes, &typeMeta); err != nil {
				t.Fatalf("%s document type: %v", path, err)
			}
			switch typeMeta.Kind {
			case "Deployment", "StatefulSet", "DaemonSet":
				var object struct {
					Metadata metav1.ObjectMeta `json:"metadata"`
					Spec     struct {
						Selector *metav1.LabelSelector `json:"selector"`
						Template struct {
							Metadata metav1.ObjectMeta `json:"metadata"`
						} `json:"template"`
					} `json:"spec"`
				}
				if err := json.Unmarshal(jsonBytes, &object); err != nil {
					t.Fatalf("%s %s: %v", path, typeMeta.Kind, err)
				}
				if object.Spec.Selector == nil {
					t.Fatalf("%s %s/%s declares no selector", path, typeMeta.Kind, object.Metadata.Name)
				}
				workloads = append(workloads, workload{
					file:      path,
					kind:      typeMeta.Kind,
					name:      object.Metadata.Name,
					selector:  object.Spec.Selector.MatchLabels,
					podLabels: object.Spec.Template.Metadata.Labels,
				})
			case "Service":
				var object corev1.Service
				if err := json.Unmarshal(jsonBytes, &object); err != nil {
					t.Fatalf("%s Service: %v", path, err)
				}
				services = append(services, service{
					file:     path,
					name:     object.Name,
					labels:   object.Labels,
					selector: object.Spec.Selector,
				})
			}
		}
	}
	return workloads, services
}

// serviceMonitorSelector returns the matchLabels of the shipped ServiceMonitor.
func serviceMonitorSelector(t *testing.T) map[string]string {
	t.Helper()
	for _, doc := range manifestDocuments(t, filepath.Join("optional", "servicemonitor.yaml")) {
		jsonBytes, err := utilyaml.ToJSON(doc)
		if err != nil {
			t.Fatalf("deploy/optional/servicemonitor.yaml document is not valid YAML: %v", err)
		}
		var typeMeta metav1.TypeMeta
		if err := json.Unmarshal(jsonBytes, &typeMeta); err != nil {
			t.Fatalf("deploy/optional/servicemonitor.yaml document type: %v", err)
		}
		if typeMeta.Kind != "ServiceMonitor" {
			continue
		}
		var object struct {
			Spec struct {
				Selector metav1.LabelSelector `json:"selector"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(jsonBytes, &object); err != nil {
			t.Fatalf("deploy/optional/servicemonitor.yaml ServiceMonitor: %v", err)
		}
		return object.Spec.Selector.MatchLabels
	}
	t.Fatal("deploy/optional/servicemonitor.yaml carries no ServiceMonitor")
	return nil
}

// selectorsOverlap reports whether some pod label set satisfies both selectors:
// every key they share must agree, and a key only one names is free.
func selectorsOverlap(a, b map[string]string) bool {
	for key, valueA := range a {
		if valueB, shared := b[key]; shared && valueB != valueA {
			return false
		}
	}
	return true
}

// labelsMatch reports whether the label set carries every selector entry.
func labelsMatch(selector, labels map[string]string) bool {
	for key, want := range selector {
		if got, present := labels[key]; !present || got != want {
			return false
		}
	}
	return true
}

// formatLabels renders a label set deterministically for failure messages.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	rendered := make([]string, 0, len(keys))
	for _, key := range keys {
		rendered = append(rendered, key+"="+labels[key])
	}
	return "{" + strings.Join(rendered, ",") + "}"
}
