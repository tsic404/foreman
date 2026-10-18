// TC-tech-catalog-06 / AC-31 (zero-parse negative): Foreman relays the probe's
// result as opaque bytes, so three checks must all come back empty — Foreman's
// logs carry no report-body field name, the catalog metrics carry no derived
// label, and nothing in the probe's container (or in Foreman's own deployment
// config) selects a provider command (ADR-019 决策结果 1 与符合性确认 1;
// 03-acceptance.md「模型目录」).
package acceptance

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
)

// bodyFieldMarkers are report-body field names (plus one field only the probe's
// protocol defines): a parsing Foreman would have them in a log line, a metric
// label or a struct tag somewhere on the relay path.
var bodyFieldMarkers = []string{"probe_body_marker", "selector", "thinking", "supported_levels", "models_count"}

func TestCatalog06ZeroParseNegative(t *testing.T) {
	// The fake server logs through the package default logger; capture it so the
	// negative covers every Foreman-side log line of the relay.
	serverLogs := &writeBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(serverLogs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	h := newHarness(t, true)
	h.useRealBuilder(t)
	h.upstream.registerIDs = map[string]string{"omp": "rid-omp", "zz": "rid-zz"}
	h.declare(declaredRuntimes())

	// A body full of field names Foreman must never touch: the relay carries it
	// byte for byte instead of interpreting it.
	const resultBody = `{"status":"completed","probe_body_marker":"m","models":[{"id":"m1","selector":"s1",` +
		`"thinking":[{"level":"off"}],"supported_levels":["off"]}],"models_count":1}`
	h.serveProbeDaemon(t, resultBody)

	// The production metric implementation, so the label-shape check reads what
	// a scrape of the real endpoint would.
	metrics := observability.NewMetrics()
	reporterLogs := &writeBuffer{}
	reporter := proxy.NewModelCatalogReporter(h.cfg, h.client, h.sched, h.slots, metrics,
		proxy.WithReporterLogger(slog.New(slog.NewJSONHandler(reporterLogs, nil))))
	h.runHeartbeatStream(t, reporter)

	id := h.requestModelList(t, "rid-omp")
	h.waitModelCompleted(t, id)
	waitFor(t, "the relay metric", 3*time.Second, func() bool {
		return strings.Contains(scrapeMetrics(t, metrics), `foreman_model_catalog_relays_total{code="200"} 1`)
	})

	// Check 1 — logs: no log line of the relay carries a body field name.
	logs := reporterLogs.String() + h.logs.String() + serverLogs.String()
	for _, marker := range bodyFieldMarkers {
		if strings.Contains(logs, marker) {
			t.Errorf("Foreman logs carry the report-body field name %q: %s", marker, logs)
		}
	}

	// Check 2 — metrics: no derived label. The outcome counter is labelled by
	// the C21 status alone, the span histogram by bucket bounds alone.
	exposition := scrapeMetrics(t, metrics)
	for _, family := range []string{"foreman_model_catalog_relays_total", "foreman_model_catalog_seconds"} {
		for _, labels := range seriesLabelNames(t, exposition, family) {
			if !slices.Equal(labels, []string{"code"}) && !slices.Equal(labels, []string{"le"}) && len(labels) != 0 {
				t.Errorf("%s series carries labels %v, want the status code or the bucket bound only", family, labels)
			}
		}
	}
	for _, marker := range bodyFieldMarkers {
		if strings.Contains(exposition, marker) {
			t.Errorf("the metrics exposition carries the report-body field name %q: %s", marker, exposition)
		}
	}

	// Check 3 — the container: the probe's payload is the deployment template's
	// own command, the official daemon under the probe's profile. A provider
	// command reappearing in this container (the form ADR-018 retired) would
	// break the equality.
	jobs := h.jobs.probeJobsCreated()
	if len(jobs) != 1 {
		t.Fatalf("probe Jobs = %v, want exactly one", jobs)
	}
	probe := h.createdJob(t, jobs[0])
	agent := probe.Spec.Template.Spec.Containers[0]
	if got := strings.Join(agent.Command, " "); got != "/usr/bin/dumb-init -- /usr/local/bin/multica" {
		t.Errorf("probe container command = %q, want the template's official daemon invocation", got)
	}
	if got := strings.Join(agent.Args, " "); got != "daemon start --foreground --profile foreman-catalog" {
		t.Errorf("probe container args = %q, want the daemon started under the probe profile", got)
	}
	for _, container := range probeContainers(probe) {
		for _, token := range append(append([]string(nil), container.Command...), container.Args...) {
			for _, runtime := range declaredRuntimes().Runtimes {
				if token == runtime.Type {
					t.Errorf("container %s invokes the provider command %q", container.Name, token)
				}
			}
		}
		for _, env := range container.Env {
			if strings.Contains(env.Name, "DISCOVERY") || strings.Contains(env.Name, "CATALOG_COMMAND") {
				t.Errorf("container %s carries a provider-command key %q", container.Name, env.Name)
			}
		}
	}

	// The same negative on Foreman's own configuration: the catalog surface is
	// the three deployed knobs, and no provider command is one of them.
	deployment := deployedForemanDeployment(t)
	for _, container := range deployment.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if strings.Contains(env.Name, "CATALOG") && !slices.Contains(catalogConfigKeys, env.Name) {
				t.Errorf("deployment env %q is a catalog key beyond the three pinned knobs", env.Name)
			}
		}
	}
}

// catalogConfigKeys is the catalog configuration surface of the deployment
// (02-contracts.md §5.1): the switch, the window and the cache TTL.
var catalogConfigKeys = []string{
	"FOREMAN_MODEL_CATALOG_ENABLED",
	"FOREMAN_MODEL_CATALOG_TIMEOUT",
	"FOREMAN_MODEL_CATALOG_CACHE_TTL",
}

// probeContainers returns every container of a rendered Job (init containers
// first) — the whole payload the node would start.
func probeContainers(job *batchv1.Job) []corev1.Container {
	containers := append([]corev1.Container(nil), job.Spec.Template.Spec.InitContainers...)
	return append(containers, job.Spec.Template.Spec.Containers...)
}

// scrapeMetrics renders one metrics registry the way the endpoint does.
func scrapeMetrics(t *testing.T, m *observability.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape /metrics = %d", rec.Code)
	}
	return rec.Body.String()
}

// seriesLabelNames returns the label names of every series of one metric family
// in the exposition (an empty list for an unlabelled series).
func seriesLabelNames(t *testing.T, exposition, family string) [][]string {
	t.Helper()
	var found [][]string
	for _, line := range strings.Split(exposition, "\n") {
		if line == "" || line[0] == '#' || !strings.HasPrefix(line, family) {
			continue
		}
		open := strings.Index(line, "{")
		if open < 0 {
			found = append(found, nil)
			continue
		}
		closeIdx := strings.Index(line, "}")
		if closeIdx < open {
			t.Fatalf("unparsable exposition line: %s", line)
		}
		var names []string
		for _, pair := range strings.Split(line[open+1:closeIdx], ",") {
			name, _, ok := strings.Cut(pair, "=")
			if !ok {
				t.Fatalf("unparsable label in %s", line)
			}
			names = append(names, name)
		}
		found = append(found, names)
	}
	if len(found) == 0 {
		t.Fatalf("no series of %s in the exposition", family)
	}
	return found
}
