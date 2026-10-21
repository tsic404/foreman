// TC-tech-catalog-10 / AC-80: the probe budget's two boundaries. The catalog
// budget's startup self-check refuses a value below three probe heartbeat
// intervals, and the rendered activeDeadlineSeconds rounds a positive budget
// up to whole seconds — a sub-second budget renders one second, never the zero
// that would make the Job expire at creation; a non-positive budget writes
// nothing.
package acceptance

import (
	"strings"
	"testing"
	"time"

	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
)

func TestCatalog10ProbeBudgetBoundaries(t *testing.T) {
	// The self-check face: the loader main runs at startup.
	env := func(timeout string) func(string) string {
		base := map[string]string{
			proxy.EnvServerURL:           "http://multica.example",
			proxy.EnvWorkspaceID:         "ws-1",
			proxy.EnvModelCatalogTimeout: timeout,
		}
		return func(key string) string { return base[key] }
	}
	if _, err := proxy.LoadConfig(env("30s")); err == nil {
		t.Fatal("a catalog budget below three heartbeat intervals was accepted")
	} else {
		// AC-80's refusal criterion: the error states the inequality and the
		// actual value, not just the key name.
		for _, want := range []string{proxy.EnvModelCatalogTimeout, ">= 3x", "30s"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q does not name %q", err, want)
			}
		}
	}
	if _, err := proxy.LoadConfig(env("45s")); err != nil {
		t.Fatalf("the boundary budget was refused: %v", err)
	}

	// The rendering face: the production builder over the deployment template.
	h := newHarness(t, true)
	h.useRealBuilder(t)
	cases := []struct {
		requestID string
		budget    time.Duration
		want      int64
	}{
		{"req-below-second", 500 * time.Millisecond, 1},
		{"req-fraction", 1500 * time.Millisecond, 2},
		{"req-whole", 45 * time.Second, 45},
		// A non-positive budget writes nothing: the reference template's own
		// value (deploy/25-job-template.yaml) survives.
		{"req-no-budget", 0, 86400},
	}
	for _, tc := range cases {
		if _, err := h.sched.StartProbe(t.Context(), tc.requestID, "", "", registry.ProbeRegisterOnly, tc.budget); err != nil {
			t.Fatalf("StartProbe(%s): %v", tc.requestID, err)
		}
		job := h.createdJob(t, "fm-catalog-"+tc.requestID)
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != tc.want {
			t.Errorf("%s: activeDeadlineSeconds = %v, want %d", tc.requestID, job.Spec.ActiveDeadlineSeconds, tc.want)
		}
	}
}
