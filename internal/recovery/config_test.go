package recovery

import (
	"context"
	"errors"
	"testing"

	"github.com/tsic404/foreman/internal/registry"
)

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ReconcileInterval != DefaultReconcileInterval {
		t.Fatalf("ReconcileInterval = %s, want %s", cfg.ReconcileInterval, DefaultReconcileInterval)
	}

	cfg, err = LoadConfig(func(key string) string {
		if key == EnvReconcileInterval {
			return "5s"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ReconcileInterval.String() != "5s" {
		t.Fatalf("ReconcileInterval = %s, want 5s", cfg.ReconcileInterval)
	}
}

func TestLoadConfigRejectsBadInterval(t *testing.T) {
	for _, raw := range []string{"soon", "0s", "-3s"} {
		if _, err := LoadConfig(func(key string) string {
			if key == EnvReconcileInterval {
				return raw
			}
			return ""
		}); err == nil {
			t.Fatalf("LoadConfig(%q): want an error", raw)
		}
	}
}

func TestPendingReportsDirFollowsStateRoot(t *testing.T) {
	got := PendingReportsDir(func(key string) string {
		if key == EnvStateRoot {
			return "/srv/foreman/"
		}
		return ""
	})
	if got != "/srv/foreman/pending-reports" {
		t.Fatalf("PendingReportsDir = %q", got)
	}
	if got := PendingReportsDir(func(string) string { return "" }); got != DefaultStateRoot+"/pending-reports" {
		t.Fatalf("PendingReportsDir default = %q", got)
	}
}

func TestCompensatorFailDelegatesAndSurfacesErrors(t *testing.T) {
	set := &fakeSettler{}
	c := NewCompensator(set, nil)
	e := testEntry("t1")

	if err := c.Fail(context.Background(), e, "job_failed"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if len(set.failJobs) != 1 || set.failJobs[0] != (failCall{"fm-t1", "job_failed"}) {
		t.Fatalf("failJobs = %+v, want [fm-t1 job_failed]", set.failJobs)
	}

	set.err = errors.New("server down")
	if err := c.Fail(context.Background(), e, "job_failed"); err == nil {
		t.Fatal("Fail: want the settlement error surfaced for the next round")
	}
}

func TestCompensatorFailSettlesTerminalEntryOnly(t *testing.T) {
	set := &fakeSettler{}
	c := NewCompensator(set, nil)
	e := testEntry("t1")
	e.State = registry.StateTerminal
	e.Result = registry.ResultFailed

	if err := c.Fail(context.Background(), e, "job_failed"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if len(set.failJobs) != 0 {
		t.Fatalf("failJobs = %+v, want no second report for a terminal entry", set.failJobs)
	}
	if len(set.settles) != 1 || set.settles[0] != (settleCall{"t1", "failed"}) {
		t.Fatalf("settles = %+v, want the lingering objects cleaned up", set.settles)
	}
}
