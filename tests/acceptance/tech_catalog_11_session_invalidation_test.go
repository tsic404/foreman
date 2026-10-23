// TC-tech-catalog-11 / AC-32 ⑤: a catalog probe's Job Token dies with its
// session. Both production removals — Scheduler.DeleteProbe and
// SweepStaleProbes — take the session row with them, and the same token is then
// 401 on every probe route: workspaces, registration, heartbeat, deregistration
// and the result path. The registry-level removal is judged as well, so the
// teardown order above is not the single path to the 401s. The window between
// the lookup and the write-back is judged too: a session reaped before the
// registration update must not be resurrected (02-contracts.md §6.3/§6.5).
package acceptance

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

const (
	catalog11RequestID    = "req-11"
	catalog11ProbeName    = "fm-catalog-" + catalog11RequestID
	catalog11ProbeRuntime = "rt-" + catalog11RequestID
	catalog11RealRuntime  = "rid-omp"
)

// probeFace is one request the probe daemon may send while its session lives.
type probeFace struct {
	name   string
	method string
	path   string
	body   []byte
}

// probeFaces is the probe's route surface: one request per §2.2 endpoint that
// adjudicates on the session (S-1, S-4, S-5, S-19, S-27).
func probeFaces() []probeFace {
	register := []byte(`{"daemon_id":"` + catalog11ProbeName + `","runtimes":[{"name":"foreman-job","type":"omp"}]}`)
	heartbeat := []byte(`{"runtime_id":"` + catalog11ProbeRuntime + `"}`)
	resultPath := "/api/daemon/runtimes/" + catalog11ProbeRuntime + "/models/" + catalog11RequestID + "/result"
	return []probeFace{
		{name: "S1 workspaces", method: http.MethodGet, path: "/api/daemon/workspaces"},
		{name: "S4 register", method: http.MethodPost, path: "/api/daemon/register", body: register},
		{name: "S5 heartbeat", method: http.MethodPost, path: "/api/daemon/heartbeat", body: heartbeat},
		{name: "S19 deregister", method: http.MethodPost, path: "/api/daemon/deregister", body: []byte(`{}`)},
		{name: "S27 model result", method: http.MethodPost, path: resultPath, body: []byte(`{"models":[]}`)},
	}
}

// seedCatalogProbe issues the probe's own Job Token and inserts the session
// row the token is adjudicated against (§6.3).
func seedCatalogProbe(t *testing.T, h *harness) (string, registry.ProbeSession) {
	t.Helper()
	token, err := h.issuer.IssueProbe(catalog11ProbeName, catalog11RequestID, h.schedCfg.WorkspaceID, 10*time.Minute)
	if err != nil {
		t.Fatalf("IssueProbe: %v", err)
	}
	session := registry.ProbeSession{
		JobName:       catalog11ProbeName,
		Mode:          registry.ProbeCommandDriven,
		RequestID:     catalog11RequestID,
		TokenHash:     h.issuer.Hash(token),
		JobRuntime:    catalog11ProbeRuntime,
		RealRuntimeID: catalog11RealRuntime,
		WorkspaceID:   h.schedCfg.WorkspaceID,
		Deadline:      time.Now().Add(time.Minute),
	}
	if err := h.probes.PutProbe(session); err != nil {
		t.Fatalf("PutProbe: %v", err)
	}
	return token, session
}

func TestCatalog11ProbeTokenDiesWithItsSession(t *testing.T) {
	staleProbeRemnant := func(t *testing.T, h *harness) {
		t.Helper()
		grace := h.schedCfg.ModelCatalogTimeout + 2*h.schedCfg.ReconcileInterval
		stale := metav1.NewTime(time.Now().Add(-grace - time.Minute))
		labels := map[string]string{
			registry.LabelRole:        registry.RoleCatalogProbe,
			registry.LabelWorkspaceID: h.schedCfg.WorkspaceID,
		}
		h.jobs.programListed(
			[]batchv1.Job{{ObjectMeta: metav1.ObjectMeta{
				Name: catalog11ProbeName, Labels: labels, CreationTimestamp: stale,
			}}},
			[]corev1.Secret{{ObjectMeta: metav1.ObjectMeta{
				Name: catalog11ProbeName + "-cred", Labels: labels, CreationTimestamp: stale,
			}}},
		)
		if err := h.sched.SweepStaleProbes(t.Context()); err != nil {
			t.Fatalf("SweepStaleProbes: %v", err)
		}
	}

	for _, tc := range []struct {
		name   string
		remove func(t *testing.T, h *harness)
	}{
		{
			name: "deleted",
			remove: func(t *testing.T, h *harness) {
				t.Helper()
				if err := h.sched.DeleteProbe(t.Context(), scheduler.ProbeHandle{JobName: catalog11ProbeName}); err != nil {
					t.Fatalf("Scheduler.DeleteProbe: %v", err)
				}
			},
		},
		{
			name: "session dropped",
			remove: func(t *testing.T, h *harness) {
				t.Helper()
				if err := h.probes.DeleteProbe(catalog11ProbeName); err != nil {
					t.Fatalf("ProbeRegistry.DeleteProbe: %v", err)
				}
			},
		},
		{name: "swept", remove: staleProbeRemnant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, true)
			token, _ := seedCatalogProbe(t, h)
			faces := probeFaces()

			// Control: with the session in place the same token reaches all
			// five routes, so the 401s below are not a token-shape artefact.
			for _, face := range faces {
				code, body := h.call(face.method, face.path, token, face.body)
				if code != http.StatusOK {
					t.Fatalf("%s while the session lives = %d: %s, want 200", face.name, code, body)
				}
			}
			forwards := len(h.upstream.pathCalls("/api/daemon/runtimes/" + catalog11RealRuntime + "/models/"))
			if forwards == 0 {
				t.Fatal("the live session forwarded no result; the no-forward judgement below would be vacuous")
			}

			tc.remove(t, h)
			if _, ok := h.probes.ProbeByJob(catalog11ProbeName); ok {
				t.Fatal("the session survived its removal")
			}
			for _, face := range faces {
				code, body := h.call(face.method, face.path, token, face.body)
				if code != http.StatusUnauthorized {
					t.Errorf("%s after the session was removed = %d: %s, want 401", face.name, code, body)
				}
			}
			if got := len(h.upstream.pathCalls("/api/daemon/runtimes/" + catalog11RealRuntime + "/models/")); got != forwards {
				t.Errorf("result forwards after the removal = %d, want the live session's %d", got, forwards)
			}
		})
	}
}

// reapOnWriteProbeStore is the shared session face whose row disappears at the
// first write: the reap lands between the handler's lookup and its update.
type reapOnWriteProbeStore struct {
	rows map[string][]byte
}

func (s *reapOnWriteProbeStore) PutProbeSession(_ context.Context, jobName string, raw []byte, _ time.Duration) error {
	s.rows[jobName] = raw
	return nil
}

func (s *reapOnWriteProbeStore) CASProbeSession(_ context.Context, jobName string, _, _ []byte, _ time.Duration) (bool, error) {
	delete(s.rows, jobName)
	return false, nil
}

func (s *reapOnWriteProbeStore) GetProbeSession(_ context.Context, jobName string) ([]byte, bool, error) {
	raw, ok := s.rows[jobName]
	return raw, ok, nil
}

func (s *reapOnWriteProbeStore) DeleteProbeSession(_ context.Context, jobName string) error {
	delete(s.rows, jobName)
	return nil
}

func (s *reapOnWriteProbeStore) ListProbeSessions(context.Context) (map[string][]byte, error) {
	out := make(map[string][]byte, len(s.rows))
	for name, raw := range s.rows {
		out[name] = raw
	}
	return out, nil
}

// TestCatalog11ReapBetweenLookupAndWriteIsUnauthorized pins the registration
// update as the authority: the session is still resolvable when the request is
// authenticated and gone when the write lands, so the answer is 401 and the
// row is not written back.
func TestCatalog11ReapBetweenLookupAndWriteIsUnauthorized(t *testing.T) {
	h := newHarness(t, true)
	token, session := seedCatalogProbe(t, h)
	raw, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}
	store := &reapOnWriteProbeStore{rows: map[string][]byte{catalog11ProbeName: raw}}
	h.probes.AttachShared(store)

	body := []byte(`{"daemon_id":"` + catalog11ProbeName + `","runtimes":[{"name":"foreman-job","type":"omp"}]}`)
	code, resp := h.call(http.MethodPost, "/api/daemon/register", token, body)
	if code != http.StatusUnauthorized {
		t.Fatalf("register of a reaped session = %d: %s, want 401", code, resp)
	}
	if _, ok := store.rows[catalog11ProbeName]; ok {
		t.Error("the update must not resurrect a reaped session row")
	}
}
