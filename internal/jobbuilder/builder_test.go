package jobbuilder

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/foreman/internal/registry"
)

const (
	testTaskID  = "3f6b1a2c-9d4e-4a5b-8c7d-0e1f2a3b4c5d"
	testJobName = "fm-" + testTaskID
	testDigest  = "sha256:4242424242424242424242424242424242424242424242424242424242424242"
	// testPriorSession is testEntry's session-resume path, in the shape the
	// payload carries it: an absolute path to a node-local session file.
	testPriorSession = "/state/home/.multica/pi-sessions/sess-1.jsonl"
)

// testSessionKey is the index key of testPriorSession — the key the learning
// side and the payload side must derive identically (§4.5).
const testSessionKey = "sess-1"

var testClaimedAt = time.Date(2026, 10, 4, 8, 30, 0, 0, time.UTC)

type fakeIssuer struct {
	jobName     string
	taskID      string
	workspaceID string
	ttl         time.Duration
	token       string
	err         error

	// The probe face of the same seam (ADR-019): recorded beside the task
	// call's arguments, so a test can pin the probe's TTL arithmetic.
	probeJobName     string
	probeRequestID   string
	probeWorkspaceID string
	probeTTL         time.Duration
}

func (f *fakeIssuer) Issue(jobName, taskID, workspaceID string, ttl time.Duration) (string, error) {
	f.jobName, f.taskID, f.workspaceID, f.ttl = jobName, taskID, workspaceID, ttl
	if f.err != nil {
		return "", f.err
	}
	return f.token, nil
}

func (f *fakeIssuer) IssueProbe(jobName, requestID, workspaceID string, ttl time.Duration) (string, error) {
	f.probeJobName, f.probeRequestID, f.probeWorkspaceID, f.probeTTL = jobName, requestID, workspaceID, ttl
	if f.err != nil {
		return "", f.err
	}
	return f.token, nil
}

// Hash mirrors auth.Issuer.Hash: the hex sha256 of the token, the digest
// OnClaim records as token_hash (ADR-017).
func (f *fakeIssuer) Hash(token string) string { return tokenDigest(token) }

// tokenDigest is the digest of a token; the real issuer's Hash is the same
// hex sha256, so the tests can recompute what Build returns.
func tokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// fakeNodes is a NodeIndex keyed by session key, the way the registry stores
// it, with nothing saturated.
type fakeNodes map[string]string

func (n fakeNodes) LastNodeForSession(key string) string { return n[key] }
func (n fakeNodes) Saturated(string) bool                { return false }

// sessionNodeIndex is the index the affinity tests hand to the builder: the
// test entry's session, learned on node.
func sessionNodeIndex(node string) fakeNodes { return fakeNodes{testSessionKey: node} }

// saturatedNodes is a node index whose node sits at its per-node soft cap.
type saturatedNodes struct {
	fakeNodes
	node string
}

func (n saturatedNodes) Saturated(node string) bool { return node == n.node }

func testConfig() Config {
	cfg, err := LoadConfig(envFrom(map[string]string{}))
	if err != nil {
		panic(err)
	}
	// The template is required, so every builder needs one: leaving the
	// deployment path unset lets newTestBuilder supply the reference template,
	// while templateConfig points at a test-owned manifest.
	cfg.TemplatePath = ""
	return cfg
}

func testEntry() TaskEntry {
	return TaskEntry{
		TaskID:          testTaskID,
		AgentID:         "0a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
		IssueID:         "aa10a105-59cf-4b6c-964e-4f857b580bcf",
		IssueIdentifier: "MULTI-9000",
		WorkspaceID:     "c20ff019-2980-4936-8cb0-22d79d694e71",
		JobRuntimeID:    "5d4c3b2a-1e0f-4a9b-8c7d-6e5f4a3b2c1d",
		ClaimedAt:       testClaimedAt,
		Attempt:         1,
		PriorSessionID:  testPriorSession,
	}
}

func mustBuild(t *testing.T, cfg Config, e TaskEntry) (*batchv1.Job, *corev1.Secret) {
	t.Helper()
	issuer := &fakeIssuer{token: "fmj_test.sig"}
	cfg.Issuer = issuer
	job, secret, tokenHash, err := newTestBuilder(t, cfg).Build(e)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if job == nil || secret == nil {
		t.Fatalf("Build returned nil objects (job=%v, secret=%v)", job, secret)
	}
	// The digest Build returns is the token_hash OnClaim records, so it must
	// be the digest of the very token the Secret carries (ADR-017).
	var config struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(secret.StringData["config.json"]), &config); err != nil {
		t.Fatalf("config.json is not valid JSON: %v", err)
	}
	if want := issuer.Hash(config.Token); tokenHash != want {
		t.Fatalf("Build token hash = %q, want the digest %q of the Secret's token", tokenHash, want)
	}
	return job, secret
}

// newTestBuilder runs the startup gate over cfg. The template is required
// (ADR-030), so a config without one gets the reference template — the
// baseline's Job document, byte for byte.
func newTestBuilder(t *testing.T, cfg Config) *Builder {
	t.Helper()
	if cfg.TemplatePath == "" {
		cfg.TemplatePath = referenceTemplatePath(t)
	}
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	builder, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// TestBuildKeepsJobImageReferenceVerbatim (AC-13): the configured reference
// reaches both containers exactly as given — a digest-carrying reference
// included. The builder appends and normalizes nothing (ADR-012).
func TestBuildKeepsJobImageReferenceVerbatim(t *testing.T) {
	for _, ref := range []string{
		"ghcr.io/tsic404/foreman-job:v0.1.0",
		"ghcr.io/tsic404/foreman-job@" + testDigest,
		"ghcr.io/tsic404/foreman-job:v0.1.0@" + testDigest,
	} {
		cfg := testConfig()
		cfg.JobImage = ref
		job, _ := mustBuild(t, cfg, testEntry())
		spec := job.Spec.Template.Spec
		if spec.Containers[0].Image != ref {
			t.Errorf("agent image = %q, want the verbatim %q", spec.Containers[0].Image, ref)
		}
		if spec.InitContainers[0].Image != ref {
			t.Errorf("prepare image = %q, want the verbatim %q", spec.InitContainers[0].Image, ref)
		}
	}
}

func TestBuildPreferredNodeAffinity(t *testing.T) {
	entry := testEntry()

	cfg := testConfig()
	cfg.Nodes = sessionNodeIndex("company-02")
	job, _ := mustBuild(t, cfg, entry)
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		t.Fatal("affinity missing for a learned session node")
	}
	terms := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].Weight != 100 {
		t.Fatalf("preferred terms = %+v, want exactly 1 term weight 100", terms)
	}
	expr := terms[0].Preference.MatchExpressions
	if len(expr) != 1 || expr[0].Key != "kubernetes.io/hostname" || expr[0].Operator != corev1.NodeSelectorOpIn ||
		len(expr[0].Values) != 1 || expr[0].Values[0] != "company-02" {
		t.Errorf("affinity expression = %+v, want hostname In [company-02]", expr)
	}

	cfg.PreferNodeReuse = false
	job, _ = mustBuild(t, cfg, entry)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted when FOREMAN_PREFER_NODE_REUSE=false")
	}

	cfg = testConfig()
	cfg.Nodes = fakeNodes{}
	job, _ = mustBuild(t, cfg, entry)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted when the session key was never learned")
	}

	// A first run carries no prior session, so it has no key at all — the
	// loaded index must not reach it (ADR-015: issue_id is not a key either).
	firstRun := entry
	firstRun.PriorSessionID = ""
	cfg = testConfig()
	cfg.Nodes = sessionNodeIndex("company-02")
	job, _ = mustBuild(t, cfg, firstRun)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted for a task with no prior session")
	}

	// Another session on the same node is not this task's session.
	otherSession := entry
	otherSession.PriorSessionID = "/state/home/.multica/pi-sessions/sess-9.jsonl"
	job, _ = mustBuild(t, cfg, otherSession)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted for an unlearned session key")
	}
}

// The two sides of the index derive the key with one function (§4.5): the
// learning face stored a path under its session key, and a payload naming the
// same file — a different root directory, as a changed HOME gives it — still
// resolves it. The real registry is the witness that both sides match.
func TestBuildPreferredNodeFromLearnedRegistry(t *testing.T) {
	reg := registry.New(time.Now)
	reg.LearnSession(registry.SessionKey("/state/home/.multica/pi-sessions/sess-1.jsonl"), "company-02")

	entry := testEntry()
	entry.PriorSessionID = "/root/.multica/pi-sessions/sess-1.jsonl"

	cfg := testConfig()
	cfg.Nodes = reg
	job, _ := mustBuild(t, cfg, entry)
	terms := job.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("preferred terms = %+v, want the one learned session item", terms)
	}
	if got := terms[0].Preference.MatchExpressions[0].Values; len(got) != 1 || got[0] != "company-02" {
		t.Errorf("resolved node = %v, want company-02", got)
	}
}

// A node at FOREMAN_MAX_JOBS_PER_NODE is no longer a candidate for the next
// Job of the session (ADR-006 节流); the Job itself is still rendered.
func TestBuildDropsAffinityForSaturatedNode(t *testing.T) {
	entry := testEntry()

	cfg := testConfig()
	cfg.Nodes = saturatedNodes{fakeNodes: sessionNodeIndex("company-02"), node: "company-02"}
	job, _ := mustBuild(t, cfg, entry)
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("affinity must be omitted for a node at its per-node cap")
	}

	// Another node's saturation leaves the reuse affinity in place.
	cfg.Nodes = saturatedNodes{fakeNodes: sessionNodeIndex("company-02"), node: "company-03"}
	job, _ = mustBuild(t, cfg, entry)
	if job.Spec.Template.Spec.Affinity == nil {
		t.Error("affinity must survive when only an unrelated node is saturated")
	}
}

// TestBuildTriggerContextAnnotations (AC-24): the four trigger-context keys are
// independent conditional blocks — a non-empty field writes its key with that
// value, an empty field writes no key at all. The issue group carries its two
// mixed forms as well: an issue id without an issue identifier (and the
// reverse) is legal, so neither key may be written from the other's value. The
// last two cases (autopilot and chat entry, no issue context) are the payload
// shapes the build path used to refuse (ADR-014).
func TestBuildTriggerContextAnnotations(t *testing.T) {
	const (
		issueID        = "aa10a105-59cf-4b6c-964e-4f857b580bcf"
		issueKey       = "MULTI-9000"
		autopilotRunID = "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"
		chatSessionID  = "c1d2e3f4-5061-4b8c-9d0e-1f2a3b4c5d6e"
	)
	cases := []struct {
		name   string
		mutate func(*TaskEntry)
		want   map[string]string
	}{
		{"issue trigger", func(*TaskEntry) {}, map[string]string{
			annoIssueID:         issueID,
			annoIssueIdentifier: issueKey,
		}},
		{"issue trigger, identifier absent", func(e *TaskEntry) {
			e.IssueIdentifier = ""
		}, map[string]string{annoIssueID: issueID}},
		{"issue trigger, id absent", func(e *TaskEntry) {
			e.IssueID = ""
		}, map[string]string{annoIssueIdentifier: issueKey}},
		{"autopilot trigger", func(e *TaskEntry) {
			e.IssueID, e.IssueIdentifier, e.AutopilotRunID = "", "", autopilotRunID
		}, map[string]string{annoAutopilotRunID: autopilotRunID}},
		{"chat entry trigger", func(e *TaskEntry) {
			e.IssueID, e.IssueIdentifier, e.ChatSessionID = "", "", chatSessionID
		}, map[string]string{annoChatSessionID: chatSessionID}},
		{"no trigger context", func(e *TaskEntry) {
			e.IssueID, e.IssueIdentifier = "", ""
		}, map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := testEntry()
			tc.mutate(&entry)
			job, _ := mustBuild(t, testConfig(), entry)

			for _, key := range triggerContextAnnotationKeys {
				want, wanted := tc.want[key]
				got, present := job.Annotations[key]
				switch {
				case !wanted && present:
					t.Errorf("annotation %q = %q, want the key absent", key, got)
				case wanted && (!present || got != want):
					t.Errorf("annotation %q = %q (present=%v), want %q", key, got, present, want)
				}
			}
			for _, key := range requiredAnnotationKeys {
				if job.Annotations[key] == "" {
					t.Errorf("mandatory annotation %q must stay non-empty", key)
				}
			}
			for key, value := range job.Annotations {
				if strings.HasPrefix(key, foremanDomain) && value == "" {
					t.Errorf("annotation %q is an empty placeholder", key)
				}
			}
		})
	}
}

// TestBuildAffinityWithoutIssueContext (ADR-015): the affinity key is the
// session, never the trigger context — the no-issue-context entries (chat
// entry, autopilot) the ADR was written for resume on their session's node.
// An issue-context gate here would silently re-break exactly that case.
func TestBuildAffinityWithoutIssueContext(t *testing.T) {
	entry := testEntry()
	entry.IssueID, entry.IssueIdentifier = "", ""
	entry.AutopilotRunID = "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"

	cfg := testConfig()
	cfg.Nodes = sessionNodeIndex("company-02")
	job, _ := mustBuild(t, cfg, entry)
	affinity := job.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		t.Fatal("affinity missing: the key is the session, not the issue context")
	}
	terms := affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("preferred terms = %+v, want the one learned session item", terms)
	}
	if got := terms[0].Preference.MatchExpressions[0].Values; len(got) != 1 || got[0] != "company-02" {
		t.Errorf("resolved node = %v, want company-02", got)
	}
}

func TestBuildInvalidTaskID(t *testing.T) {
	for _, taskID := range []string{
		"",
		"UPPER-CASE",
		"has space",
		"underscore_ok_no",
		strings.Repeat("a", 62), // fm- + 62 chars exceeds the 63-char DNS-1123 label limit
	} {
		entry := testEntry()
		entry.TaskID = taskID
		job, secret, tokenHash, err := newTestBuilder(t, testConfigWithIssuer()).Build(entry)
		if err == nil {
			t.Errorf("taskID %q: expected error, got job=%v", taskID, job != nil)
		}
		if !errors.Is(err, ErrInvalidTaskID) {
			t.Errorf("taskID %q: error must wrap ErrInvalidTaskID (failure_reason=invalid_task_id), got %v", taskID, err)
		}
		if job != nil || secret != nil || tokenHash != "" {
			t.Errorf("taskID %q: objects and digest must be zero on error", taskID)
		}
	}
}

func TestBuildIssuerError(t *testing.T) {
	cfg := testConfig()
	cfg.Issuer = &fakeIssuer{err: errors.New("boom")}
	if _, _, _, err := newTestBuilder(t, cfg).Build(testEntry()); err == nil {
		t.Fatal("expected issuer error to propagate")
	}
}

func TestBuildMissingIssuer(t *testing.T) {
	if _, _, _, err := newTestBuilder(t, testConfig()).Build(testEntry()); err == nil {
		t.Fatal("expected error when Config.Issuer is unset")
	}
}

func TestBuildSecret(t *testing.T) {
	cfg := testConfig()
	issuer := &fakeIssuer{token: "fmj_payload.sig"}
	cfg.Issuer = issuer
	entry := testEntry()
	_, secret, tokenHash, err := newTestBuilder(t, cfg).Build(entry)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if issuer.jobName != testJobName || issuer.taskID != entry.TaskID || issuer.workspaceID != entry.WorkspaceID || issuer.ttl != cfg.JobTokenTTL {
		t.Errorf("Issue called with (%q, %q, %q, %s)", issuer.jobName, issuer.taskID, issuer.workspaceID, issuer.ttl)
	}
	if secret.Name != testJobName+"-cred" || secret.Namespace != DefaultJobNamespace {
		t.Errorf("secret %q/%q", secret.Namespace, secret.Name)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/managed-by": "foreman",
		"foreman.tsic.top/task-id":     testTaskID,
	}
	if len(secret.Labels) != len(wantLabels) {
		t.Fatalf("secret labels = %v, want exactly %v", secret.Labels, wantLabels)
	}
	for k, v := range wantLabels {
		if secret.Labels[k] != v {
			t.Errorf("secret label %q = %q, want %q", k, secret.Labels[k], v)
		}
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", secret.Type)
	}
	var config struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(secret.StringData["config.json"]), &config); err != nil {
		t.Fatalf("config.json is not valid JSON: %v", err)
	}
	if config.Token != "fmj_payload.sig" {
		t.Errorf("config.json token = %q, want the issued token", config.Token)
	}
	// Build returns the digest of that token — the token_hash OnClaim
	// records (ADR-017).
	if want := tokenDigest(config.Token); tokenHash != want {
		t.Errorf("Build token hash = %q, want %q", tokenHash, want)
	}
}

func testConfigWithIssuer() Config {
	cfg := testConfig()
	cfg.Issuer = &fakeIssuer{token: "fmj_test.sig"}
	return cfg
}
