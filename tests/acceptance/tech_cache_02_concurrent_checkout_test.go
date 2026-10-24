// TC-tech-cache-02 (AC-12 / ADR-006): two tasks on one node check out the same
// repository through the shared .repos bare cache without corrupting it. The
// case builds its own preconditions — an origin repository, the node directory
// both rendered Jobs mount, and a first checkout that fills the cache — then
// races the daemon's checkout sequence (bare clone or fetch, then worktree
// add) for both tasks, judging each task's success, a clean git fsck, a cache
// reused rather than rebuilt, and reuse after the burst. The live-cluster
// observation of the tasks themselves runs from tech-cache-02-cluster.sh.
package acceptance

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/tsic404/foreman/internal/scheduler"
)

// tcCache02App is the file every checkout must carry at the fetched head.
const tcCache02App = "src/app.txt"

// tcCache02Git runs one git command and returns its combined output.
func tcCache02Git(args ...string) (string, error) {
	out, err := exec.Command("git", args...).CombinedOutput()
	return string(out), err
}

// tcCache02MustGit runs one git command whose failure fails the case.
func tcCache02MustGit(t *testing.T, args ...string) string {
	t.Helper()
	out, err := tcCache02Git(args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(out)
}

// tcCache02Commit writes the checkout's file and commits it, returning the new
// head. The local identity keeps the fixture independent of the runner's git
// configuration.
func tcCache02Commit(t *testing.T, repository, message, content string) string {
	t.Helper()
	path := filepath.Join(repository, tcCache02App)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	tcCache02MustGit(t, "-C", repository, "add", "-A")
	tcCache02MustGit(t, "-C", repository, "-c", "user.name=tc-cache-02",
		"-c", "user.email=tc-cache-02@example.invalid", "commit", "-qm", message)
	return tcCache02MustGit(t, "-C", repository, "rev-parse", "HEAD")
}

// tcCache02Source creates the origin repository the two tasks check out.
func tcCache02Source(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "origin")
	tcCache02MustGit(t, "init", "-q", "-b", "main", dir)
	return dir
}

// The two paths a checkout can take against the shared cache: creating it
// (clone) or reusing it (fetch). AC-12's reuse face is judged on this value.
const (
	tcCache02Clone = "clone"
	tcCache02Reuse = "fetch"
)

// tcCache02CheckoutOnce is one task's checkout against the shared cache: the
// bare clone when the cache is absent, a fetch when it is present, then the
// task's own detached worktree at the fetched head. It returns the path taken
// and the failing command's output, so the caller can judge both.
func tcCache02CheckoutOnce(source, bare, worktree string) (string, string, error) {
	mode := tcCache02Reuse
	if _, err := os.Stat(filepath.Join(bare, "HEAD")); err != nil {
		mode = tcCache02Clone
		if out, err := tcCache02Git("clone", "--bare", source, bare); err != nil {
			return mode, out, fmt.Errorf("bare clone: %w", err)
		}
	} else if out, err := tcCache02Git("-C", bare, "fetch", "--prune", "origin",
		"+refs/heads/*:refs/heads/*"); err != nil {
		return mode, out, fmt.Errorf("fetch: %w", err)
	}
	if out, err := tcCache02Git("-C", bare, "worktree", "add", "--detach", worktree, "refs/heads/main"); err != nil {
		return mode, out, fmt.Errorf("worktree add: %w", err)
	}
	return mode, "", nil
}

// tcCache02Result records one checkout: the paths its attempts took, the first
// attempt's output when it failed, and the final outcome.
type tcCache02Result struct {
	attempts int
	modes    []string
	firstOut string
	err      error
}

// tcCache02Checkout runs one task's checkout, allowing the single busy retry
// ADR-006 documents. A failed worktree add can leave its registration behind,
// so the retry prunes it first. The first attempt's output is kept: whether the
// retry was owed to a git lock conflict is a judged fact, not an assumption.
func tcCache02Checkout(source, bare, worktree string) tcCache02Result {
	res := tcCache02Result{attempts: 1}
	mode, out, err := tcCache02CheckoutOnce(source, bare, worktree)
	res.modes = append(res.modes, mode)
	if err == nil {
		return res
	}
	res.firstOut = out
	res.attempts = 2
	if rmErr := os.RemoveAll(worktree); rmErr != nil {
		res.err = fmt.Errorf("%v; clear %s: %v", err, worktree, rmErr)
		return res
	}
	if pruneOut, pruneErr := tcCache02Git("-C", bare, "worktree", "prune"); pruneErr != nil {
		res.err = fmt.Errorf("%v; worktree prune: %v\n%s", err, pruneErr, pruneOut)
		return res
	}
	mode, out, err = tcCache02CheckoutOnce(source, bare, worktree)
	res.modes = append(res.modes, mode)
	if err != nil {
		res.err = fmt.Errorf("%v; second attempt: %w\n%s", res.firstOut, err, out)
	}
	return res
}

// tcCache02IsLockConflict reports the failure shape ADR-006 predicts: git
// refused the concurrent write on one of its own locks rather than the cache
// being damaged.
func tcCache02IsLockConflict(out string) bool {
	lower := strings.ToLower(out)
	if !strings.Contains(lower, "lock") {
		return false
	}
	return strings.Contains(lower, "file exists") || strings.Contains(lower, "cannot lock") ||
		strings.Contains(lower, "could not lock") || strings.Contains(lower, "unable to create")
}

// tcCache02SharedRoot returns the hostPath both rendered Jobs mount as their
// workspaces volume — the node directory two pods share. The renders must
// agree: a per-task rewrite would give the tasks separate caches, which is the
// arrangement this case is about.
func tcCache02SharedRoot(t *testing.T, jobs ...*batchv1.Job) string {
	t.Helper()
	root := ""
	for i, job := range jobs {
		volume := volumeByName(t, job.Spec.Template.Spec.Volumes, "workspaces")
		if volume.HostPath == nil {
			t.Fatalf("job %s: the workspaces volume is not a hostPath: %+v", job.Name, volume)
		}
		if i == 0 {
			root = volume.HostPath.Path
			continue
		}
		if volume.HostPath.Path != root {
			t.Errorf("job %s: workspaces hostPath = %q, want job %s's %q",
				job.Name, volume.HostPath.Path, jobs[0].Name, root)
		}
	}
	return root
}

func TestCache02ConcurrentCheckoutLeavesCacheIntact(t *testing.T) {
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)

	// Two tasks of different agents, both placed on the node the precondition
	// names; they stay live until their reports at the end of the case.
	tasks := []string{"task-1", "task-2"}
	for i, id := range tasks {
		tcClaimOnClaim(t, h, tcClaimPayloadBody{
			ID: id, AgentID: fmt.Sprintf("agent-%d", i+1), WorkspaceID: "ws-1", RuntimeID: "rid-zz",
			IssueID: "issue-1", IssueIdentifier: "TSI-1",
			Agent: tcAgentObject{Name: fmt.Sprintf("agent-%d", i+1)},
		})
		h.reg.SetNode(id, "node-a")
	}
	// Same node is the case's given input: the two pods share the workspaces
	// hostPath only because both tasks land on node-a. The placed-node read-back
	// is the cluster script's observation, not a judgement here.

	root := tcCache02SharedRoot(t, h.createdJob(t, "fm-task-1"), h.createdJob(t, "fm-task-2"))
	sandbox := t.TempDir()
	shared := filepath.Join(sandbox, root)
	bare := filepath.Join(shared, ".repos", "ws-1", "tc-cache-02.git")
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatalf("create the cache parent: %v", err)
	}

	// Precondition: the node's first checkout creates the cache; the burst
	// below must then find it present and take the reuse path.
	source := tcCache02Source(t)
	seed := tcCache02Commit(t, source, "seed", "seed\n")
	seedRun := tcCache02Checkout(source, bare, filepath.Join(sandbox, "worktree-seed"))
	if seedRun.err != nil || seedRun.modes[0] != tcCache02Clone {
		t.Fatalf("seed checkout = %v (modes %v), want the clone path", seedRun.err, seedRun.modes)
	}
	if got := tcCache02MustGit(t, "-C", filepath.Join(sandbox, "worktree-seed"), "rev-parse", "HEAD"); got != seed {
		t.Fatalf("seed worktree head = %s, want %s", got, seed)
	}

	// A commit lands on the origin after the seed: both racing checkouts must
	// bring it into the shared cache, which is the ref-update conflict the
	// design's native-lock story is about.
	want := tcCache02Commit(t, source, "second", "second\n")

	worktrees := []string{filepath.Join(sandbox, "worktree-task-1"), filepath.Join(sandbox, "worktree-task-2")}
	results := make([]tcCache02Result, len(worktrees))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range worktrees {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = tcCache02Checkout(source, bare, worktrees[i])
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range worktrees {
		res := results[i]
		if res.err != nil {
			t.Errorf("task-%d checkout failed after %d attempts: %v", i+1, res.attempts, res.err)
			continue
		}
		for _, mode := range res.modes {
			if mode != tcCache02Reuse {
				t.Errorf("task-%d took the %s path; the burst must reuse the cache the seed created", i+1, mode)
			}
		}
		if res.attempts == 2 && !tcCache02IsLockConflict(res.firstOut) {
			t.Errorf("task-%d retried after a non-lock failure:\n%s", i+1, res.firstOut)
		}
		if got := tcCache02MustGit(t, "-C", worktrees[i], "rev-parse", "HEAD"); got != want {
			t.Errorf("task-%d worktree head = %s, want the fetched head %s", i+1, got, want)
		}
		raw, err := os.ReadFile(filepath.Join(worktrees[i], tcCache02App))
		if err != nil || string(raw) != "second\n" {
			t.Errorf("task-%d checked out %s = %q (%v), want the fetched content", i+1, tcCache02App, raw, err)
		}
		if dirty := tcCache02MustGit(t, "-C", worktrees[i], "status", "--porcelain"); dirty != "" {
			t.Errorf("task-%d worktree is not clean after the burst:\n%s", i+1, dirty)
		}
	}

	// The cache is uncorrupted and was reused: fsck clean, the fetched ref
	// survives, and a checkout after the burst still takes the reuse path.
	out, err := tcCache02Git("-C", bare, "fsck", "--no-progress")
	if err != nil {
		t.Errorf("git fsck on the shared cache: %v\n%s", err, out)
	}
	for _, line := range strings.Split(out, "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "error") || strings.Contains(lower, "fatal") ||
			strings.Contains(lower, "corrupt") || strings.Contains(lower, "bad object") {
			t.Errorf("git fsck reports a damaged cache: %s", line)
		}
	}
	if got := tcCache02MustGit(t, "-C", bare, "rev-parse", "refs/heads/main"); got != want {
		t.Errorf("shared cache ref = %s, want the fetched head %s", got, want)
	}
	after := filepath.Join(sandbox, "worktree-after")
	afterRun := tcCache02Checkout(source, bare, after)
	if afterRun.err != nil {
		t.Errorf("checkout after the burst: %v", afterRun.err)
	} else {
		if mode := afterRun.modes[0]; mode != tcCache02Reuse {
			t.Errorf("the post-burst checkout took the %s path; the cache must still be reused", mode)
		}
		if got := tcCache02MustGit(t, "-C", after, "rev-parse", "HEAD"); got != want {
			t.Errorf("post-burst worktree head = %s, want %s", got, want)
		}
	}

	// Both live tasks reach their terminal report: the task-success face of
	// the case.
	for _, id := range tasks {
		tcCacheRunTask(t, h, id, "")
	}
	if forwards := h.server.forwarded(scheduler.EPComplete); len(forwards) != 2 {
		t.Fatalf("terminal forwards = %d, want 2 (one per task)", len(forwards))
	}
	terminal := h.events("task.terminal")
	if len(terminal) != 2 {
		t.Fatalf("task.terminal events = %d, want 2", len(terminal))
	}
	for _, event := range terminal {
		if got := tcEventString(event, "result"); got != "completed" {
			t.Errorf("terminal result = %q, want completed", got)
		}
	}
	for _, id := range tasks {
		settled, ok := h.reg.Settled(id)
		if !ok || settled.State != "terminal" || settled.Result != "completed" {
			t.Errorf("settled %s = %s/%s (present %v), want terminal/completed", id, settled.State, settled.Result, ok)
		}
		if !h.jobs.jobDeleted("fm-" + id) {
			t.Errorf("the Job for %s must be deleted after its terminal report", id)
		}
	}
}
