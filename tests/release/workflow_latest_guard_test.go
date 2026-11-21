// Package release pins the two release workflows' version semantics: both images
// ride movable tags with `imagePullPolicy: Always`, so a run must publish exactly
// the tag its trigger names. For build-foreman.yml that is the version tag on a
// `v*.*.*` tag push — never `latest`, so the tag keeps naming the image built
// from it (ADR-012's rollback path) — and `latest` on a main push — never a
// version tag, so the untagged line cannot mint or overwrite one. The scripts
// under test are the workflows' own `run:` blocks — with their `env:` expressions
// resolved against the preceding step's outputs, so the wiring is under test too
// — executed with stubbed docker/curl.
package release

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	foremanWorkflow    = "../../.github/workflows/build-foreman.yml"
	foremanJobWorkflow = "../../.github/workflows/build-foreman-job.yml"
)

// TestJobImageLatestOnlyFollowsUpstreamsNewestRelease covers
// build-foreman-job.yml: the version is upstream's newest release unless a
// dispatch names one, every run publishes the version tag (the rollback path),
// and only the run that publishes upstream's newest release moves
// `foreman-job:latest` beside it.
func TestJobImageLatestOnlyFollowsUpstreamsNewestRelease(t *testing.T) {
	requireTools(t, "bash", "git")
	top := workflowEnv(t, foremanJobWorkflow)
	steps := workflowSteps(t, foremanJobWorkflow)
	resolve := requireStep(t, steps, "Resolve the upstream daemon version", foremanJobWorkflow)
	publish := requireStep(t, steps, "Build and push the image", foremanJobWorkflow)

	for _, tc := range []struct {
		input      string
		version    string
		isNewest   string
		wantLatest bool
	}{
		{input: "", version: "0.6.3", isNewest: "true", wantLatest: true},
		{input: "0.6.3", version: "0.6.3", isNewest: "true", wantLatest: true},
		{input: "v0.6.3", version: "0.6.3", isNewest: "true", wantLatest: true},
		{input: "0.6.1", version: "0.6.1", isNewest: "false", wantLatest: false},
	} {
		t.Run("input="+tc.input, func(t *testing.T) {
			dir := t.TempDir()
			stub(t, dir, "curl", "#!/bin/sh\necho \"https://github.com/multica-ai/multica/releases/tag/v0.6.3\"\n")
			resolved := runStep(t, resolve.run, dir, dir, mergeEnv(top, resolveEnv(t, resolve.env, everyInput(tc.input), nil)))
			assertOutput(t, resolved, "version", tc.version)
			assertOutput(t, resolved, "is_newest", tc.isNewest)
			assertOutput(t, resolved, "latest", "0.6.3")

			// The publish step's own env decides whether `latest` moves: its
			// PUSH_LATEST must come from the resolve step's output, never from
			// the trigger alone.
			// Outputs of the step this case skips (the artifact resolution):
			// the publish step's own wiring is what is under test.
			env := mergeEnv(top, resolveEnv(t, publish.env, nil, map[string]string{
				"MULTICA_CLI_URL_AMD64":    "u",
				"MULTICA_CLI_SHA256_AMD64": "s",
				"MULTICA_CLI_URL_ARM64":    "u",
				"MULTICA_CLI_SHA256_ARM64": "s",
			}, resolved.outputs))
			buildArgs := runPublish(t, publish.run, dir, env)
			// The version tag is the rollback path (ADR-012), so every run
			// pushes it; only upstream's newest release moves `latest` beside it.
			wantTags := []string{env["REGISTRY"] + "/foreman-job:" + tc.version}
			if tc.wantLatest {
				wantTags = append(wantTags, env["REGISTRY"]+"/foreman-job:latest")
			}
			assertPublishTags(t, buildArgs, wantTags...)
		})
	}

	t.Run("junk input is refused", func(t *testing.T) {
		dir := t.TempDir()
		stub(t, dir, "curl", "#!/bin/sh\necho \"https://github.com/multica-ai/multica/releases/tag/v0.6.3\"\n")
		out := runStepAllowFailure(t, resolve.run, dir, dir, mergeEnv(top, resolveEnv(t, resolve.env, everyInput("garbage"), nil)))
		if out.err == nil {
			t.Fatalf("version resolution accepted a non-release input:\n%s", out.stdout)
		}
	})
}

// TestForemanImageTagPushPublishesOnlyThatVersion covers build-foreman.yml: a
// `v*.*.*` tag push publishes exactly `foreman:v<X.Y.Z>` — never `latest`,
// which would point the version tag's rollback path at main content — and a ref
// that is not a v<major>.<minor>.<patch> tag fails before any build.
func TestForemanImageTagPushPublishesOnlyThatVersion(t *testing.T) {
	requireTools(t, "bash", "git")
	top, resolve, publish := foremanReleaseSteps(t)
	repo := t.TempDir()

	resolved := runStep(t, resolve.run, repo, t.TempDir(), mergeEnv(top, map[string]string{
		"GITHUB_REF_TYPE": "tag",
		"GITHUB_REF_NAME": "v0.2.0",
	}))
	assertOutput(t, resolved, "version", "v0.2.0")

	args, env := runForemanPublish(t, top, publish, resolved, repo)
	assertPublishTags(t, args, env["REGISTRY"]+"/foreman:v0.2.0")

	t.Run("a malformed tag fails before the build", func(t *testing.T) {
		out := runStepAllowFailure(t, resolve.run, repo, t.TempDir(), mergeEnv(top, map[string]string{
			"GITHUB_REF_TYPE": "tag",
			"GITHUB_REF_NAME": "v1.2",
		}))
		if out.err == nil {
			t.Fatalf("version resolution accepted v1.2:\n%s", out.stdout)
		}
	})
}

// TestForemanImageMainPushPublishesOnlyLatest covers build-foreman.yml: a `main`
// push (and a manual dispatch, its re-run entry point) publishes exactly
// `foreman:latest` whether or not the repository carries version tags — the
// repository's own tags never become the version.
func TestForemanImageMainPushPublishesOnlyLatest(t *testing.T) {
	requireTools(t, "bash", "git")
	top, resolve, publish := foremanReleaseSteps(t)

	for _, tc := range []struct {
		label string
		repo  func(*testing.T) string
	}{
		{label: "repository carries published tags", repo: taggedRepo},
		{label: "repository carries no tag", repo: scratchRepo},
	} {
		t.Run(tc.label, func(t *testing.T) {
			repo := tc.repo(t)
			resolved := runStep(t, resolve.run, repo, t.TempDir(), mergeEnv(top, map[string]string{
				"GITHUB_REF_TYPE": "branch",
				"GITHUB_REF_NAME": "main",
			}))
			assertOutput(t, resolved, "version", "latest")

			args, env := runForemanPublish(t, top, publish, resolved, repo)
			assertPublishTags(t, args, env["REGISTRY"]+"/foreman:latest")
		})
	}
}

// foremanReleaseSteps extracts the two build-foreman.yml steps whose scripts
// decide what a run publishes, plus the workflow-level env they inherit.
func foremanReleaseSteps(t *testing.T) (top map[string]string, resolve, publish step) {
	t.Helper()
	top = workflowEnv(t, foremanWorkflow)
	steps := workflowSteps(t, foremanWorkflow)
	return top, requireStep(t, steps, "Resolve the version", foremanWorkflow),
		requireStep(t, steps, "Build and push the image", foremanWorkflow)
}

// runForemanPublish resolves the publish step against the resolve step's outputs
// and runs it, returning the recorded buildx arguments and the resolved env the
// reference is built from.
func runForemanPublish(t *testing.T, top map[string]string, publish step, resolved stepRun, cwd string) (string, map[string]string) {
	t.Helper()
	env := mergeEnv(top, resolveEnv(t, publish.env, nil, nil, resolved.outputs))
	return runPublish(t, publish.run, cwd, env), env
}

// assertPublishTags requires the recorded buildx invocation to carry exactly the
// tags the trigger names, compared as a set: `--tag` order is not part of the
// contract. For a job publish that is the version tag plus `latest` only when
// this run publishes upstream's newest release.
func assertPublishTags(t *testing.T, args string, want ...string) {
	t.Helper()
	var got []string
	for _, match := range buildxTag.FindAllStringSubmatch(args, -1) {
		got = append(got, match[1])
	}
	// Sorted copies: a reordered argv is equivalent, a duplicated tag is not —
	// it still shows as a length or element mismatch.
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("buildx published %v, want %v\n%s", got, want, args)
	}
}

// everyInput answers every `inputs.*` reference in the job workflow with the
// dispatch input under test (that workflow has exactly one).
func everyInput(value string) map[string]string {
	return map[string]string{
		"inputs.version":  value,
		"inputs.force":    "true",
		"github.ref_type": "branch",
		"github.ref_name": "main",
	}
}

type step struct {
	run    string
	env    map[string]string // raw values, still carrying their ${{ … }} expressions
	ifExpr string            // the step's own `if:`, as written
}

// resolveEnv evaluates a step's `env:` the way Actions would, from the values
// the case provides and the outputs the preceding steps produced. An
// expression it cannot resolve fails the case: the guard has to come from a
// step output, not from the trigger.
func resolveEnv(t *testing.T, env, provided, extra map[string]string, outputs ...map[string]string) map[string]string {
	t.Helper()
	merge := func(maps ...map[string]string) map[string]string {
		out := map[string]string{}
		for _, m := range maps {
			for k, v := range m {
				out[k] = v
			}
		}
		return out
	}
	// Step outputs: the ones the case ran win over the ones it fabricates for
	// steps it skips.
	outputs = append(outputs, extra)
	got := merge(outputs...)
	all := merge(provided, extra)
	resolved := map[string]string{}
	for key, raw := range env {
		resolved[key] = resolveValue(t, key, raw, all, got)
	}
	return resolved
}

var (
	outputExpr  = regexp.MustCompile(`^steps\.[A-Za-z0-9_-]+\.outputs\.([A-Za-z0-9_-]+)$`)
	inputExpr   = regexp.MustCompile(`^inputs\.[A-Za-z0-9_-]+$`)
	ternaryExpr = regexp.MustCompile(`^inputs\.[A-Za-z0-9_-]+ && '(.*)' \|\| '(.*)'$`)
	varsExpr    = regexp.MustCompile(`^vars\.[A-Za-z0-9_-]+ \|\| '(.*)'$`)
	contextExpr = regexp.MustCompile(`^(github|env|secrets|needs|matrix|runner)\.`)
	buildxTag   = regexp.MustCompile(`--tag (\S+)`)
)

func resolveValue(t *testing.T, key, raw string, provided, outputs map[string]string) string {
	t.Helper()
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, `"`)
	if !strings.HasPrefix(value, "${{") {
		return value
	}
	expr := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "${{"), "}}"))
	switch {
	case outputExpr.MatchString(expr):
		name := outputExpr.FindStringSubmatch(expr)[1]
		out, ok := outputs[name]
		if !ok {
			t.Fatalf("%s = %s: the step this reads produced no output %s", key, raw, name)
		}
		return out
	case inputExpr.MatchString(expr):
		out, ok := provided[expr]
		if !ok {
			t.Fatalf("%s = %s: the case provides no value for %s", key, raw, expr)
		}
		return out
	case ternaryExpr.MatchString(expr):
		// `inputs.<name> && '<a>' || '<b>'` — Actions' inline if: a boolean input
		// is true only when the dispatch set it.
		name := strings.SplitN(expr, " ", 2)[0]
		if truthy(provided[name]) {
			return ternaryExpr.FindStringSubmatch(expr)[1]
		}
		return ternaryExpr.FindStringSubmatch(expr)[2]
	case varsExpr.MatchString(expr):
		return varsExpr.FindStringSubmatch(expr)[1]
	case contextExpr.MatchString(expr):
		out, ok := provided[expr]
		if !ok {
			t.Fatalf("%s = %s: a trigger-only expression — the guard must be a step output", key, raw)
		}
		return out
	}
	t.Fatalf("%s = %s: unsupported expression", key, raw)
	return ""
}

type stepRun struct {
	stdout  string
	stderr  string
	outputs map[string]string
	err     error
}

// runStep executes one extracted `run:` block the way the workflow does
// (`bash -e`), with the variables Actions always provides and PATH prefixed by
// the stub directory, so docker/curl are the test's.
func runStep(t *testing.T, script, cwd, stubDir string, env map[string]string) stepRun {
	t.Helper()
	res := runStepAllowFailure(t, script, cwd, stubDir, env)
	if res.err != nil {
		t.Fatalf("step failed: %v\n--- stdout ---\n%s\n--- stderr ---\n%s", res.err, res.stdout, res.stderr)
	}
	return res
}

func runStepAllowFailure(t *testing.T, script, cwd, stubDir string, env map[string]string) stepRun {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "step.sh")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatalf("write step script: %v", err)
	}
	outFile, sumFile := filepath.Join(dir, "out"), filepath.Join(dir, "summary")
	// The workflow-level env and the variables Actions sets on every step
	// (empty inputs included): the scripts run under `set -u`.
	base := map[string]string{
		"GITHUB_OUTPUT":       outFile,
		"GITHUB_STEP_SUMMARY": sumFile,
		"GITHUB_SHA":          "deadbeef",
		"GITHUB_REF_TYPE":     "branch",
		"GITHUB_REF_NAME":     "main",
	}
	cmd := exec.Command("bash", "-e", path)
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for k, v := range base {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	outputs := map[string]string{}
	if body, readErr := os.ReadFile(outFile); readErr == nil {
		for _, line := range strings.Split(string(body), "\n") {
			if name, value, ok := strings.Cut(line, "="); ok {
				outputs[name] = value
			}
		}
	}
	return stepRun{stdout: stdout.String(), stderr: stderr.String(), outputs: outputs, err: err}
}

// runPublish runs a publish step with a stubbed docker that records the buildx
// arguments instead of building anything, and returns them.
func runPublish(t *testing.T, script, cwd string, env map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "docker.log")
	// The publish step only builds and pushes now: nothing reads the registry
	// back, so the stub only records the buildx arguments.
	stub(t, dir, "docker", `#!/bin/sh
if [ "$1 $2" = "buildx build" ]; then
  shift 2
  echo "BUILD $*" >> "$STUB_DOCKER_LOG"
fi
exit 0
`)
	copied := map[string]string{"STUB_DOCKER_LOG": log}
	for k, v := range env {
		copied[k] = v
	}
	runStep(t, script, cwd, dir, copied)
	body, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("the publish step never called buildx build: %v", err)
	}
	return string(body)
}

// scratchRepo is a scratch repository with two commits and no tag — the state
// the scheduler line is in before its first release.
func scratchRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q")
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		gitIn(t, dir, "add", name)
		gitIn(t, dir, "commit", "-q", "-m", name)
	}
	return dir
}

// taggedRepo is scratchRepo carrying two published version tags (v0.1.0 older,
// v0.9.0 newest): a main push must publish `latest` from it, never one of them.
func taggedRepo(t *testing.T) string {
	t.Helper()
	dir := scratchRepo(t)
	gitIn(t, dir, "tag", "v0.1.0", "HEAD~1")
	gitIn(t, dir, "tag", "v0.9.0", "HEAD")
	return dir
}

func gitIn(t *testing.T, dir string, args ...string) {
	t.Helper()
	argv := append([]string{"-c", "user.name=release-test", "-c", "user.email=release-test@example.invalid"}, args...)
	cmd := exec.Command("git", argv...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func stub(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
}

func assertOutput(t *testing.T, run stepRun, key, want string) {
	t.Helper()
	got, ok := run.outputs[key]
	if !ok {
		t.Fatalf("GITHUB_OUTPUT carries no %s (stderr: %s)", key, run.stderr)
	}
	if got != want {
		t.Errorf("GITHUB_OUTPUT %s = %q, want %q", key, got, want)
	}
}

// mergeEnv layers maps left to right (the rightmost wins), which is how
// Actions composes the workflow-level env, the step's env and the built-ins.
func mergeEnv(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// workflowEnv reads the workflow's top-level `env:` block — the variables the
// steps inherit.
func workflowEnv(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	env := map[string]string{}
	for cursor := 0; cursor < len(lines); cursor++ {
		if lines[cursor] != "env:" {
			continue
		}
		for cursor++; cursor < len(lines); cursor++ {
			line := lines[cursor]
			if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if indentOf(line) == 0 {
				break
			}
			if key, value, ok := strings.Cut(strings.TrimSpace(line), ": "); ok {
				env[key] = strings.Trim(value, `"`)
			}
		}
		break
	}
	if len(env) == 0 {
		t.Fatalf("%s carries no workflow-level env", path)
	}
	return env
}

// truthy follows Actions' coercion: a boolean input arrives as true/false, an
// empty string is false, and anything else non-empty is true.
func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "false", "0":
		return false
	}
	return true
}

func requireStep(t *testing.T, steps map[string]step, name, path string) step {
	t.Helper()
	got, ok := steps[name]
	if !ok {
		t.Fatalf("%s carries no %q step", path, name)
	}
	return got
}

func requireTools(t *testing.T, tools ...string) {
	t.Helper()
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("host has no %s", tool)
		}
	}
}

// workflowSteps extracts every step's `env:` mapping and `run: |` block, the
// block dedented to its own indentation — the shell the workflow itself runs.
func workflowSteps(t *testing.T, path string) map[string]step {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	steps := map[string]step{}
	for cursor := 0; cursor < len(lines); {
		name, indent, ok := stepName(lines[cursor])
		if !ok {
			cursor++
			continue
		}
		env, ifExpr, body, next, found := stepBlocks(lines, cursor+1, indent)
		if found {
			steps[name] = step{run: body, env: env, ifExpr: ifExpr}
		}
		cursor = next
	}
	if len(steps) == 0 {
		t.Fatalf("%s: no step carried a run block", path)
	}
	return steps
}

// stepBlocks collects the `env:` mapping and the `run: |` block of the step
// that starts at `from`, and returns the index the scan resumes from.
func stepBlocks(lines []string, from, stepIndent int) (env map[string]string, ifExpr, run string, next int, found bool) {
	env = map[string]string{}
	for cursor := from; cursor < len(lines); {
		line := lines[cursor]
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "if: ") && indentOf(line) == stepIndent+2:
			ifExpr = strings.TrimSpace(strings.TrimPrefix(trimmed, "if: "))
			cursor++
		case trimmed == "env:":
			envIndent := indentOf(line)
			cursor++
			for cursor < len(lines) {
				inner := lines[cursor]
				if strings.TrimSpace(inner) == "" || strings.HasPrefix(strings.TrimSpace(inner), "#") {
					cursor++
					continue
				}
				if indentOf(inner) <= envIndent {
					break
				}
				if key, value, ok := strings.Cut(strings.TrimSpace(inner), ": "); ok {
					env[key] = value
				}
				cursor++
			}
		case trimmed == "run: |":
			runIndent := indentOf(line)
			cursor++
			var body []string
			for cursor < len(lines) {
				inner := lines[cursor]
				if strings.TrimSpace(inner) == "" {
					body = append(body, "")
					cursor++
					continue
				}
				if indentOf(inner) <= runIndent {
					break
				}
				body = append(body, strings.TrimPrefix(inner, strings.Repeat(" ", runIndent+2)))
				cursor++
			}
			return env, ifExpr, strings.TrimRight(strings.Join(body, "\n"), "\n") + "\n", cursor, true
		case trimmed != "" && indentOf(line) <= stepIndent:
			return nil, "", "", cursor, false
		default:
			cursor++
		}
	}
	return nil, "", "", len(lines), false
}

func stepName(line string) (name string, indent int, ok bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "- name: ") || indentOf(line) > 8 {
		return "", 0, false
	}
	return strings.TrimPrefix(trimmed, "- name: "), indentOf(line), true
}

func indentOf(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}
