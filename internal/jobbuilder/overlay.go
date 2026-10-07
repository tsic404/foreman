package jobbuilder

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"

	batchv1 "k8s.io/api/batch/v1"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

// OverlayPath is the fixed overlay carrier location (03-contracts.md §5.4):
// the Deployment mounts the optional ConfigMap foreman-job-template here with
// key job-overlay.yaml. Enablement is file existence only — there is no env
// switch, so a setting cannot point at a file the Deployment never mounted.
const OverlayPath = "/etc/foreman/job-template/job-overlay.yaml"

// strictJobSerializer rejects unknown fields instead of dropping them
// ("contianers" must fail loudly, §5.4 阶段 1).
var strictJobSerializer = jsonserializer.NewSerializerWithOptions(
	jsonserializer.DefaultMetaFactory, scheme.Scheme, scheme.Scheme,
	jsonserializer.SerializerOptions{Yaml: false, Strict: true},
)

var unknownFieldPattern = regexp.MustCompile(`unknown field "([^"]+)"`)

// LoadAndValidateOverlay is the startup gate (called by NewBuilder): it reads
// the optional overlay and runs both validation stages — stage 1 on the raw
// map, stage 2 on the merged, normalized default template — returning the raw
// overlay for Build to merge. A missing file means present=false: the default
// template path (AC-20 regression), never an error.
func LoadAndValidateOverlay(cfg Config) (map[string]any, bool, error) {
	path := cfg.OverlayPath
	if path == "" {
		path = OverlayPath
	}
	log := cfg.logger()

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Warn("job overlay absent: falling back to the built-in default template",
			"overlay", path,
			"effect", "a ConfigMap change takes effect only after a Foreman restart")
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read job overlay %s: %w", path, err)
	}

	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	log.Info("job overlay found: validating before serving",
		"overlay", path, "overlay_sha256", hash,
		"effect", "loaded at startup only; a ConfigMap change takes effect after a restart")

	raw, violations, decoded := decodeOverlay(path, data)
	if decoded {
		violations = append(violations, PremergeValidator{}.CheckOverlayMap(raw)...)
	}
	// Stage 2 on the merged object: render the default template with no
	// per-task values, merge, normalize and re-check (job-template.md
	// §关键流程 step 0).
	if decoded && len(violations) == 0 {
		probe := &Builder{cfg: cfg, overlay: raw, overlayPresent: true}
		if _, renderErr := probe.render(dryRunJobName, dryRunEntry()); renderErr != nil {
			violations = append(violations, violationsOf(renderErr)...)
		}
	}
	if len(violations) > 0 {
		log.Error("job overlay rejected: refusing to start", "overlay", path,
			"overlay_sha256", hash, "violations", len(violations))
		return nil, true, &ValidationError{Source: "job overlay " + path, Violations: violations}
	}

	log.Info("job overlay accepted", "overlay", path, "overlay_sha256", hash)
	return raw, true, nil
}

// decodeOverlay turns the overlay file into the raw map stage 1 validates.
// It also enforces the two structural rules the raw map can decide: the
// declared apiVersion/kind must be the Job's, and unknown fields are rejected
// (stage 1 「未知字段即拒」). ok is false when the overlay cannot be treated
// as a Job map at all, in which case the caller skips the later stages.
func decodeOverlay(path string, data []byte) (raw map[string]any, violations []Violation, ok bool) {
	jsonBytes, err := utilyaml.ToJSON(data)
	if err != nil {
		return nil, []Violation{{Path: path, Rule: fmt.Sprintf("overlay not parseable as YAML: %v", err)}}, false
	}
	var overlay map[string]any
	if err := json.Unmarshal(jsonBytes, &overlay); err != nil {
		return nil, []Violation{{Path: path, Rule: fmt.Sprintf("overlay must be a Job manifest object: %v", err)}}, false
	}
	if overlay == nil {
		// An empty document (only comments) is not a Job object; refusing it
		// catches a ConfigMap key created empty by mistake.
		return nil, []Violation{{Path: path, Rule: "overlay must be a Job manifest object, got an empty document"}}, false
	}
	if apiVersion, declared := overlay["apiVersion"]; declared && apiVersion != "batch/v1" {
		violations = append(violations, Violation{Path: "apiVersion", Rule: fmt.Sprintf("A: overlay must be a batch/v1 Job, got %v", apiVersion)})
	}
	if kind, declared := overlay["kind"]; declared && kind != "Job" {
		violations = append(violations, Violation{Path: "kind", Rule: fmt.Sprintf("A: overlay must be a Job, got %v", kind)})
	}
	if _, _, err := strictJobSerializer.Decode(jsonBytes, nil, &batchv1.Job{}); err != nil {
		violations = append(violations, strictDecodeViolations(err)...)
	}
	if len(violations) > 0 {
		return overlay, violations, false
	}
	return overlay, nil, true
}

// strictDecodeViolations renders each strict-decoding complaint as its own
// Violation so the operator log keeps the offending field path.
func strictDecodeViolations(err error) []Violation {
	msg := err.Error()
	matches := unknownFieldPattern.FindAllStringSubmatch(msg, -1)
	if len(matches) == 0 {
		return []Violation{{Path: OverlayPath, Rule: "stage 1 strict decode failed: " + msg}}
	}
	out := make([]Violation, 0, len(matches))
	for _, m := range matches {
		out = append(out, Violation{Path: m[1], Rule: "stage 1: unknown field rejected (strict decode)"})
	}
	return out
}

// violationsOf unwraps the violations a render step reported; anything else
// becomes a single violation carrying the error text.
func violationsOf(err error) []Violation {
	var invalid *ValidationError
	if errors.As(err, &invalid) {
		return invalid.Violations
	}
	return []Violation{{Path: OverlayPath, Rule: err.Error()}}
}
