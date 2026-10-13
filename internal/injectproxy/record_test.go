package injectproxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testRecorder(t *testing.T) (*Recorder, string) {
	t.Helper()
	dir := t.TempDir()
	rec, err := OpenRecorder(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenRecorder: %v", err)
	}
	t.Cleanup(func() { rec.Close() })
	return rec, dir
}

// readRecordFile reads a recorded blob (body_path / bytes_path).
func readRecordFile(dir, rel string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
	return string(data), err
}

func TestRecorderRowFields(t *testing.T) {
	rec, dir := testRecorder(t)
	rec.Append(record{
		TS:                  stamp(time.Now()),
		Direction:           directionForemanToServer,
		Kind:                kindHTTP,
		Method:              "POST",
		Path:                "/api/daemon/tasks/t-1/complete",
		AuthorizationPrefix: "mdt_",
		Headers:             map[string]string{"X-Client-Capabilities": "skill-bundles-v1"},
		BodySHA256:          "abc",
		BodyPath:            "bodies/0007.bin",
		Status:              200,
	})

	raw, err := os.ReadFile(filepath.Join(dir, recordsFile))
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatalf("row is not JSON: %v (%s)", err, raw)
	}
	for _, key := range []string{"ts", "direction", "kind", "method", "path", "authorization_prefix", "headers", "body_sha256", "body_path", "status", "injected"} {
		if _, ok := row[key]; !ok {
			t.Fatalf("row is missing %q: %v", key, row)
		}
	}
	if row["injected"] != false {
		t.Fatalf("injected = %v, want false", row["injected"])
	}
}

func TestRecorderOmitsAbsentFields(t *testing.T) {
	rec, dir := testRecorder(t)
	// An injected WS refusal has no HTTP status and no body file; a response
	// body row has no method (§4.3).
	rec.Append(record{TS: stamp(time.Now()), Direction: directionAgentToServer, Kind: kindHTTP, Method: "GET", Path: "/api/daemon/ws", Injected: true})
	rec.Append(responseRow(time.Now(), "/api/daemon/tasks/claim", 200, "sha", "bodies/0001.bin"))

	raw, err := os.ReadFile(filepath.Join(dir, recordsFile))
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 rows, got %d: %s", len(lines), raw)
	}
	var refusal, response map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &refusal); err != nil {
		t.Fatalf("refusal row: %v", err)
	}
	if _, ok := refusal["status"]; ok {
		t.Fatalf("refused upgrade row carries a status: %v", refusal)
	}
	if refusal["injected"] != true {
		t.Fatalf("refused upgrade row injected = %v, want true", refusal["injected"])
	}
	if err := json.Unmarshal([]byte(lines[1]), &response); err != nil {
		t.Fatalf("response row: %v", err)
	}
	if _, ok := response["method"]; ok {
		t.Fatalf("response body row carries a method: %v", response)
	}
	if response["path"] != "/api/daemon/tasks/claim" || response["direction"] != directionServerToForeman {
		t.Fatalf("response row = %v", response)
	}
}

func TestRecorderReadSinceAndLimit(t *testing.T) {
	rec, _ := testRecorder(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	rec.Append(record{TS: stamp(base), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/first"})
	rec.Append(record{TS: stamp(base.Add(time.Second)), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/second"})
	rec.Append(record{TS: stamp(base.Add(2 * time.Second)), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/third"})

	all, err := rec.Read(time.Time{}, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("rows = %d, want 3", len(all))
	}

	// since is exclusive: the row at the boundary is not repeated.
	after, err := rec.Read(base.Add(time.Second), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(after) != 1 || after[0].Path != "/third" {
		t.Fatalf("rows after since = %+v, want only /third", after)
	}

	limited, err := rec.Read(time.Time{}, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(limited) != 2 || limited[1].Path != "/second" {
		t.Fatalf("limited rows = %+v", limited)
	}
}

func TestRecorderSkipsTornLine(t *testing.T) {
	rec, dir := testRecorder(t)
	rec.Append(record{TS: stamp(time.Now()), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/ok"})
	file, err := os.OpenFile(filepath.Join(dir, recordsFile), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := file.WriteString(`{"ts":"2026-10-13T00:00:00Z","direct`); err != nil {
		t.Fatalf("write: %v", err)
	}
	file.Close()

	rows, err := rec.Read(time.Time{}, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != "/ok" {
		t.Fatalf("rows = %+v, want only the complete one", rows)
	}
}

// TestRecorderBoundsOversizedRows is the poisoned-face case: one oversized
// header value must not produce a line the record face cannot read back.
func TestRecorderBoundsOversizedRows(t *testing.T) {
	rec, dir := testRecorder(t)
	rec.Append(record{
		TS:        stamp(time.Now()),
		Direction: directionAgentToServer,
		Kind:      kindHTTP,
		Method:    strings.Repeat("M", 4*maxRecordedMethodBytes),
		Path:      "/api/daemon/tasks/claim",
		Headers:   map[string]string{"X-Client-Capabilities": strings.Repeat("<", 200<<10)},
	})

	raw, err := os.ReadFile(filepath.Join(dir, recordsFile))
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if len(line) > maxRecordLineBytes {
			t.Fatalf("row is %d bytes, want at most %d", len(line), maxRecordLineBytes)
		}
	}

	rows, err := rec.Read(time.Time{}, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want the bounded row", rows)
	}
	value := rows[0].Headers["X-Client-Capabilities"]
	if len(value) > maxRecordedValueBytes+len(truncationMark) || !strings.HasSuffix(value, truncationMark) {
		t.Fatalf("recorded value = %d bytes, want a marked truncation", len(value))
	}
	if len(rows[0].Method) > maxRecordedMethodBytes+len(truncationMark) {
		t.Fatalf("recorded method = %q", rows[0].Method)
	}
	if rows[0].Path != "/api/daemon/tasks/claim" {
		t.Fatalf("recorded path = %q, want it untouched", rows[0].Path)
	}
}

// TestRecorderSkipsAnOversizedLine covers a file an older build could have
// written: a line no reader can scan must be skipped, not fatal, or the whole
// record face (and every restart) goes down with it.
func TestRecorderSkipsAnOversizedLine(t *testing.T) {
	rec, dir := testRecorder(t)
	rec.Append(record{TS: stamp(time.Now()), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/ok"})
	file, err := os.OpenFile(filepath.Join(dir, recordsFile), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	oversized := `{"ts":"2026-10-13T00:00:00Z","kind":"http","path":"` + strings.Repeat("x", maxRecordLineBytes) + `"}` + "\n"
	if _, err := file.WriteString(oversized); err != nil {
		t.Fatalf("write: %v", err)
	}
	file.Close()
	rec.Append(record{TS: stamp(time.Now().Add(time.Second)), Direction: directionAgentToServer, Kind: kindHTTP, Path: "/after"})

	rows, err := rec.Read(time.Time{}, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(rows) != 2 || rows[0].Path != "/ok" || rows[1].Path != "/after" {
		t.Fatalf("rows = %+v, want the rows on both sides of the oversized line", rows)
	}

	// The restart path scans the same file.
	reopened, err := OpenRecorder(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopened.Close()
}

func TestRecorderBodySink(t *testing.T) {
	rec, dir := testRecorder(t)
	payload := []byte(`{"probe":"transparency"}`)
	sink := rec.NewBodySink()
	if _, err := sink.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sha, rel := sink.Close()

	want := sha256.Sum256(payload)
	if sha != hex.EncodeToString(want[:]) {
		t.Fatalf("sha = %s, want %s", sha, hex.EncodeToString(want[:]))
	}
	if rel != "bodies/0001.bin" {
		t.Fatalf("path = %q, want bodies/0001.bin", rel)
	}
	stored, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(stored) != string(payload) {
		t.Fatalf("stored body = %q, want %q", stored, payload)
	}

	// An empty body hashes to sha256("") and leaves no file behind — not even
	// an unreferenced empty one.
	empty := rec.NewBodySink()
	sha, rel = empty.Close()
	emptySum := sha256.Sum256(nil)
	if sha != hex.EncodeToString(emptySum[:]) || rel != "" {
		t.Fatalf("empty body = %q/%q, want sha256(\"\") and no file", sha, rel)
	}
	entries, err := os.ReadDir(filepath.Join(dir, bodiesDir))
	if err != nil {
		t.Fatalf("read bodies dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "0001.bin" {
		t.Fatalf("bodies dir holds %d entries, want only the referenced body", len(entries))
	}
}

// TestRecorderOpenBlobUnderARelativeDir is the default deployment shape:
// INJECT_RECORD_DIR=./records must serve /record/blob like any absolute path.
func TestRecorderOpenBlobUnderARelativeDir(t *testing.T) {
	t.Chdir(t.TempDir())
	rec, err := OpenRecorder(DefaultRecordDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenRecorder(%q): %v", DefaultRecordDir, err)
	}
	defer rec.Close()

	sink := rec.NewBodySink()
	sink.Write([]byte("body"))
	_, rel := sink.Close()

	file, err := rec.OpenBlob(rel)
	if err != nil {
		t.Fatalf("OpenBlob(%q): %v", rel, err)
	}
	defer file.Close()
	stored, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if string(stored) != "body" {
		t.Fatalf("blob = %q, want %q", stored, "body")
	}
}

func TestRecorderResumesSequence(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := OpenRecorder(dir, log)
	if err != nil {
		t.Fatalf("OpenRecorder: %v", err)
	}
	sink := first.NewBodySink()
	sink.Write([]byte("one"))
	_, rel := sink.Close()
	chunk := first.NewChunkSink()
	chunk.Write([]byte("two"))
	_, chunkRel := chunk.Close()
	first.Close()
	if rel != "bodies/0001.bin" || chunkRel != "ws/0001.bin" {
		t.Fatalf("first run paths = %q/%q", rel, chunkRel)
	}

	second, err := OpenRecorder(dir, log)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	sink = second.NewBodySink()
	sink.Write([]byte("three"))
	_, rel = sink.Close()
	if rel != "bodies/0002.bin" {
		t.Fatalf("second run body path = %q, want bodies/0002.bin", rel)
	}
}

func TestRecorderOpenBlobRejectsEscapes(t *testing.T) {
	rec, dir := testRecorder(t)
	sink := rec.NewBodySink()
	sink.Write([]byte("body"))
	_, rel := sink.Close()

	file, err := rec.OpenBlob(rel)
	if err != nil {
		t.Fatalf("OpenBlob(%q): %v", rel, err)
	}
	file.Close()

	for _, bad := range []string{"", "../records.jsonl", "/etc/passwd", "bodies/../../etc/passwd", "bodies/missing.bin", "."} {
		if _, err := rec.OpenBlob(bad); err == nil {
			t.Fatalf("OpenBlob(%q) succeeded, want an error", bad)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, recordsFile)); err != nil {
		t.Fatalf("record file: %v", err)
	}
}
