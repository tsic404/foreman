package injectproxy

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Record-face constants (§4.3).
const (
	recordsFile = "records.jsonl"
	bodiesDir   = "bodies"
	wsDir       = "ws"
	blobExt     = ".bin"

	kindHTTP    = "http"
	kindWSChunk = "ws-chunk"

	// Directions: F→S = Foreman's own requests (mdt_/mul_ credential),
	// A→S = transparent requests under any other credential (mat_, probe
	// credentials, no credential at all — the record face classifies callers
	// by credential, it cannot see who they are), S→F = the server's
	// responses and its WS downlink bytes.
	directionForemanToServer = "F→S"
	directionAgentToServer   = "A→S"
	directionServerToForeman = "S→F"
)

// Record-line ceilings. One row is written and read back under the same bound:
// a line the reader cannot scan would take the whole record face with it
// (GET /record answers 500, a restart fails), and JSON escaping expands a
// value up to six bytes per input byte, so the caps are on raw bytes with an
// order of magnitude of headroom.
const (
	// maxRecordLineBytes bounds one encoded row.
	maxRecordLineBytes = 1 << 20
	// maxRecordedValueBytes bounds one recorded header value.
	maxRecordedValueBytes = 1 << 10
	// maxRecordedPathBytes bounds the recorded request path.
	maxRecordedPathBytes = 2 << 10
	// maxRecordedMethodBytes bounds the recorded request method.
	maxRecordedMethodBytes = 32
	// truncationMark ends a value that was cut to fit one of the caps above.
	truncationMark = "…"
)

// record is one §4.3 line. Field order matches the contract's example row;
// method is omitted on S→F rows (a response body row keeps the request's
// path, not its method), and status is omitted only where no HTTP status
// exists (a WS upgrade refused by ws-drop).
type record struct {
	TS                  string            `json:"ts"`
	Direction           string            `json:"direction"`
	Kind                string            `json:"kind"`
	Method              string            `json:"method,omitempty"`
	Path                string            `json:"path,omitempty"`
	AuthorizationPrefix string            `json:"authorization_prefix,omitempty"`
	Headers             map[string]string `json:"headers,omitempty"`
	BodySHA256          string            `json:"body_sha256,omitempty"`
	BodyPath            string            `json:"body_path,omitempty"`
	BytesPath           string            `json:"bytes_path,omitempty"`
	Status              int               `json:"status,omitempty"`
	Injected            bool              `json:"injected"`
}

// requestRow builds the row of one request. body fields are filled in by the
// caller once the body is consumed (or absent).
func requestRow(ts time.Time, direction, method, path, prefix string, headers map[string]string) record {
	return record{
		TS:                  stamp(ts),
		Direction:           direction,
		Kind:                kindHTTP,
		Method:              method,
		Path:                path,
		AuthorizationPrefix: prefix,
		Headers:             headers,
	}
}

// responseRow builds the §4.3 response-body row: direction S→F, the request's
// path, no method, the response status.
func responseRow(ts time.Time, path string, status int, sha, rel string) record {
	return record{
		TS:         stamp(ts),
		Direction:  directionServerToForeman,
		Kind:       kindHTTP,
		Path:       path,
		BodySHA256: sha,
		BodyPath:   rel,
		Status:     status,
	}
}

// chunkRow builds one WS byte-segment row: raw bytes on disk, no frame
// parsing (§4.3).
func chunkRow(ts time.Time, direction, path, rel string) record {
	return record{
		TS:        stamp(ts),
		Direction: direction,
		Kind:      kindWSChunk,
		Path:      path,
		BytesPath: rel,
	}
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Recorder writes the record face: one JSONL line per row, one file per body
// or WS byte segment. Recording is best effort by design — a full disk must
// degrade the record face, never the transparent data path (§4.1: the relay's
// job is forwarding).
type Recorder struct {
	dir string
	log *slog.Logger

	mu      sync.Mutex
	file    *os.File
	bodySeq int
	wsSeq   int
}

// OpenRecorder opens (creating it if needed) the record directory and appends
// to an existing records.jsonl, resuming the body/WS sequence numbers so a
// restarted injector does not overwrite the previous run's bytes.
func OpenRecorder(dir string, log *slog.Logger) (*Recorder, error) {
	// Normalized once, here: OpenBlob compares a joined path against dir, and
	// the default record dir is relative ("./records").
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve record dir: %w", err)
	}
	for _, sub := range []string{bodiesDir, wsDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create record dir %s: %w", dir, err)
		}
	}
	file, err := os.OpenFile(filepath.Join(dir, recordsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open record file: %w", err)
	}
	rec := &Recorder{dir: dir, log: log, file: file}
	if err := rec.resumeSeqs(); err != nil {
		file.Close()
		return nil, err
	}
	return rec, nil
}

// Close releases the record file.
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.file.Close()
}

// Append writes one row. Errors are logged, never returned: the data path
// must not learn about record-face trouble.
func (r *Recorder) Append(row record) {
	line, err := json.Marshal(boundRow(row))
	if err != nil {
		r.log.Error("record marshal failed", "kind", row.Kind, "err", err)
		return
	}
	line = append(line, '\n')

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.file.Write(line); err != nil {
		r.log.Error("record write failed", "kind", row.Kind, "err", err)
	}
}

// boundRow caps the fields a request head can inflate. The caps are on raw
// bytes, JSON escaping expands at most sixfold, and every other field of a row
// is fixed-width, so the encoded line stays far inside maxRecordLineBytes. A
// cut value keeps its first bytes and ends with an ellipsis: the record face is
// evidence, and a truncated value still says which field carried the oversized
// payload.
func boundRow(row record) record {
	row.Method = truncateForRecord(row.Method, maxRecordedMethodBytes)
	row.Path = truncateForRecord(row.Path, maxRecordedPathBytes)
	if len(row.Headers) == 0 {
		return row
	}
	headers := make(map[string]string, len(row.Headers))
	for _, name := range slices.Sorted(maps.Keys(row.Headers)) {
		headers[name] = truncateForRecord(row.Headers[name], maxRecordedValueBytes)
	}
	row.Headers = headers
	return row
}

// truncateForRecord cuts s to at most max bytes, on a rune boundary.
func truncateForRecord(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncationMark
}

// NewBodySink reserves the next body file and returns the tee a body relay
// writes decoded bytes into.
func (r *Recorder) NewBodySink() *bodySink {
	r.mu.Lock()
	r.bodySeq++
	seq := r.bodySeq
	r.mu.Unlock()
	return r.openSink(bodiesDir, seq)
}

// NewChunkSink reserves the next WS byte-segment file.
func (r *Recorder) NewChunkSink() *bodySink {
	r.mu.Lock()
	r.wsSeq++
	seq := r.wsSeq
	r.mu.Unlock()
	return r.openSink(wsDir, seq)
}

func (r *Recorder) openSink(subdir string, seq int) *bodySink {
	sink := &bodySink{rec: r, hash: sha256.New()}
	for {
		rel := path.Join(subdir, fmt.Sprintf("%04d%s", seq, blobExt))
		file, err := os.OpenFile(filepath.Join(r.dir, filepath.FromSlash(rel)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			sink.rel = rel
			sink.file = file
			return sink
		}
		if !os.IsExist(err) {
			sink.failed = true
			r.log.Error("record body open failed", "path", rel, "err", err)
			return sink
		}
		seq++
	}
}

// Read returns the rows with ts strictly after since (zero time = all) in
// append order, at most limit rows (limit <= 0 = no cap).
func (r *Recorder) Read(since time.Time, limit int) ([]record, error) {
	file, err := os.Open(filepath.Join(r.dir, recordsFile))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var rows []record
	err = scanRows(file, func(line []byte) error {
		var row record
		if err := json.Unmarshal(line, &row); err != nil {
			// A torn last line (crash mid-write) is the only expected case.
			return nil
		}
		if !since.IsZero() {
			ts, err := time.Parse(time.RFC3339Nano, row.TS)
			if err != nil || !ts.After(since) {
				return nil
			}
		}
		if limit > 0 && len(rows) >= limit {
			return errScanDone
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// errScanDone ends a row scan early; it is not a failure.
var errScanDone = errors.New("scan done")

// scanRows walks the record file line by line. A line longer than
// maxRecordLineBytes is skipped rather than fatal: it could only come from a
// build that did not bound its rows, and one unreadable line must not take the
// whole record face — reads and restarts included — with it.
func scanRows(file *os.File, fn func(line []byte) error) error {
	r := bufio.NewReaderSize(file, 64<<10)
	for {
		line, err := readLine(r, maxRecordLineBytes)
		if errors.Is(err, errLineTooLong) {
			continue
		}
		if len(line) > 0 {
			if ferr := fn(line); ferr != nil {
				if errors.Is(ferr, errScanDone) {
					return nil
				}
				return ferr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// OpenBlob opens a body or WS byte file referenced by a row's body_path /
// bytes_path. The path must stay inside the record directory.
func (r *Recorder) OpenBlob(rel string) (*os.File, error) {
	if rel == "" {
		return nil, fmt.Errorf("path is required")
	}
	clean := path.Clean(rel)
	if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return nil, fmt.Errorf("path %q escapes the record directory", rel)
	}
	full := filepath.Join(r.dir, filepath.FromSlash(clean))
	if full != r.dir && !strings.HasPrefix(full, r.dir+string(filepath.Separator)) {
		return nil, fmt.Errorf("path %q escapes the record directory", rel)
	}
	info, err := os.Stat(full)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path %q is not a regular file", rel)
	}
	return os.Open(full)
}

// resumeSeqs scans an existing record file for the highest body and WS
// sequence in use.
func (r *Recorder) resumeSeqs() error {
	file, err := os.Open(filepath.Join(r.dir, recordsFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	return scanRows(file, func(line []byte) error {
		var row record
		if err := json.Unmarshal(line, &row); err != nil {
			return nil
		}
		r.bodySeq = max(r.bodySeq, seqOf(row.BodyPath))
		r.wsSeq = max(r.wsSeq, seqOf(row.BytesPath))
		return nil
	})
}

// seqOf extracts the sequence number from "bodies/0007.bin".
func seqOf(rel string) int {
	if rel == "" {
		return 0
	}
	name := strings.TrimSuffix(path.Base(rel), blobExt)
	seq, err := strconv.Atoi(name)
	if err != nil {
		return 0
	}
	return seq
}

// bodySink tees decoded body bytes to disk while hashing them. Writes never
// fail: on a disk error the sink keeps hashing so the row still carries a
// truthful sha256 of what passed through, and drops the file reference.
type bodySink struct {
	rec    *Recorder
	rel    string
	file   *os.File
	hash   hash.Hash
	size   int64
	failed bool
}

// Write records p and always reports success.
func (s *bodySink) Write(p []byte) (int, error) {
	if s == nil {
		return len(p), nil
	}
	s.hash.Write(p)
	s.size += int64(len(p))
	if s.file != nil {
		if _, err := s.file.Write(p); err != nil {
			s.failed = true
			s.rec.log.Error("record body write failed", "path", s.rel, "err", err)
			s.file.Close()
			s.file = nil
			s.remove()
		}
	}
	return len(p), nil
}

// Close finishes the sink and returns the sha256 of the body and its path.
// A body that carried no bytes leaves no file behind: the sink reserves its
// file when the exchange starts, so every bodyless request would otherwise
// litter the record directory with an empty file no row references.
func (s *bodySink) Close() (sha, rel string) {
	if s == nil {
		return "", ""
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.failed = true
			s.rec.log.Error("record body close failed", "path", s.rel, "err", err)
		}
		s.file = nil
	}
	sha = hex.EncodeToString(s.hash.Sum(nil))
	if s.failed || s.size == 0 {
		s.remove()
		return sha, ""
	}
	return sha, s.rel
}

// remove drops the sink's file and its reference.
func (s *bodySink) remove() {
	if s.rel == "" {
		return
	}
	rel := s.rel
	s.rel = ""
	if err := os.Remove(filepath.Join(s.rec.dir, filepath.FromSlash(rel))); err != nil && !os.IsNotExist(err) {
		s.rec.log.Error("record body remove failed", "path", rel, "err", err)
	}
}
