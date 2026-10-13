package injectproxy

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testControl(t *testing.T) (*httptest.Server, *Injector, *Recorder) {
	t.Helper()
	rec, _ := testRecorder(t)
	injector := NewInjector()
	control := NewControl(injector, rec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(control.Handler())
	t.Cleanup(server.Close)
	return server, injector, rec
}

func doRequest(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(data)
}

func TestControlInjectionRoundTrip(t *testing.T) {
	server, _, _ := testControl(t)

	status, body := doRequest(t, http.MethodGet, server.URL+"/inject/state", "")
	if status != http.StatusOK {
		t.Fatalf("state status = %d", status)
	}
	var state State
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatalf("state is not JSON: %v (%s)", err, body)
	}
	if state.Mode != string(ModeNone) || state.Until != nil {
		t.Fatalf("initial state = %+v", state)
	}

	status, body = doRequest(t, http.MethodPost, server.URL+"/inject",
		`{"mode":"terminal-5xx","path_regex":"^/api/daemon/tasks/[^/]+/(complete|fail|cancel-ack)$","status":503,"duration_s":300}`)
	if status != http.StatusOK {
		t.Fatalf("inject status = %d: %s", status, body)
	}
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatalf("reply is not JSON: %v (%s)", err, body)
	}
	if state.Mode != string(ModeTerminal5xx) || state.Status != 503 || state.Until == nil {
		t.Fatalf("set state = %+v", state)
	}
	if _, err := time.Parse(time.RFC3339Nano, *state.Until); err != nil {
		t.Fatalf("until %q: %v", *state.Until, err)
	}

	_, body = doRequest(t, http.MethodGet, server.URL+"/inject/state", "")
	if !strings.Contains(body, `"mode":"terminal-5xx"`) {
		t.Fatalf("state read back = %s", body)
	}

	status, body = doRequest(t, http.MethodPost, server.URL+"/inject/reset", "")
	if status != http.StatusOK || !strings.Contains(body, `"mode":"none"`) {
		t.Fatalf("reset = %d %s", status, body)
	}
}

func TestControlRejectsBadRequests(t *testing.T) {
	server, injector, _ := testControl(t)
	for name, body := range map[string]string{
		"not json":      `{`,
		"unknown field": `{"mode":"ws-drop","path_regexx":"^/x$"}`,
		"unknown mode":  `{"mode":"explode"}`,
		"bad regex":     `{"mode":"terminal-5xx","path_regex":"^/api/($"}`,
		"status range":  `{"mode":"terminal-5xx","status":42}`,
		"negative time": `{"mode":"ws-drop","duration_s":-5}`,
		"wrong type":    `{"mode":123}`,
	} {
		status, reply := doRequest(t, http.MethodPost, server.URL+"/inject", body)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: status = %d (%s), want 400", name, status, reply)
		}
		if !strings.Contains(reply, `"error"`) {
			t.Fatalf("%s: reply = %s, want an error body", name, reply)
		}
	}
	if state := injector.State(); state.Mode != string(ModeNone) {
		t.Fatalf("a rejected request changed the state: %+v", state)
	}
}

func TestControlRouting(t *testing.T) {
	server, _, _ := testControl(t)
	if status, _ := doRequest(t, http.MethodGet, server.URL+"/inject", ""); status != http.StatusMethodNotAllowed {
		t.Fatalf("GET /inject = %d, want 405", status)
	}
	if status, _ := doRequest(t, http.MethodPost, server.URL+"/inject/state", ""); status != http.StatusMethodNotAllowed {
		t.Fatalf("POST /inject/state = %d, want 405", status)
	}
	if status, _ := doRequest(t, http.MethodGet, server.URL+"/nope", ""); status != http.StatusNotFound {
		t.Fatalf("unknown route = %d, want 404", status)
	}
}

func TestControlRecordQuery(t *testing.T) {
	server, _, rec := testControl(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	rec.Append(record{TS: stamp(base), Direction: directionForemanToServer, Kind: kindHTTP, Method: "POST", Path: "/first", Status: 200})
	rec.Append(record{TS: stamp(base.Add(time.Second)), Direction: directionAgentToServer, Kind: kindHTTP, Method: "GET", Path: "/second", Status: 401})

	status, body := doRequest(t, http.MethodGet, server.URL+"/record", "")
	if status != http.StatusOK {
		t.Fatalf("record status = %d", status)
	}
	var rows []record
	if err := json.Unmarshal([]byte(body), &rows); err != nil {
		t.Fatalf("record body is not a JSON array: %v (%s)", err, body)
	}
	if len(rows) != 2 || rows[0].Path != "/first" {
		t.Fatalf("rows = %+v", rows)
	}

	since := base.Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	_, body = doRequest(t, http.MethodGet, server.URL+"/record?since="+since+"&limit=5", "")
	if err := json.Unmarshal([]byte(body), &rows); err != nil {
		t.Fatalf("record body: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != "/second" {
		t.Fatalf("since/limit rows = %+v", rows)
	}

	for _, query := range []string{"?since=yesterday", "?limit=many", "?limit=-1"} {
		if status, reply := doRequest(t, http.MethodGet, server.URL+"/record"+query, ""); status != http.StatusBadRequest {
			t.Fatalf("/record%s = %d (%s), want 400", query, status, reply)
		}
	}

	_, body = doRequest(t, http.MethodGet, server.URL+"/record?limit=0", "")
	if err := json.Unmarshal([]byte(body), &rows); err != nil || len(rows) != 2 {
		t.Fatalf("limit=0 rows = %s", body)
	}
}

func TestControlBlobReads(t *testing.T) {
	server, _, rec := testControl(t)
	sink := rec.NewBodySink()
	sink.Write([]byte(`{"tasks":[]}`))
	_, rel := sink.Close()

	status, body := doRequest(t, http.MethodGet, server.URL+"/record/blob?path="+rel, "")
	if status != http.StatusOK || body != `{"tasks":[]}` {
		t.Fatalf("blob = %d %q", status, body)
	}
	if status, _ := doRequest(t, http.MethodGet, server.URL+"/record/blob?path=bodies/missing.bin", ""); status != http.StatusNotFound {
		t.Fatalf("missing blob = %d, want 404", status)
	}
	for _, bad := range []string{"", "../records.jsonl", "/etc/passwd"} {
		if status, _ := doRequest(t, http.MethodGet, server.URL+"/record/blob?path="+bad, ""); status != http.StatusBadRequest {
			t.Fatalf("blob %q = %d, want 400", bad, status)
		}
	}
}
