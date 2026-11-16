package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeDeregisterer struct {
	workspace string
	err       error
	calls     int
	onCall    func()
}

func (f *fakeDeregisterer) Deregister(context.Context) error {
	f.calls++
	if f.onCall != nil {
		f.onCall()
	}
	return f.err
}

func (f *fakeDeregisterer) WorkspaceID() string { return f.workspace }

type fakeGate struct {
	last bool
	err  error
}

func (g fakeGate) LastInstance(context.Context) (bool, error) { return g.last, g.err }

// TestShutdownOrdersCancelThenDeregisterThenDrain pins the §4.6 order: the
// loops stop first (so no new work starts), C14 goes out next, and only then
// does the HTTP listener drain. A reordering would let a claim round race a
// deregistration, or deregister while work is still being accepted.
func TestShutdownOrdersCancelThenDeregisterThenDrain(t *testing.T) {
	var order []string
	d := &fakeDeregisterer{workspace: "ws-1", onCall: func() { order = append(order, "deregister") }}

	Shutdown(context.Background(), ShutdownOptions{
		Cancel:   func() { order = append(order, "cancel") },
		Served:   []Deregisterer{d},
		Drain:    func(context.Context) { order = append(order, "drain") },
		Wait:     func(context.Context) { order = append(order, "wait") },
		Log:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Inflight: func() int { return 2 },
	})

	want := []string{"cancel", "deregister", "drain", "wait"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestShutdownLogsInflightAndOutcome covers the observability contract: an
// operator must be able to tell a clean exit from a crash, and must see how
// many tasks were still live when the exit began.
func TestShutdownLogsInflightAndOutcome(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	Shutdown(context.Background(), ShutdownOptions{
		Served:   []Deregisterer{&fakeDeregisterer{workspace: "ws-1"}},
		Log:      log,
		Inflight: func() int { return 3 },
	})

	out := buf.String()
	for _, want := range []string{`"msg":"foreman.shutdown"`, `"inflight":3`, `"msg":"foreman.stopped"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %s in shutdown log:\n%s", want, out)
		}
	}
}

// TestShutdownDeregistersEveryWorkspace asserts one C14 per served identity —
// a multi-workspace process that deregistered only the first would leave the
// rest registered against the server until their expiry.
func TestShutdownDeregistersEveryWorkspace(t *testing.T) {
	a := &fakeDeregisterer{workspace: "ws-a"}
	b := &fakeDeregisterer{workspace: "ws-b"}

	Shutdown(context.Background(), ShutdownOptions{
		Served: []Deregisterer{a, b},
		Log:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	if a.calls != 1 || b.calls != 1 {
		t.Fatalf("deregister calls = %d,%d; want 1,1", a.calls, b.calls)
	}
}

// TestDeregisterGateSkipsWhenNotLastInstance covers the survivor case: a
// process that is not the last live instance must not send C14, because the
// surviving instance holds the identity.
func TestDeregisterGateSkipsWhenNotLastInstance(t *testing.T) {
	d := &fakeDeregisterer{workspace: "ws-1"}
	var buf bytes.Buffer

	DeregisterOnShutdown(context.Background(), fakeGate{last: false},
		[]Deregisterer{d}, slog.New(slog.NewJSONHandler(&buf, nil)))

	if d.calls != 0 {
		t.Fatalf("deregistered %d times, want 0 when another instance is alive", d.calls)
	}
	if !strings.Contains(buf.String(), "deregister_skipped") {
		t.Fatalf("skip reason not logged:\n%s", buf.String())
	}
}

// TestDeregisterGateSkipsWhenStoreUnreachable: an unreachable store cannot
// prove the instance count, so the row is left to the server's expiry.
func TestDeregisterGateSkipsWhenStoreUnreachable(t *testing.T) {
	d := &fakeDeregisterer{workspace: "ws-1"}
	var buf bytes.Buffer

	DeregisterOnShutdown(context.Background(), fakeGate{err: errors.New("store down")},
		[]Deregisterer{d}, slog.New(slog.NewJSONHandler(&buf, nil)))

	if d.calls != 0 {
		t.Fatalf("deregistered %d times, want 0 when the store is unreachable", d.calls)
	}
	if !strings.Contains(buf.String(), "shared state unavailable") {
		t.Fatalf("skip reason not logged:\n%s", buf.String())
	}
}

// TestDeregisterFailureIsLoggedAndDoesNotStopTheRest: one workspace's failure
// must not prevent the others from deregistering.
func TestDeregisterFailureIsLoggedAndDoesNotStopTheRest(t *testing.T) {
	bad := &fakeDeregisterer{workspace: "ws-bad", err: errors.New("boom")}
	good := &fakeDeregisterer{workspace: "ws-good"}
	var buf bytes.Buffer

	DeregisterOnShutdown(context.Background(), nil, []Deregisterer{bad, good},
		slog.New(slog.NewJSONHandler(&buf, nil)))

	if good.calls != 1 {
		t.Fatal("a failed deregistration stopped the following workspace")
	}
	out := buf.String()
	if !strings.Contains(out, "deregister_failed") || !strings.Contains(out, "deregistered") {
		t.Fatalf("want both failure and success lines:\n%s", out)
	}
}

// TestShutdownRespectsDeadline: a Wait that blocks must not hold the exit past
// the caller's budget — the process still has to stop.
func TestShutdownRespectsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		Shutdown(ctx, ShutdownOptions{
			Wait: func(c context.Context) { <-c.Done() },
			Log:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Shutdown ignored the context deadline")
	}
}
