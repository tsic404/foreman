package jobexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// RequestEnv carries the phase request. The supervisor appends it to the
// inherited environment, so an adapter reads it directly.
const RequestEnv = "FOREMAN_JOB_REQUEST"

// ExitSessionNotCreated is the only exit that proves no remote session
// exists. Every other failure leaves the creation result unknown, and the
// supervisor then isolates the reservation instead of returning it.
const ExitSessionNotCreated = 4

// PhaseFunc answers one phase. The returned value becomes the single JSON
// document on stdout.
type PhaseFunc func(ctx context.Context, request json.RawMessage) (any, error)

// ExitError asks Main for one specific exit status.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }

func (e *ExitError) Unwrap() error { return e.Err }

// Exit wraps err with an explicit status.
func Exit(code int, err error) error { return &ExitError{Code: code, Err: err} }

// Main runs one adapter process and returns its exit status. It is the thin
// process wrapper around Serve: argv[1] is the phase, RequestEnv the request.
func Main(phases map[string]PhaseFunc) int {
	return Serve(os.Args[1:], os.Getenv(RequestEnv), phases, os.Stdout, os.Stderr)
}

// Serve answers one phase and returns the process's exit status. A status of 2
// is always a usage or wiring error in the adapter itself; 1 is a failed
// observation whose remote effect is unknown; ExitSessionNotCreated is the
// one status that proves no session was opened.
func Serve(args []string, request string, phases map[string]PhaseFunc, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: <adapter> <phase>")
		return 2
	}
	phase := args[0]
	fn, ok := phases[phase]
	if !ok {
		fmt.Fprintf(stderr, "unsupported phase %q\n", phase)
		return 2
	}
	body := strings.TrimSpace(request)
	if body == "" {
		fmt.Fprintf(stderr, "%s is empty: the supervisor passes one JSON request\n", RequestEnv)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	answer, err := fn(ctx, json.RawMessage(body))
	if err != nil {
		var exit *ExitError
		if errors.As(err, &exit) {
			fmt.Fprintln(stderr, exit.Error())
			return exit.Code
		}
		fmt.Fprintln(stderr, err.Error())
		return 1
	}
	if answer == nil {
		answer = struct{}{}
	}
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(answer); err != nil {
		fmt.Fprintf(stderr, "encode the answer: %v\n", err)
		return 1
	}
	return 0
}

// decodeRequest decodes one request strictly. Adapters ship in the runtime
// image beside the supervisor, so a field the adapter does not know is a
// version mismatch rather than something to ignore.
func decodeRequest(body json.RawMessage, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("decode the request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("the request carries trailing JSON")
	}
	return nil
}
