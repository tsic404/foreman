package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	mathrand "math/rand/v2"
	"time"

	"github.com/tsic404/foreman/internal/dispatch"
)

// Endpoint is the dispatcher's lifecycle-callback vocabulary, re-exported so
// the proxy's two faces name endpoints exactly the way the dispatcher does.
type Endpoint = dispatch.Endpoint

// Task lifecycle endpoints (§2.2), re-exported from the dispatcher.
const (
	EPStart        = dispatch.EPStart
	EPProgress     = dispatch.EPProgress
	EPMessages     = dispatch.EPMessages
	EPUsage        = dispatch.EPUsage
	EPComplete     = dispatch.EPComplete
	EPFail         = dispatch.EPFail
	EPCancelAck    = dispatch.EPCancelAck
	EPSession      = dispatch.EPSession
	EPWaitLocalDir = dispatch.EPWaitLocalDir
)

// ErrTaskNotFound is the C13 "the server does not know this task" signal.
var ErrTaskNotFound = dispatch.ErrTaskNotFound

// terminalRetryWaits is the §10 retry budget for terminal forwards.
var terminalRetryWaits = []time.Duration{0, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second}

// jitterBand is the fraction of a step that jitter may add. One constant so the
// claim backoff and the terminal-forward ladder cannot drift apart.
const jitterBand = 0.25

// jitterWait spreads one retry step over [d, d*(1+jitterBand)). Every terminal
// forward and every claim loop shares its ladder with siblings, so a server
// blip would otherwise align them on the same instant and hit recovery as one
// burst.
func jitterWait(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(float64(d)*jitterBand*mathrand.Float64())
}

// maxBodyBytes bounds a daemon-face request body: the face carries
// control-plane JSON, never payloads.
const maxBodyBytes = 32 << 20

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("proxy: marshal static body: " + err.Error())
	}
	return b
}

func bodyReader(b []byte) io.Reader {
	if b == nil {
		return nil
	}
	return bytes.NewReader(b)
}

func readLimited(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxBodyBytes+1))
}
