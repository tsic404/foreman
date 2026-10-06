package observability

import (
	"context"
	"io"
	"log/slog"
	"regexp"
)

// tokenPattern matches the wire form of every credential Foreman handles
// (Job Token fmj_, server mdt_/mul_, task mat_). Log lines must never carry
// a token in clear (observability.md §禁止); the redacting handler below
// makes that structural instead of relying on call-site discipline.
var tokenPattern = regexp.MustCompile(`(fmj|mdt|mul|mat)_[A-Za-z0-9_-]{8,}`)

const redactedValue = "[redacted]"

// NewLogger builds the process logger: single-line JSON via
// slog.JSONHandler with the contract's fixed fields — ts (renamed from
// slog's "time"), level, msg, component. The returned logger redacts token
// values and any attr keyed "auth_token" before they reach w.
//
// An empty component leaves the logger untagged: the composition root uses
// that as the shared base, so components that tag their own component
// (slog.Default().With("component", …)) emit exactly one component field.
func NewLogger(w io.Writer, level slog.Level, component string) *slog.Logger {
	json := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		// The fixed field is called ts (observability.md §结构化日志), not
		// slog's default "time".
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "ts"
			}
			return a
		},
	})
	log := slog.New(&redactHandler{next: json})
	if component == "" {
		return log
	}
	return log.With("component", component)
}

// redactHandler scrubs credentials from every record. Callers must still
// never log a full task payload; this is the last line of defense, not the
// primary mechanism.
type redactHandler struct {
	next slog.Handler
}

func (h *redactHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *redactHandler) Handle(ctx context.Context, r slog.Record) error {
	scrubbed := slog.NewRecord(r.Time, r.Level, redactString(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		scrubbed.AddAttrs(redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, scrubbed)
}

func (h *redactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Pre-bound attrs ride every record without passing through Handle, so
	// they must be scrubbed here or With("auth_token", tok) would leak.
	scrubbed := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		scrubbed = append(scrubbed, redactAttr(a))
	}
	return &redactHandler{next: h.next.WithAttrs(scrubbed)}
}

func (h *redactHandler) WithGroup(name string) slog.Handler {
	return &redactHandler{next: h.next.WithGroup(name)}
}

// redactAttr rewrites one attribute; groups recurse so a token nested in a
// grouped value cannot leak either.
func redactAttr(a slog.Attr) slog.Attr {
	if a.Key == "auth_token" {
		return slog.String(a.Key, redactedValue)
	}
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindString:
		return slog.String(a.Key, redactString(a.Value.String()))
	case slog.KindGroup:
		attrs := a.Value.Group()
		scrubbed := make([]slog.Attr, 0, len(attrs))
		for _, ga := range attrs {
			scrubbed = append(scrubbed, redactAttr(ga))
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(scrubbed...)}
	case slog.KindAny:
		if s, ok := a.Value.Any().(string); ok {
			return slog.String(a.Key, redactString(s))
		}
	}
	return a
}

func redactString(s string) string {
	return tokenPattern.ReplaceAllString(s, redactedValue)
}
