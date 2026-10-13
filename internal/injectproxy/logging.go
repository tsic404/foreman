package injectproxy

import (
	"context"
	"io"
	"log/slog"
	"regexp"
)

// tokenPattern matches the wire form of every credential the injector can see
// passing through. §4.1.4/02-boundaries F7 forbid any of them from reaching a
// log line; the redacting handler below makes that structural instead of
// relying on call-site discipline.
//
// The injector carries its own copy of this guard rather than importing the
// product's observability package: that package pulls the Kubernetes client
// into a test component whose only dependency is a net.Conn.
var tokenPattern = regexp.MustCompile(`(fmj|mdt|mul|mat)_[A-Za-z0-9_-]{8,}`)

const redactedValue = "[redacted]"

// NewLogger builds the injector's process logger: single-line JSON with the
// repo's fixed fields (ts, level, msg, component) and token redaction applied
// to every message, attribute and pre-bound attribute.
func NewLogger(w io.Writer, level slog.Level, component string) *slog.Logger {
	json := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
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
	// Pre-bound attrs ride every record without passing through Handle.
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
// group cannot leak either. An error is rewritten through its message: slog's
// JSON handler writes err.Error() verbatim, and the errors on this path are
// built from wire bytes.
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
		switch v := a.Value.Any().(type) {
		case error:
			return slog.String(a.Key, redactString(v.Error()))
		case string:
			return slog.String(a.Key, redactString(v))
		}
	}
	return a
}

func redactString(s string) string {
	return tokenPattern.ReplaceAllString(s, redactedValue)
}
