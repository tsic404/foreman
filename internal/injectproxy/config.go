package injectproxy

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// Environment variable names. The injector is a test-infrastructure
// component, so its knobs are its own (INJECT_*) instead of Foreman's
// contract table (§5.1).
const (
	EnvUpstreamURL          = "INJECT_UPSTREAM_URL"
	EnvDataAddr             = "INJECT_DATA_ADDR"
	EnvControlAddr          = "INJECT_CONTROL_ADDR"
	EnvRecordDir            = "INJECT_RECORD_DIR"
	EnvRecordResponseBodies = "INJECT_RECORD_RESPONSE_BODIES"
	EnvLogLevel             = "INJECT_LOG_LEVEL"
)

// Defaults of the access-path contract.
const (
	DefaultDataAddr    = ":8080"
	DefaultControlAddr = ":8081"
	DefaultRecordDir   = "./records"
	// DefaultRecordResponseBodies is the minimal face §4.3 needs: the C3
	// claim response原文 that forward-01 step 3 diffs. The knob is
	// configuration (deployment side), never a control-plane parameter.
	DefaultRecordResponseBodies = `^/api/daemon/tasks/claim$`

	// upstreamDialTimeout bounds dialing the test server. There is no read
	// deadline anywhere else: §4.1 forbids cutting or restarting traffic, and
	// the WS channel is long-lived by design.
	upstreamDialTimeout = 10 * time.Second
	// copyBufferSize is the per-connection relay scratch size; one WS byte
	// segment is one such read (§4.3).
	copyBufferSize = 32 << 10
	// maxHeadBytes caps one HTTP message head. A longer head is refused
	// instead of being buffered without bound; the cap is applied per line
	// while a line is read, not after a head is complete.
	maxHeadBytes = 256 << 10
	// maxChunkLineBytes caps the lines that frame a chunked body (a chunk
	// size, its trailing CRLF, a trailer field). They carry no payload: a
	// longer line is a peer that is not speaking chunked encoding.
	maxChunkLineBytes = 8 << 10
	// readerBufferSize is the bufio window in front of a connection; larger
	// heads are still read correctly (readLine handles buffer refills).
	readerBufferSize = 8 << 10
)

// Config is the injector's deployment configuration.
type Config struct {
	// Upstream is the test multica server the access path relays to.
	Upstream *url.URL
	// DataAddr is the transparent data plane: the address behind the stable
	// access point Foreman's MULTICA_SERVER_URL points at.
	DataAddr string
	// ControlAddr is the injection/record control plane (§4.2/§4.3). It is a
	// separate port so the two faces can be split by Service and the control
	// face kept internal to the test network.
	ControlAddr string
	// RecordDir holds records.jsonl, bodies/ and ws/.
	RecordDir string
	// RecordResponseBodies selects which response bodies are written to disk
	// (§4.3); matching is on the request path without query.
	RecordResponseBodies []*regexp.Regexp
	LogLevel             slog.Level
}

// LoadConfig reads the injector configuration from env (os.Getenv when getenv
// is nil). Missing values fall back to the contract defaults; malformed ones
// are errors (fail fast beats a half-configured access path).
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		DataAddr:    DefaultDataAddr,
		ControlAddr: DefaultControlAddr,
		RecordDir:   DefaultRecordDir,
		LogLevel:    slog.LevelInfo,
	}

	raw := strings.TrimSpace(getenv(EnvUpstreamURL))
	if raw == "" {
		return Config{}, fmt.Errorf("%s is required (the test multica server address)", EnvUpstreamURL)
	}
	upstream, err := url.Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("%s is not a valid URL: %v", EnvUpstreamURL, err)
	}
	if upstream.Scheme != "http" && upstream.Scheme != "https" {
		return Config{}, fmt.Errorf("%s must use http or https, got %q", EnvUpstreamURL, upstream.Scheme)
	}
	if _, _, err := net.SplitHostPort(upstream.Host); err != nil {
		upstream.Host = net.JoinHostPort(upstream.Host, defaultPort(upstream.Scheme))
	}
	if upstream.Hostname() == "" {
		return Config{}, fmt.Errorf("%s has no host: %q", EnvUpstreamURL, raw)
	}
	cfg.Upstream = upstream

	if v := strings.TrimSpace(getenv(EnvDataAddr)); v != "" {
		cfg.DataAddr = v
	}
	if v := strings.TrimSpace(getenv(EnvControlAddr)); v != "" {
		cfg.ControlAddr = v
	}
	if cfg.DataAddr == cfg.ControlAddr {
		return Config{}, fmt.Errorf("%s and %s must differ (data and control faces are separate listeners)", EnvDataAddr, EnvControlAddr)
	}
	if v := strings.TrimSpace(getenv(EnvRecordDir)); v != "" {
		cfg.RecordDir = v
	}

	respBodies := strings.TrimSpace(getenv(EnvRecordResponseBodies))
	if respBodies == "" {
		respBodies = DefaultRecordResponseBodies
	}
	if cfg.RecordResponseBodies, err = parseRegexList(EnvRecordResponseBodies, respBodies); err != nil {
		return Config{}, err
	}

	if v := strings.TrimSpace(getenv(EnvLogLevel)); v != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("%s must be debug|info|warn|error, got %q", EnvLogLevel, v)
		}
		cfg.LogLevel = level
	}
	return cfg, nil
}

// RecordsResponseBody reports whether path (without query, percent-encoded as
// sent) is in the response-body face. An empty list means "status codes only".
func (c Config) RecordsResponseBody(path string) bool {
	for _, re := range c.RecordResponseBodies {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// RecordResponseBodySources lists the configured response-body patterns, for
// startup logging.
func (c Config) RecordResponseBodySources() []string {
	sources := make([]string, 0, len(c.RecordResponseBodies))
	for _, re := range c.RecordResponseBodies {
		sources = append(sources, re.String())
	}
	return sources
}

func defaultPort(scheme string) string {
	if scheme == "https" {
		return "443"
	}
	return "80"
}

// parseRegexList splits a comma-separated regex list; an empty element is
// skipped so "a,,b" and "a,b," both mean {a, b}.
func parseRegexList(key, raw string) ([]*regexp.Regexp, error) {
	var out []*regexp.Regexp
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		re, err := regexp.Compile(part)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is not a valid regex: %v", key, part, err)
		}
		out = append(out, re)
	}
	return out, nil
}
