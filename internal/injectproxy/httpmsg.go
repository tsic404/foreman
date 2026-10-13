package injectproxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// headerField is one header line as sent: name keeps the sender's casing,
// value the field value. Both feed the record face and injection matching
// only — the relayed bytes are the ones read off the wire, never a rendering
// of these.
type headerField struct {
	name  string
	value string
}

// messageHead is the parsed view of one HTTP message head. raw is the head
// exactly as it arrived (start line and every header line, terminator
// included) and is what gets forwarded.
type messageHead struct {
	raw     []byte
	proto   string
	method  string
	target  string
	status  int
	reason  string
	headers []headerField
}

// bodyFraming is how a message body is delimited (RFC 9112 §6).
type bodyFraming int

const (
	bodyNone bodyFraming = iota
	bodyContentLength
	bodyChunked
	bodyUntilClose
)

// readHead reads one message head verbatim, up to and including the empty
// line that ends it.
func readHead(r *bufio.Reader) ([]byte, error) {
	var head []byte
	for {
		line, err := readLine(r, maxHeadBytes)
		if len(line) > 0 {
			head = append(head, line...)
			if len(head) > maxHeadBytes {
				return nil, fmt.Errorf("message head exceeds %d bytes", maxHeadBytes)
			}
		}
		if err != nil {
			return nil, err
		}
		if isBlankLine(line) {
			return head, nil
		}
	}
}

// readRequestHead reads the next request head together with the blank lines a
// client may leave in front of it (RFC 9112 §2.2 lets a server ignore them).
// They are not a request, but §4.1 is byte transparency, so the caller relays
// them instead of dropping them.
func readRequestHead(r *bufio.Reader) (leading, head []byte, err error) {
	for range 4 {
		next, err := readHead(r)
		if err != nil {
			return nil, nil, err
		}
		if len(bytes.TrimSpace(next)) > 0 {
			return leading, next, nil
		}
		leading = append(leading, next...)
	}
	return nil, nil, fmt.Errorf("too many blank lines before the request line")
}

func parseRequestHead(raw []byte) (*messageHead, error) { return parseHead(raw, false) }

func parseResponseHead(raw []byte) (*messageHead, error) { return parseHead(raw, true) }

func parseHead(raw []byte, isResponse bool) (*messageHead, error) {
	lines := splitHeadLines(raw)
	if len(lines) == 0 || len(lines[0]) == 0 {
		return nil, fmt.Errorf("empty start line")
	}
	head := &messageHead{raw: raw}
	parts := bytes.SplitN(lines[0], []byte(" "), 3)
	if isResponse {
		if len(parts) < 2 {
			return nil, fmt.Errorf("malformed status line %q", lines[0])
		}
		status, err := strconv.Atoi(string(parts[1]))
		if err != nil || status < 100 || status > 599 {
			return nil, fmt.Errorf("malformed status code %q", parts[1])
		}
		head.proto, head.status = string(parts[0]), status
		if len(parts) == 3 {
			head.reason = string(parts[2])
		}
	} else {
		if len(parts) != 3 {
			return nil, fmt.Errorf("malformed request line %q", lines[0])
		}
		head.method, head.target, head.proto = string(parts[0]), string(parts[1]), string(parts[2])
	}
	for _, line := range lines[1:] {
		if len(line) == 0 {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			continue // obsolete line folding: ignored, yet still relayed verbatim
		}
		name, value, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			return nil, fmt.Errorf("malformed header line %q", line)
		}
		head.headers = append(head.headers, headerField{name: string(name), value: strings.TrimSpace(string(value))})
	}
	return head, nil
}

// splitHeadLines splits a head into its lines with the line endings removed.
func splitHeadLines(raw []byte) [][]byte {
	lines := bytes.Split(raw, []byte("\n"))
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		out = append(out, bytes.TrimSuffix(line, []byte("\r")))
	}
	if n := len(out); n > 0 && len(out[n-1]) == 0 {
		out = out[:n-1] // the terminator line
	}
	return out
}

// first returns the first value of a header field (empty when absent).
func (h *messageHead) first(name string) string {
	for _, f := range h.headers {
		if strings.EqualFold(f.name, name) {
			return f.value
		}
	}
	return ""
}

// hasToken reports whether a header field carries token as one of its
// comma-separated members (header values are lists, not strings).
func (h *messageHead) hasToken(name, token string) bool {
	for _, f := range h.headers {
		if !strings.EqualFold(f.name, name) {
			continue
		}
		for _, part := range strings.Split(f.value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// requestPath is the request target's path without its query (§4.2 matches on
// the path; the record face records the same form).
func (h *messageHead) requestPath() string {
	target := h.target
	if i := strings.Index(target, "://"); i >= 0 {
		rest := target[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			target = rest[j:]
		} else {
			target = "/"
		}
	}
	if i := strings.IndexByte(target, '?'); i >= 0 {
		target = target[:i]
	}
	return target
}

// isWebSocket reports an upgrade request for the daemon WS channel: the only
// connection HTTP turns into a tunnel (§4.2 ws-drop, §4.3 ws-chunk).
func (h *messageHead) isWebSocket() bool {
	return h.hasToken("Connection", "upgrade") && h.hasToken("Upgrade", "websocket")
}

func (h *messageHead) hasUpgrade() bool { return h.hasToken("Connection", "upgrade") }

func (h *messageHead) expectContinue() bool { return h.hasToken("Expect", "100-continue") }

// keepAlive reports whether the connection may carry another exchange after
// this message (RFC 9112 §9.3).
func (h *messageHead) keepAlive() bool {
	if h.hasToken("Connection", "close") {
		return false
	}
	if h.proto == "HTTP/1.0" {
		return h.hasToken("Connection", "keep-alive")
	}
	return true
}

// requestFraming reports how the request body is delimited: chunked, a
// content length, or no body at all.
func (h *messageHead) requestFraming() (bodyFraming, int64, error) {
	if h.hasToken("Transfer-Encoding", "chunked") {
		return bodyChunked, 0, nil
	}
	return h.contentLength()
}

// responseFraming reports how the response body is delimited. reqMethod is
// the method of the request being answered — a response to HEAD carries no
// body whatever its headers claim.
func (h *messageHead) responseFraming(reqMethod string) (bodyFraming, int64, error) {
	if (h.status >= 100 && h.status < 200) || h.status == 204 || h.status == 304 {
		return bodyNone, 0, nil
	}
	if reqMethod == "HEAD" {
		return bodyNone, 0, nil
	}
	if h.hasToken("Transfer-Encoding", "chunked") {
		return bodyChunked, 0, nil
	}
	framing, length, err := h.contentLength()
	if err != nil {
		return bodyNone, 0, err
	}
	if framing == bodyNone {
		return bodyUntilClose, 0, nil
	}
	return framing, length, nil
}

// contentLength reads the Content-Length framing; bodyNone means the header
// is absent (a present "Content-Length: 0" is an empty body, not a missing
// length — the distinction decides whether a response ends at the next byte
// or at connection close).
func (h *messageHead) contentLength() (bodyFraming, int64, error) {
	value := h.first("Content-Length")
	if value == "" {
		return bodyNone, 0, nil
	}
	length, err := strconv.ParseInt(value, 10, 64)
	if err != nil || length < 0 {
		return bodyNone, 0, fmt.Errorf("invalid Content-Length %q", value)
	}
	return bodyContentLength, length, nil
}

// hasRequestBody reports whether a framing actually carries bytes: a
// Content-Length: 0 body is empty, not a body to relay.
func hasRequestBody(framing bodyFraming, length int64) bool {
	switch framing {
	case bodyNone:
		return false
	case bodyContentLength:
		return length > 0
	default:
		return true
	}
}

// relayBody copies one framed body from src to dst. Framing bytes are
// forwarded verbatim — a chunked body keeps its chunk headers — while the
// decoded body bytes are tee'd into sink for the record face (§4.3).
func relayBody(dst io.Writer, src *bufio.Reader, framing bodyFraming, length int64, sink *bodySink, buf []byte) error {
	switch framing {
	case bodyNone:
		return nil
	case bodyContentLength:
		_, err := io.CopyBuffer(io.MultiWriter(dst, sink), io.LimitReader(src, length), buf)
		return err
	case bodyUntilClose:
		_, err := io.CopyBuffer(io.MultiWriter(dst, sink), src, buf)
		return err
	case bodyChunked:
		return relayChunkedBody(dst, src, sink, buf)
	default:
		return fmt.Errorf("unknown body framing %d", framing)
	}
}

func relayChunkedBody(dst io.Writer, src *bufio.Reader, sink *bodySink, buf []byte) error {
	for {
		line, err := readLine(src, maxChunkLineBytes)
		if err != nil {
			return err
		}
		if _, err := dst.Write(line); err != nil {
			return err
		}
		size, err := parseChunkSize(line)
		if err != nil {
			return err
		}
		if size == 0 {
			return relayTrailers(dst, src)
		}
		if _, err := io.CopyBuffer(io.MultiWriter(dst, sink), io.LimitReader(src, size), buf); err != nil {
			return err
		}
		crlf, err := readLine(src, maxChunkLineBytes)
		if err != nil {
			return err
		}
		if _, err := dst.Write(crlf); err != nil {
			return err
		}
	}
}

// relayTrailers forwards the trailer section that closes a chunked body.
func relayTrailers(dst io.Writer, src *bufio.Reader) error {
	for {
		line, err := readLine(src, maxChunkLineBytes)
		if len(line) > 0 {
			if _, werr := dst.Write(line); werr != nil {
				return werr
			}
		}
		if err != nil {
			return err
		}
		if isBlankLine(line) {
			return nil
		}
	}
}

// parseChunkSize reads the hex size that starts a chunk, ignoring chunk
// extensions.
func parseChunkSize(line []byte) (int64, error) {
	value := string(bytes.TrimSpace(line))
	if i := strings.IndexByte(value, ';'); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	size, err := strconv.ParseInt(value, 16, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid chunk size %q", value)
	}
	return size, nil
}

// errLineTooLong reports a line longer than the caller's cap.
var errLineTooLong = errors.New("line exceeds its limit")

// readLine reads up to and including the next LF; the returned bytes are the
// wire bytes when the call succeeded. bufio.ErrBufferFull is retried with the
// accumulated prefix so lines longer than the reader window are read whole,
// and the cap is enforced on every chunk: a line that exceeds max is refused
// while it arrives — the caller never holds more than max bytes of it — and
// the rest of the line is drained so the reader stays on a line boundary.
func readLine(r *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	overflowed := false
	drained := 0
	for {
		chunk, err := r.ReadSlice('\n')
		if overflowed {
			drained += len(chunk)
		} else {
			line = append(line, chunk...)
			if len(line) > max {
				overflowed, line = true, nil
			}
		}
		if err != bufio.ErrBufferFull {
			if overflowed {
				return nil, fmt.Errorf("%w: %d bytes", errLineTooLong, max)
			}
			return line, err
		}
		// A line that keeps growing past the cap without ending is drained no
		// further: the caller abandons the stream, and a reader left mid-line
		// costs at most one mis-parsed line.
		if overflowed && drained > max {
			return nil, fmt.Errorf("%w: %d bytes", errLineTooLong, max)
		}
	}
}

func isBlankLine(line []byte) bool { return len(bytes.TrimRight(line, "\r\n")) == 0 }

// writeSyntheticStatus writes the injected response: the configured status
// and an empty body. §4.2 lets the fault be an HTTP status and a
// connection-level close — nothing is synthesized beyond that.
func writeSyntheticStatus(w io.Writer, status int, closeConn bool) error {
	var head bytes.Buffer
	fmt.Fprintf(&head, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	head.WriteString("Content-Length: 0\r\n")
	if closeConn {
		head.WriteString("Connection: close\r\n")
	}
	head.WriteString("\r\n")
	_, err := w.Write(head.Bytes())
	return err
}

// drainBuffered forwards whatever a reader already buffered (bytes that
// arrived while the previous message was being processed) and reports them as
// one byte segment, so a tunnel never loses the bytes read during its
// handshake.
func drainBuffered(r *bufio.Reader, dst io.Writer, record func([]byte)) error {
	n := r.Buffered()
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if _, err := dst.Write(buf); err != nil {
		return err
	}
	record(buf)
	return nil
}
