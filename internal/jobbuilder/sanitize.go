package jobbuilder

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
)

// maxPayloadAnnotationBytes caps the base64 annotation value; larger payloads
// drop the annotation (contract §3.2 ">128KB 时省略").
const maxPayloadAnnotationBytes = 128 * 1024

// serverTokenShape matches mdt_/mul_ token prefixes: any hit in the redacted
// payload drops the whole annotation (F3: rather no annotation than a leak).
var serverTokenShape = regexp.MustCompile(`m(dt|ul)_`)

// sanitizePayload returns the base64-encoded redacted payload for the
// foreman.tsic.top/payload annotation; ok is false when the annotation must
// be omitted.
func sanitizePayload(payload json.RawMessage) (encoded string, ok bool) {
	if len(payload) == 0 {
		return "", false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		// An unparseable payload cannot be redacted, so it is never annotated.
		return "", false
	}
	delete(fields, "auth_token")
	delete(fields, "remote_mcp_daemon_token")
	redacted, err := json.Marshal(fields)
	if err != nil {
		return "", false
	}
	if serverTokenShape.Match(redacted) {
		return "", false
	}
	encoded = base64.StdEncoding.EncodeToString(redacted)
	if len(encoded) > maxPayloadAnnotationBytes {
		return "", false
	}
	return encoded, true
}
