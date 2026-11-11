package errorformat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/felinics/twilight/sdk"
)

// maxUpstreamRunes caps the upstream message appended to an APIError's
// Message. The upstream body may echo parts of the request, so only the start
// of its message is kept.
const maxUpstreamRunes = 300

// openRouterMetadata is the part of OpenRouter's error metadata that names the
// upstream provider and carries its response: provider_name is null when no
// provider was reached, and raw is the upstream's body as text, which may be
// a JSON object, a JSON array, server-sent events or plain text.
type openRouterMetadata struct {
	ProviderName string          `json:"provider_name"`
	Raw          json.RawMessage `json:"raw"`
}

// openRouterEnvelope locates the metadata. The Chat Completions endpoint puts
// it in the error object; the Responses and Anthropic Messages endpoints put
// it at the top level, beside the error. object is set on a Responses API
// response object, whose top-level metadata is the caller's own.
type openRouterEnvelope struct {
	Object   string              `json:"object"`
	Error    json.RawMessage     `json:"error"`
	Metadata *openRouterMetadata `json:"metadata"`
}

// AddOpenRouterUpstream appends what OpenRouter relays about a failed
// upstream provider to e.Message. OpenRouter answers an upstream failure with
// a generic message such as "Provider returned error" and puts the upstream
// provider's name in metadata.provider_name and its response in metadata.raw
// (https://openrouter.ai/docs/api-reference/errors). When a message can be
// read from raw and e.Message does not already contain it, the result is
// "<message> (<provider_name>: <upstream message>)"; when e.Message already
// contains it, only the provider name is appended. The upstream message has
// its whitespace collapsed and is cut to 300 characters.
//
// A body without that metadata, or whose raw has no readable message, leaves
// e unchanged. Type, Code and Kind are never changed.
func AddOpenRouterUpstream(e *sdk.APIError) {
	meta := findOpenRouterMetadata(e.Body)
	if meta == nil {
		return
	}
	upstream := truncateRunes(collapseSpace(rawMessage(meta.Raw)), maxUpstreamRunes)
	if upstream == "" {
		return
	}
	provider := truncateRunes(collapseSpace(meta.ProviderName), maxUpstreamRunes)
	detail := upstream
	if strings.Contains(collapseSpace(e.Message), upstream) {
		if provider == "" {
			return
		}
		detail = ""
	}
	switch {
	case provider != "" && detail != "":
		detail = provider + ": " + detail
	case provider != "":
		detail = provider
	}
	if e.Message == "" {
		e.Message = detail
		return
	}
	e.Message += " (" + detail + ")"
}

// findOpenRouterMetadata returns the metadata of error.metadata or, failing
// that, of the top-level metadata, or nil when neither has a raw.
func findOpenRouterMetadata(body []byte) *openRouterMetadata {
	var env openRouterEnvelope
	if json.Unmarshal(body, &env) != nil {
		return nil
	}
	var inner struct {
		Metadata *openRouterMetadata `json:"metadata"`
	}
	if bytes.HasPrefix(bytes.TrimSpace(env.Error), []byte("{")) &&
		json.Unmarshal(env.Error, &inner) == nil && inner.Metadata != nil && len(inner.Metadata.Raw) > 0 {
		return inner.Metadata
	}
	if env.Object == "" && env.Metadata != nil && len(env.Metadata.Raw) > 0 {
		return env.Metadata
	}
	return nil
}

// rawMessage reads the upstream message from metadata.raw, which is normally
// a JSON string holding the upstream body; a raw sent as JSON is read as is.
func rawMessage(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] != '"' {
		return jsonMessage(raw)
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return ""
	}
	return textMessage(text)
}

// textMessage reads a message from an upstream body: a JSON value, a stream of
// server-sent events, or plain text. A body that looks like JSON but does not
// parse, or an HTML page, yields "".
func textMessage(text string) string {
	text = strings.TrimSpace(text)
	switch {
	case text == "":
		return ""
	case text[0] == '{' || text[0] == '[':
		return jsonMessage([]byte(text))
	case isSSE(text):
		return sseMessage(text)
	case isHTML(text):
		return ""
	default:
		return text
	}
}

func isSSE(text string) bool {
	return strings.HasPrefix(text, "data:") || strings.HasPrefix(text, "event:") ||
		strings.Contains(text, "\ndata:")
}

func isHTML(text string) bool {
	lower := strings.ToLower(text[:min(len(text), 16)])
	return strings.HasPrefix(lower, "<!doctype") || strings.HasPrefix(lower, "<html")
}

// sseMessage returns the message of the first data line that has one.
func sseMessage(text string) string {
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(nil, len(text)+1)
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" || data == "[DONE]" {
			continue
		}
		if msg := textMessage(data); msg != "" {
			return msg
		}
	}
	return ""
}

// jsonMessage reads a message from a JSON value, or returns "" when it does
// not parse or holds none.
func jsonMessage(raw []byte) string {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return ""
	}
	return valueMessage(v, 0)
}

// messageKeys are the members an error object keeps its message in, in the
// order they are tried: a top-level message, OpenAI's and Anthropic's
// error.message or an error string, FastAPI's detail and msg, and the
// errors array some APIs send.
var messageKeys = []string{"message", "error", "detail", "msg", "error_message", "errors"}

// valueMessage returns the message of a decoded JSON value: a string itself,
// the first message member of an object, or the messages of an array's items
// joined with "; ".
func valueMessage(v any, depth int) string {
	if depth > 4 {
		return ""
	}
	switch v := v.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		for _, key := range messageKeys {
			if member, ok := v[key]; ok {
				if msg := valueMessage(member, depth+1); msg != "" {
					return msg
				}
			}
		}
	case []any:
		var msgs []string
		for _, item := range v {
			if msg := valueMessage(item, depth+1); msg != "" {
				msgs = append(msgs, msg)
			}
		}
		return strings.Join(msgs, "; ")
	}
	return ""
}

// collapseSpace trims s and replaces each run of whitespace with one space, so
// a multi-line upstream message stays on one line.
func collapseSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// truncateRunes cuts s to at most n runes, marking a cut with "…".
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return strings.TrimSpace(string(runes[:n-1])) + "…"
}
