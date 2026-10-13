// Package providertest is the seam conformance suite for chat providers.
//
// It reaches a provider through sdk.Generate and sdk.Stream, plus Provider.Test
// for the health check, so the same
// fixtures keep working when the provider interface underneath changes: the
// suite asserts behavior, not method signatures. A provider package supplies a
// Fixture -- how to construct the provider against a test server, plus replies
// in its own wire format -- and every provider runs the same cases.
//
// The cases exist because a provider boundary has two failure modes that no
// compile error catches. A request field can be silently dropped on the way to
// the wire, and the streaming and non-streaming paths can disagree about the
// same response. Both are invisible until production, and both are checked here
// without the suite knowing any provider's wire format.
package providertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// Fixture is one chat provider under test.
type Fixture struct {
	// NewProvider binds the provider to baseURL, the suite's test server.
	NewProvider func(baseURL string) sdk.Provider
	// ModelID is the model every case requests.
	ModelID string
	// Reply answers a non-streaming request in this provider's wire format. It
	// may assert on the request it received: t.Errorf is safe from the server
	// goroutine.
	Reply http.HandlerFunc
	// ReplyStream answers a streaming request. Nil skips the stream case.
	ReplyStream http.HandlerFunc
	// ReplyError answers a request with a provider-shaped error. Nil skips the
	// error case. It answers both the generated and the streamed request.
	ReplyError http.HandlerFunc
	// WantError is the *sdk.APIError that ReplyError must surface on both
	// paths. Provider, StatusCode, Type, Code, Message, RequestID and Kind are
	// compared; Header and Body must be what ReplyError wrote. Nil only checks
	// that the reply becomes an error.
	WantError *sdk.APIError
	// ReplyErrorBody answers a non-streaming request with a 2xx body that
	// reports a failure. Nil skips that half of the in-band error case.
	ReplyErrorBody http.HandlerFunc
	// ReplyErrorEvent answers a streaming request with a 2xx stream that
	// reports a failure in an event after the stream started. Nil skips that
	// half of the in-band error case.
	ReplyErrorEvent http.HandlerFunc
	// WantInBandError is the *sdk.APIError that ReplyErrorBody and
	// ReplyErrorEvent must surface. It is compared as WantError is; its
	// StatusCode is 0, and Body must be the error object or event data found
	// in what the handler wrote.
	WantInBandError *sdk.APIError
	// ReplyStreamIncomplete answers a streaming request with a stream that
	// ends cleanly before the event that marks it complete. Nil skips the
	// case.
	ReplyStreamIncomplete http.HandlerFunc
	// Secret is the credential NewProvider authenticates with. It must not
	// appear in the error text.
	Secret string
	// Options, when set, is sent as this provider's own entry in
	// Request.ProviderOptions (keyed by Provider.Name()) and must reach the
	// request body: an option the provider silently drops is indistinguishable
	// from one the caller never set.
	Options json.RawMessage
	// Want is what Reply must map to.
	Want Want
	// Caps records what this provider does not support.
	Caps Caps
}

// Want is the provider-neutral meaning of the Fixture's success reply.
type Want struct {
	Text         string
	Reasoning    string
	ToolCalls    []sdk.ToolCall
	FinishReason sdk.FinishReason
	// TotalTokens is the reply's total token count; 0 skips the usage check.
	TotalTokens int
	// Response is the reply's response metadata; zero skips the check. The
	// paths-agree case compares the two paths' metadata regardless.
	Response sdk.ResponseMetadata
}

// Caps records behavior a provider legitimately does not have, so that a case
// is skipped rather than silently passing.
type Caps struct {
	// NoUsage: this provider's wire format carries no token counts.
	NoUsage bool
	// NoFinishReason: this provider's wire format carries no finish reason.
	NoFinishReason bool
}

// Factory builds a fresh Fixture for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("request", func(t *testing.T) { testRequest(t, factory(t)) })
	t.Run("generate", func(t *testing.T) { testGenerate(t, factory(t)) })
	t.Run("stream", func(t *testing.T) { testStream(t, factory(t)) })
	t.Run("paths agree", func(t *testing.T) { testPathsAgree(t, factory(t)) })
	t.Run("error", func(t *testing.T) { testError(t, factory(t)) })
	t.Run("in-band error", func(t *testing.T) { testInBandError(t, factory(t)) })
	t.Run("incomplete stream", func(t *testing.T) { testStreamIncomplete(t, factory(t)) })
	t.Run("malformed stream", func(t *testing.T) { testStreamMalformed(t, factory(t)) })
	t.Run("health", func(t *testing.T) { testHealth(t, factory(t)) })
	t.Run("model probe", func(t *testing.T) { testModelProbe(t, factory(t)) })
}

// The markers travel through the Request untouched and must come out of the
// provider's wire encoding, whatever it is. They are alphanumeric with dashes
// and underscores so that no JSON encoder escapes them, which is what makes a
// substring search a valid check.
const (
	systemMarker   = "providertest-system-marker-2d91"
	userMarker     = "providertest-user-marker-2d91"
	toolMarker     = "providertest_tool_marker_2d91"
	toolDescMarker = "providertest-tool-description-marker-2d91"
)

func (f Fixture) model(p sdk.Provider) *sdk.Model {
	return &sdk.Model{ID: f.ModelID, Provider: p}
}

func request() sdk.Request {
	return sdk.Request{
		System:   systemMarker,
		Messages: []sdk.Message{sdk.UserMessage(userMarker)},
		Tools: []sdk.ToolDefinition{{
			Name:        toolMarker,
			Description: toolDescMarker,
			Parameters:  &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"city": {Type: "string"}}, Required: []string{"city"}},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceAuto},
	}
}

// recorder wraps a fixture handler to capture what the provider actually sent,
// so the suite can look for the markers and for the model.
type recorder struct {
	next http.HandlerFunc
	mu   sync.Mutex
	hits int
	seen []string
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewReader(body))
	// A provider may carry the model in the path rather than the body (Google
	// puts it in the URL), so both count as reaching the wire.
	r.mu.Lock()
	r.hits++
	r.seen = append(r.seen, req.URL.String()+"\n"+string(body))
	r.mu.Unlock()
	r.next(w, req)
}

// wire reports what reached the server. The handler runs on the server
// goroutine, so its writes are guarded.
// withOptions attaches the fixture's provider options under the provider's own
// namespace.
func (f Fixture) withOptions(p sdk.Provider, req sdk.Request) sdk.Request {
	if len(f.Options) == 0 {
		return req
	}
	req.ProviderOptions = map[string]json.RawMessage{p.Name(): f.Options}
	return req
}

// wantOptionsOnWire requires the fixture's provider options to reach the request
// body, in the provider's own namespace.
func wantOptionsOnWire(t *testing.T, rec *recorder, f Fixture, op string) {
	t.Helper()
	if len(f.Options) == 0 {
		return
	}
	var want map[string]json.RawMessage
	if err := json.Unmarshal(f.Options, &want); err != nil {
		t.Fatalf("fixture options are not a JSON object: %v", err)
	}
	_, seen := rec.wire()
	flat := strings.Join(strings.Fields(seen), "")
	for key, value := range want {
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err != nil {
			t.Fatalf("fixture option %q is not JSON: %v", key, err)
		}
		pair := `"` + key + `":` + compact.String()
		if !strings.Contains(flat, pair) {
			t.Errorf("%s: provider option %s never reached the request body", op, pair)
		}
	}
}

func (r *recorder) wire() (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := ""
	for _, s := range r.seen {
		seen += s
	}
	return r.hits, seen
}

// serve starts a server for one case and returns a provider bound to it.
func serve(t *testing.T, f Fixture, handler http.HandlerFunc) (sdk.Provider, *recorder) {
	t.Helper()
	rec := &recorder{next: handler}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	if f.NewProvider == nil {
		t.Fatal("fixture has no NewProvider")
	}
	p := f.NewProvider(srv.URL)
	if p == nil {
		t.Fatal("fixture returned a nil provider")
	}
	return p, rec
}

// wantSentOnWire asserts that the Request the suite built survived the
// provider's encoding: every marker and the model ID must appear in either the
// URL or the body, and the handler must have been reached at all.
func wantSentOnWire(t *testing.T, rec *recorder, modelID string, op string) {
	t.Helper()
	hits, seen := rec.wire()
	if hits == 0 {
		t.Fatalf("%s: the provider sent no request", op)
	}
	for _, want := range []struct{ what, marker string }{
		{"model", modelID},
		{"system", systemMarker},
		{"user message", userMarker},
		{"tool name", toolMarker},
		{"tool description", toolDescMarker},
	} {
		if want.marker == "" {
			continue
		}
		if !contains(seen, want.marker) {
			t.Errorf("%s: the %s never reached the wire (%q absent from URL and body)", op, want.what, want.marker)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// testRequest covers the drop-a-field failure: sdk.Request must be what the
// provider encodes, not a subset of it.
func testRequest(t *testing.T, f Fixture) {
	ctx := context.Background()
	p, rec := serve(t, f, f.Reply)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	if _, err := f.model(p).Generate(ctx, req); err != nil {
		t.Fatalf("generate: %v", err)
	}
	wantSentOnWire(t, rec, f.ModelID, "request")
	wantOptionsOnWire(t, rec, f, "request")
}

// testGenerate covers the map-the-response failure: the wire reply must arrive
// as a ModelResult whose fields mean what the reply said.
func testGenerate(t *testing.T, f Fixture) {
	ctx := context.Background()
	p, rec := serve(t, f, f.Reply)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	got, err := f.model(p).Generate(ctx, req)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	wantSentOnWire(t, rec, f.ModelID, "generate")
	wantOptionsOnWire(t, rec, f, "generate")
	wantResult(t, "generate", f, got)
}

// testStream covers the paths-disagree failure: the streamed reply carries the
// same logical response as the non-streamed one, so the assembled ModelResult
// must say the same thing.
func testStream(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyStream == nil {
		t.Skip("provider does not stream")
	}
	p, rec := serve(t, f, f.ReplyStream)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	stream, err := f.model(p).Stream(ctx, req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var parts int
	for range stream.Parts {
		parts++
	}
	got, err := stream.Result()
	if err != nil {
		t.Fatalf("stream result: %v", err)
	}
	if parts == 0 {
		t.Fatal("stream produced no parts")
	}
	if got == nil {
		t.Fatal("stream produced no result")
	}
	// Most providers encode the streaming request on a different path than the
	// non-streaming one (stream=true, different instruction and tool framing),
	// so the markers are checked again here rather than inferred from generate.
	// This must happen after the parts are drained: a provider sends the request
	// from the goroutine that produces them.
	wantSentOnWire(t, rec, f.ModelID, "stream")
	wantOptionsOnWire(t, rec, f, "stream")
	wantResult(t, "stream", f, *got)
}

// testError covers the swallow-the-error failure: a provider-shaped error reply
// must become an error rather than an empty success, and on both paths that
// error must be the *sdk.APIError the reply describes.
func testError(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyError == nil {
		t.Skip("provider has no error fixture")
	}
	var written capturedReply
	p, _ := serve(t, f, written.capture(f.ReplyError))
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	result, err := f.model(p).Generate(ctx, req)
	if err == nil {
		t.Fatalf("an error reply mapped to a success: %+v", result)
	}
	wantAPIError(t, "generate", f, f.WantError, err, &written)
	if f.ReplyStream == nil {
		return
	}
	err = streamErr(t, f, p, req)
	if err == nil {
		t.Fatal("stream: an error reply mapped to a success")
	}
	wantAPIError(t, "stream", f, f.WantError, err, &written)
}

// testInBandError covers a failure reported after a 2xx status line: in the
// body of a non-streaming reply, or in an error event of a stream that already
// started. Both must surface as the *sdk.APIError the reply describes, with no
// status code.
func testInBandError(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyErrorBody == nil && f.ReplyErrorEvent == nil {
		t.Skip("provider has no in-band error fixture")
	}
	if f.WantInBandError == nil {
		t.Fatal("fixture has an in-band error reply but no WantInBandError")
	}
	if f.ReplyErrorBody != nil {
		var written capturedReply
		p, _ := serve(t, f, written.capture(f.ReplyErrorBody))
		req := f.withOptions(p, request())
		req.Model = f.ModelID
		result, err := f.model(p).Generate(ctx, req)
		if err == nil {
			t.Fatalf("generate: an in-band error mapped to a success: %+v", result)
		}
		wantAPIError(t, "generate", f, f.WantInBandError, err, &written)
	}
	if f.ReplyErrorEvent != nil {
		var written capturedReply
		p, _ := serve(t, f, written.capture(f.ReplyErrorEvent))
		req := f.withOptions(p, request())
		req.Model = f.ModelID
		err := streamErr(t, f, p, req)
		if err == nil {
			t.Fatal("stream: an error event mapped to a success")
		}
		wantAPIError(t, "stream", f, f.WantInBandError, err, &written)
	}
}

// testStreamIncomplete covers the truncated-success failure: a stream that
// ends without its terminal event must fail with sdk.ErrStreamIncomplete
// rather than finish as if the response were complete.
func testStreamIncomplete(t *testing.T, f Fixture) {
	if f.ReplyStreamIncomplete == nil {
		t.Skip("provider has no incomplete stream fixture")
	}
	p, _ := serve(t, f, f.ReplyStreamIncomplete)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	if err := streamErr(t, f, p, req); !errors.Is(err, sdk.ErrStreamIncomplete) {
		t.Fatalf("stream error = %v, want sdk.ErrStreamIncomplete", err)
	}
}

// testStreamMalformed covers an event the provider cannot decode. Every chat
// provider here streams server-sent events with JSON data, so an event whose
// data is not JSON is malformed in each wire format. It must fail the stream
// with one ErrorPart.
func testStreamMalformed(t *testing.T, f Fixture) {
	if f.ReplyStream == nil {
		t.Skip("provider does not stream")
	}
	p, _ := serve(t, f, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"providertest\": \n\n")
	})
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	if err := streamErr(t, f, p, req); err == nil {
		t.Fatal("a malformed event mapped to a success")
	}
}

// streamErr streams req and returns the stream's error. A stream fails with
// one ErrorPart, and the FinishPart that closes it must say that it failed.
func streamErr(t *testing.T, f Fixture, p sdk.Provider, req sdk.Request) error {
	t.Helper()
	stream, err := f.model(p).Stream(context.Background(), req)
	if err != nil {
		return err
	}
	var failures []error
	for part := range stream.Parts {
		switch part := part.(type) {
		case *sdk.ErrorPart:
			failures = append(failures, part.Error)
		case *sdk.FinishPart:
			if len(failures) > 0 && part.FinishReason != sdk.FinishReasonError {
				t.Errorf("stream: FinishPart after an ErrorPart has finish reason %q, want %q", part.FinishReason, sdk.FinishReasonError)
			}
		}
	}
	_, err = stream.Result()
	if len(failures) > 1 {
		t.Errorf("stream: %d ErrorParts, want at most one: %v", len(failures), failures)
	}
	if err != nil && len(failures) == 0 {
		t.Errorf("stream: failed with %v but sent no ErrorPart", err)
	}
	return err
}

// wantAPIError compares err with want. A reply with a status carries the
// error as its whole body; a failure reported after a 2xx status line carries
// it inside what the handler wrote.
func wantAPIError(t *testing.T, op string, f Fixture, want *sdk.APIError, err error, written *capturedReply) {
	t.Helper()
	if want == nil {
		return
	}
	var got *sdk.APIError
	if !errors.As(err, &got) {
		t.Fatalf("%s: error %q (%T) does not unwrap to *sdk.APIError", op, err, err)
	}
	for _, c := range []struct {
		field     string
		got, want any
	}{
		{"Provider", got.Provider, want.Provider},
		{"StatusCode", got.StatusCode, want.StatusCode},
		{"Type", got.Type, want.Type},
		{"Code", got.Code, want.Code},
		{"Message", got.Message, want.Message},
		{"RequestID", got.RequestID, want.RequestID},
		{"Kind", got.Kind, want.Kind},
	} {
		if c.got != c.want {
			t.Errorf("%s: APIError.%s = %v, want %v", op, c.field, c.got, c.want)
		}
	}
	if kind := sdk.KindOf(err); kind != want.Kind {
		t.Errorf("%s: KindOf = %q, want %q", op, kind, want.Kind)
	}
	body, header := written.last()
	if want.StatusCode == 0 {
		if len(got.Body) == 0 || !bytes.Contains(body, got.Body) {
			t.Errorf("%s: APIError.Body = %q, want the error found in the reply %q", op, got.Body, body)
		}
	} else if !bytes.Equal(got.Body, body) {
		t.Errorf("%s: APIError.Body = %q, want the reply body %q", op, got.Body, body)
	}
	for name := range header {
		if got.Header.Get(name) != header.Get(name) {
			t.Errorf("%s: APIError.Header[%s] = %q, want %q", op, name, got.Header.Get(name), header.Get(name))
		}
	}
	for _, text := range []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err)} {
		if f.Secret != "" && strings.Contains(text, f.Secret) {
			t.Errorf("%s: error text %q contains the credential", op, text)
		}
		if strings.Contains(text, string(body)) || (len(got.Body) > 0 && strings.Contains(text, string(got.Body))) {
			t.Errorf("%s: error text %q contains the raw reply body", op, text)
		}
	}
}

// capturedReply records what a handler wrote, so the suite can compare the
// APIError against the reply itself rather than against a copy in the fixture.
type capturedReply struct {
	mu     sync.Mutex
	body   []byte
	header http.Header
}

func (c *capturedReply) capture(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		next(rec, r)
		c.mu.Lock()
		c.body = rec.Body.Bytes()
		c.header = rec.Header().Clone()
		c.mu.Unlock()
		for name, values := range rec.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}
}

func (c *capturedReply) last() ([]byte, http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.body, c.header
}

// wantResult asserts the provider-neutral meaning of a result, on the same
// substantive fields for both paths.
func wantResult(t *testing.T, op string, f Fixture, got sdk.ModelResult) {
	t.Helper()
	if want := f.Want.Text; got.Text != want {
		t.Errorf("%s: text = %q, want %q", op, got.Text, want)
	}
	if want := f.Want.Reasoning; got.Reasoning != want {
		t.Errorf("%s: reasoning = %q, want %q", op, got.Reasoning, want)
	}
	if !f.Caps.NoFinishReason {
		if want := f.Want.FinishReason; got.FinishReason != want {
			t.Errorf("%s: finish reason = %q, want %q", op, got.FinishReason, want)
		}
	}
	if !f.Caps.NoUsage && f.Want.TotalTokens != 0 && got.Usage.TotalTokens != f.Want.TotalTokens {
		t.Errorf("%s: total tokens = %d, want %d", op, got.Usage.TotalTokens, f.Want.TotalTokens)
	}
	if len(got.ToolCalls) != len(f.Want.ToolCalls) {
		t.Fatalf("%s: %d tool calls, want %d (%+v)", op, len(got.ToolCalls), len(f.Want.ToolCalls), got.ToolCalls)
	}
	for i, want := range f.Want.ToolCalls {
		got := got.ToolCalls[i]
		if got.ToolName != want.ToolName {
			t.Errorf("%s: tool call %d name = %s, want %s", op, i, got.ToolName, want.ToolName)
		}
		// An empty ToolCallID means the wire format carries no id and the
		// provider must mint one, so only non-emptiness is required.
		if want.ToolCallID == "" {
			if got.ToolCallID == "" {
				t.Errorf("%s: tool call %d has no id, want a generated one", op, i)
			}
		} else if got.ToolCallID != want.ToolCallID {
			t.Errorf("%s: tool call %d id = %s, want %s", op, i, got.ToolCallID, want.ToolCallID)
		}
		if a, b := jsonOf(got.Input), jsonOf(want.Input); a != b {
			t.Errorf("%s: tool call %d input = %s, want %s", op, i, a, b)
		}
	}
	if !f.Want.Response.IsZero() {
		if jsonOf(got.Response) != jsonOf(f.Want.Response) {
			t.Errorf("%s: response metadata = %s, want %s", op, jsonOf(got.Response), jsonOf(f.Want.Response))
		}
	}
}

// testPathsAgree covers the fields wantResult does not pin to a fixture value:
// the same logical reply, generated and streamed, must carry the same response
// metadata, text metadata, raw finish reason, sources and reasoning tokens.
// The agent runtime digests whichever path an executor picked, so a field one
// path fills and the other leaves empty changes the digest with the transport.
func testPathsAgree(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyStream == nil {
		t.Skip("provider does not stream")
	}
	pg, _ := serve(t, f, f.Reply)
	req := f.withOptions(pg, request())
	req.Model = f.ModelID
	generated, err := f.model(pg).Generate(ctx, req)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	ps, _ := serve(t, f, f.ReplyStream)
	stream, err := f.model(ps).Stream(ctx, req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil {
		t.Fatalf("stream result: %v", err)
	}
	type reasoningShape struct {
		Format   sdk.ReasoningFormat
		Metadata sdk.ProviderMetadata
	}
	shapes := func(parts []sdk.ReasoningPart) []reasoningShape {
		out := make([]reasoningShape, len(parts))
		for i, p := range parts {
			out[i] = reasoningShape{Format: p.Format, Metadata: p.ProviderMetadata}
		}
		return out
	}
	checks := []struct {
		field    string
		got, exp any
	}{
		{"response metadata", streamed.Response, generated.Response},
		{"text provider metadata", streamed.TextProviderMetadata, generated.TextProviderMetadata},
		{"raw finish reason", streamed.RawFinishReason, generated.RawFinishReason},
		{"sources", streamed.Sources, generated.Sources},
		{"reasoning parts", shapes(streamed.ReasoningParts), shapes(generated.ReasoningParts)},
	}
	for _, c := range checks {
		if a, b := jsonOf(c.got), jsonOf(c.exp); a != b {
			t.Errorf("%s differs between paths:\n stream:   %s\n generate: %s", c.field, a, b)
		}
	}
}

func jsonOf(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(encoded)
}
