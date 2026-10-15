package completions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func responseFormatTestSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type: "object",
		Properties: map[string]*jsonschema.Schema{
			"city": {Type: "string"},
		},
		Required: []string{"city"},
	}
}

// captureResponseFormat runs one DoGenerate or DoStream call against a mock
// server and returns the raw response_format bytes from the request body, or
// nil when the key was omitted.
func captureResponseFormat(t *testing.T, stream bool, rf *sdk.ResponseFormat, opts ...completions.Option) json.RawMessage {
	t.Helper()
	var body map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"{}\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-test",
			"model": "m",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "{}"},
			}},
		})
	}))
	defer srv.Close()

	p := completions.New(append([]completions.Option{
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	}, opts...)...)
	req := sdk.Request{
		Model:          "m",
		Messages:       []sdk.Message{sdk.UserMessage("Hi")},
		ResponseFormat: rf,
	}
	if stream {
		res, err := p.DoStream(context.Background(), req)
		if err != nil {
			t.Fatalf("DoStream: %v", err)
		}
		for range res {
		}
	} else if _, err := p.DoGenerate(context.Background(), req); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	return body["response_format"]
}

func TestResponseFormatWireShape(t *testing.T) {
	cases := []struct {
		name string
		rf   *sdk.ResponseFormat
		opts []completions.Option
		want string // "" means response_format must be omitted
	}{
		{
			name: "unset",
			rf:   nil,
			want: "",
		},
		{
			name: "text",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatText},
			want: `{"type":"text"}`,
		},
		{
			name: "json_object",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONObject},
			want: `{"type":"json_object"}`,
		},
		{
			name: "json_schema wraps schema with name",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONSchema, JSONSchema: responseFormatTestSchema()},
			want: `{"type":"json_schema","json_schema":{"name":"response","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`,
		},
		{
			name: "json_schema without schema degrades to json_object",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONSchema},
			want: `{"type":"json_object"}`,
		},
		{
			name: "json_schema under kimi compat is left as is",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONSchema, JSONSchema: responseFormatTestSchema()},
			opts: []completions.Option{completions.WithKimiChatCompletionsCompat()},
			want: `{"type":"json_schema","json_schema":{"name":"response","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`,
		},
	}
	for _, tc := range cases {
		for _, stream := range []bool{false, true} {
			name := tc.name + "/generate"
			if stream {
				name = tc.name + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				got := captureResponseFormat(t, stream, tc.rf, tc.opts...)
				if tc.want == "" {
					if got != nil {
						t.Fatalf("response_format should be omitted, got %s", got)
					}
					return
				}
				if string(got) != tc.want {
					t.Fatalf("response_format wire JSON mismatch\n got: %s\nwant: %s", got, tc.want)
				}
			})
		}
	}
}
