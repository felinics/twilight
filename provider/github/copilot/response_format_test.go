package copilot_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func TestResponseFormatWireShape(t *testing.T) {
	schema := &jsonschema.Schema{
		Type: "object",
		Properties: map[string]*jsonschema.Schema{
			"city": {Type: "string"},
		},
		Required: []string{"city"},
	}
	cases := []struct {
		name string
		rf   *sdk.ResponseFormat
		want string // "" means response_format must be omitted
	}{
		{name: "unset", want: ""},
		{name: "text", rf: &sdk.ResponseFormat{Type: sdk.ResponseFormatText}, want: `{"type":"text"}`},
		{name: "json_object", rf: &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONObject}, want: `{"type":"json_object"}`},
		{
			name: "json_schema wraps schema with name",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONSchema, JSONSchema: schema},
			want: `{"type":"json_schema","json_schema":{"name":"response","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`,
		},
		{
			name: "json_schema without schema degrades to json_object",
			rf:   &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONSchema},
			want: `{"type":"json_object"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]json.RawMessage
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request body: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":    "copilotcmpl-test",
					"model": "m",
					"choices": []map[string]any{{
						"index":         0,
						"finish_reason": "stop",
						"message":       map[string]any{"role": "assistant", "content": "{}"},
					}},
				})
			}))
			defer srv.Close()

			p := copilot.New(copilot.WithGitHubToken("ghu_test_token"), copilot.WithBaseURL(srv.URL))
			if _, err := p.DoGenerate(context.Background(), sdk.Request{
				Model:          "m",
				Messages:       []sdk.Message{sdk.UserMessage("Hi")},
				ResponseFormat: tc.rf,
			}); err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}
			got := body["response_format"]
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
