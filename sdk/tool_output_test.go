package sdk

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestToolOutput(t *testing.T) {
	structured, err := JSONOutput(map[string]any{"temp": 22})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := RawJSONOutput(json.RawMessage(` {"b": 2, "a":1} `))
	if err != nil {
		t.Fatal(err)
	}
	nested, err := JSONOutput(struct {
		Extra json.RawMessage `json:"extra"`
	}{Extra: json.RawMessage(`{"z":1, "y":2.0}`)})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		out        ToolOutput
		wantJSON   bool
		wantString string
	}{
		{"zero value is empty text", ToolOutput{}, false, ""},
		{"text", TextOutput("sunny"), false, "sunny"},
		{"encoded value", structured, true, `{"temp":22}`},
		{"raw document is canonical", raw, true, `{"a":1,"b":2}`},
		{"embedded raw JSON is canonical too", nested, true, `{"extra":{"y":2,"z":1}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.out.IsJSON() != tc.wantJSON || tc.out.String() != tc.wantString {
				t.Fatalf("IsJSON()=%v String()=%q, want %v %q", tc.out.IsJSON(), tc.out.String(), tc.wantJSON, tc.wantString)
			}
		})
	}
	if _, err := JSONOutput(make(chan int)); err == nil {
		t.Fatal("an unencodable value was accepted")
	}
	if _, err := RawJSONOutput(json.RawMessage(`{"a":`)); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("RawJSONOutput of a truncated document = %v, want ErrInvalidJSON", err)
	}
	buf := json.RawMessage(`{"a":1}`)
	out, err := RawJSONOutput(buf)
	if err != nil {
		t.Fatal(err)
	}
	buf[2] = 'b'
	if out.String() != `{"a":1}` {
		t.Fatal("RawJSONOutput aliased the caller's buffer")
	}
}
