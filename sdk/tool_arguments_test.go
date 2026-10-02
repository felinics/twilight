package sdk

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseToolArguments(t *testing.T) {
	cases := []struct {
		name       string
		text       string
		wantValid  bool
		wantString string // String(); the verbatim text when invalid
		wantObject string // Object(); {} when invalid
	}{
		{"empty text is the empty object", "", true, "{}", "{}"},
		{"document is canonical", " {\n \"city\" : \"Tokyo\" }\n", true, `{"city":"Tokyo"}`, `{"city":"Tokyo"}`},
		{"members are sorted, numbers take their binary64 form", `{"z": 1e2, "a": {"d": 2.0, "c": 3}}`, true,
			`{"a":{"c":3,"d":2},"z":100}`, `{"a":{"c":3,"d":2},"z":100}`},
		{"repeated member is kept as text", `{"a":1,"a":2}`, false, `{"a":1,"a":2}`, "{}"},
		{"truncated document is kept as text", `{"city": "Tok`, false, `{"city": "Tok`, "{}"},
		{"prose is kept as text", "call the tool please", false, "call the tool please", "{}"},
		{"invalid utf-8 is kept as text", "{\"a\":\"\xff\"}", false, "{\"a\":\"\xff\"}", "{}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := ParseToolArguments(tc.text)
			if a.Valid() != tc.wantValid {
				t.Fatalf("Valid() = %v, want %v (%+v)", a.Valid(), tc.wantValid, a)
			}
			if got := a.String(); got != tc.wantString {
				t.Fatalf("String() = %q, want %q", got, tc.wantString)
			}
			if got := string(a.Object()); got != tc.wantObject {
				t.Fatalf("Object() = %s, want %s", got, tc.wantObject)
			}
			var decoded map[string]any
			err := a.Unmarshal(&decoded)
			if tc.wantValid && err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !tc.wantValid && !errors.Is(err, ErrInvalidToolArguments) {
				t.Fatalf("Unmarshal of invalid arguments = %v, want ErrInvalidToolArguments", err)
			}
		})
	}
}

func TestToolArgumentsJSON(t *testing.T) {
	a, err := ToolArgumentsJSON(struct {
		City  string          `json:"city"`
		Extra json.RawMessage `json:"extra"`
	}{City: "Tokyo", Extra: json.RawMessage(`{ "b":1, "a":2 }`)})
	if err != nil || !a.Valid() || a.String() != `{"city":"Tokyo","extra":{"a":2,"b":1}}` {
		t.Fatalf("ToolArgumentsJSON = %+v, %v", a, err)
	}
	if _, err := ToolArgumentsJSON(make(chan int)); err == nil {
		t.Fatal("an unencodable value was accepted")
	}
	if !(ToolArguments{}).IsZero() || (ToolArguments{Text: "x"}).IsZero() || a.IsZero() {
		t.Fatal("IsZero disagrees with the fields")
	}
}
