package sdk

import (
	"encoding/json"
	"strings"
	"testing"
)

// CanonicalProviderOptions re-encodes every value and leaves the input alone;
// a value that is not a JSON document names its namespace in the error.
func TestCanonicalProviderOptions(t *testing.T) {
	in := map[string]json.RawMessage{"mine": json.RawMessage(` {"top_p": 0.9, "n": 1} `)}
	out, err := CanonicalProviderOptions(in)
	if err != nil || string(out["mine"]) != `{"n":1,"top_p":0.9}` || string(in["mine"]) != ` {"top_p": 0.9, "n": 1} ` {
		t.Fatalf("CanonicalProviderOptions = %s %v; input now %s", out["mine"], err, in["mine"])
	}
	if got, err := CanonicalProviderOptions(nil); err != nil || got != nil {
		t.Fatalf("nil options = %v %v", got, err)
	}
	if _, err := CanonicalProviderOptions(map[string]json.RawMessage{"bad": json.RawMessage(`{`)}); err == nil || !strings.Contains(err.Error(), `"bad"`) {
		t.Fatalf("invalid value = %v, want an error naming the namespace", err)
	}
}

// TestApplyProviderOptions pins the contract both sides rely on: options are
// keyed by provider namespace, they override what the provider built, and a
// member the provider's wire request does not know is an error rather than a
// silent no-op.
func TestApplyProviderOptions(t *testing.T) {
	type wireRequest struct {
		Model       string  `json:"model"`
		Temperature float64 `json:"temperature"`
	}
	build := func() *wireRequest { return &wireRequest{Model: "m-1"} }

	// No options, and options addressed to someone else, are both a no-op.
	for name, options := range map[string]map[string]json.RawMessage{
		"none":  nil,
		"other": {"other": json.RawMessage(`{"temperature":0.9}`)},
	} {
		wire := build()
		if err := ApplyProviderOptions("mine", options, wire); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if wire.Temperature != 0 {
			t.Fatalf("%s: another provider's options reached this wire request: %+v", name, wire)
		}
	}

	// A member overrides the field the provider set and leaves the rest alone.
	wire := build()
	options := map[string]json.RawMessage{"mine": json.RawMessage(`{"temperature":0.42}`)}
	if err := ApplyProviderOptions("mine", options, wire); err != nil {
		t.Fatal(err)
	}
	if wire.Temperature != 0.42 || wire.Model != "m-1" {
		t.Fatalf("wire = %+v", wire)
	}

	// A misspelled member is loud, because an option that is quietly dropped is
	// indistinguishable from one that was never set.
	options = map[string]json.RawMessage{"mine": json.RawMessage(`{"temperatur":0.42}`)}
	if err := ApplyProviderOptions("mine", options, build()); err == nil {
		t.Fatal("a misspelled provider option was silently ignored")
	}
}
