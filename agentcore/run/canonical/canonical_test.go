package canonical

import (
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/wire"
	"github.com/felinics/twilight/sdk"
)

func TestFreezeToolArgumentsBindingForm(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string // Canonical() of the frozen arguments; "" rejects
	}{
		{"document", `{ "x" : 1 }`, `{"x":1}`},
		{"empty is the empty object", "", `{}`},
		{"malformed text is quoted", `{"x":`, `"{\"x\":"`},
		{"invalid utf-8 is rejected", string([]byte{0xff}), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sdkconv.FreezeToolArguments(sdk.ParseToolArguments(tc.text))
			if tc.want == "" {
				if err == nil {
					t.Fatalf("FreezeToolArguments accepted %q", tc.text)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Canonical().String() != tc.want {
				t.Fatalf("Canonical() = %s, want %s", got.Canonical().String(), tc.want)
			}
		})
	}
}

// RFC 8785 appendix test vectors plus structural cases.
func TestCanonicalJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"key sort ascii", `{"b":1,"a":2}`, `{"a":2,"b":1}`},
		{"nested objects", `{"z":{"b":1,"a":[true,null]},"a":"x"}`, `{"a":"x","z":{"a":[true,null],"b":1}}`},
		{"whitespace stripped", "{\n  \"a\" : 1 ,\t\"b\": [ 1 , 2 ]\n}", `{"a":1,"b":[1,2]}`},
		// RFC 8785 §3.2.3: sort by UTF-16 code units — surrogate pairs (𝄞)
		// sort after BMP chars like € and 替.
		{"utf16 order", `{"𝄞":1,"€":2,"replace":3}`, `{"replace":3,"€":2,"𝄞":1}`},
		{"number integer", `{"a":1.0}`, `{"a":1}`},
		{"number negative zero", `{"a":-0}`, `{"a":0}`},
		{"number e-notation collapse", `{"a":1e+3}`, `{"a":1000}`},
		{"number small", `{"a":0.000001}`, `{"a":0.000001}`},
		{"number tiny goes exponential", `{"a":0.0000001}`, `{"a":1e-7}`},
		{"number large stays plain to 1e21", `{"a":100000000000000000000}`, `{"a":100000000000000000000}`},
		{"number 1e21 exponential", `{"a":1e21}`, `{"a":1e+21}`},
		{"number JSONB expanded 1e21", `{"a":1000000000000000000000}`, `{"a":1e+21}`},
		{"number shortest roundtrip", `{"a":0.1}`, `{"a":0.1}`},
		{"string escapes minimal", `{"a":"A\nB\u0041"}`, "{\"a\":\"A\\nBA\"}"},
		{"string control chars", `{"a":"\u0001"}`, "{\"a\":\"\\u0001\"}"},
		{"string unicode passthrough", `{"a":"\u00e9"}`, `{"a":"é"}`},
		{"string surrogate pair", `{"a":"\ud834\udd1e"}`, `{"a":"𝄞"}`},
		{"array order preserved", `[3,1,2]`, `[3,1,2]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := es.Canonicalize([]byte(c.in))
			if err != nil {
				t.Fatalf("es.Canonicalize(%q): %v", c.in, err)
			}
			if string(got) != c.want {
				t.Fatalf("es.Canonicalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestCanonicalJSONRejects(t *testing.T) {
	for _, in := range []string{
		``, `{"a":1}garbage`, `{bad}`,
		`"\ud800"`, `"\udbff"`, `"\udc00"`, `"\ud800x"`, `"\ud800\u0041"`,
		`{"a":1,"a":2}`, `{"dry_run":true,"dry_run":false}`,
		`{"x":1}]`, `{"a":1}}}`, `[1,2]]`,
		"{\"a\":\"\xff\"}",
	} {
		if _, err := es.Canonicalize([]byte(in)); err == nil {
			t.Fatalf("es.Canonicalize(%q): expected error", in)
		}
	}
}

func TestCanonicalDeterminism(t *testing.T) {
	// Map iteration order must not leak into canonical bytes.
	v := map[string]any{"z": 1, "a": map[string]any{"y": []any{1, "s"}, "b": true}, "m": nil}
	first, err := es.MarshalCanonical(v)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		got, err := es.MarshalCanonical(v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(first) {
			t.Fatalf("non-deterministic canonical output: %q vs %q", got, first)
		}
	}
}

func TestDigestPreimageCoversVersionPrefix(t *testing.T) {
	cmd := run.StartToolCall{StepID: "s1", CallID: "c1", Effect: "effect-1"}
	body1, err := es.EncodeTypedPayload(1, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	body2, err := es.EncodeTypedPayload(2, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if string(body1) == string(body2) {
		t.Fatal("schema version did not affect digest preimage")
	}
}

func TestDeriveStability(t *testing.T) {
	// Fixed inputs must produce fixed outputs across processes; freeze a few.
	id1 := (Identity{}).DeriveModelRequestCommandID("run-1", 7)
	id2 := (Identity{}).DeriveModelRequestCommandID("run-1", 7)
	if id1 != id2 {
		t.Fatal("derive is not deterministic")
	}
	if id1 == (Identity{}).DeriveModelRequestCommandID("run-1", 8) {
		t.Fatal("revision does not separate command IDs")
	}
	if id1 == (Identity{}).DeriveModelRequestCommandID("run-1", 70) {
		t.Fatal("index does not separate command IDs")
	}
	if id1 == (Identity{}).DeriveModelRequestCommandID("run-2", 7) {
		t.Fatal("run does not separate command IDs")
	}
	// Namespaces must not collide even with aligned parts.
	a := namespacedHash("twilight/model-step", "x", "y")
	b := namespacedHash("twilight/tool-step", "x", "y")
	if a == b {
		t.Fatal("namespace does not separate hashes")
	}
	// Length prefixing prevents concatenation collisions.
	c := namespacedHash("n", "ab", "c")
	d := namespacedHash("n", "a", "bc")
	if c == d {
		t.Fatal("part boundaries do not separate hashes")
	}
}

func TestDeriveResponseIDPerKind(t *testing.T) {
	a := (Identity{}).DeriveResponseID("r", "s", "c", run.ResponseApproval)
	b := (Identity{}).DeriveResponseID("r", "s", "c", run.ResponseExternal)
	if a == b {
		t.Fatal("response kind does not separate response IDs")
	}
}

// Golden vectors for the canonical encoding. They guard the persisted
// preimages; a change here is a change to every digest ever written.
func TestCanonicalGolden(t *testing.T) {
	cmd := run.CancelRun{Reason: run.ReasonCancelled}
	body, err := es.EncodeTypedPayload(1, "cancel_run", cmd)
	if err != nil {
		t.Fatal(err)
	}
	wantBody := `v1:10:cancel_run:{"reason":"cancelled"}`
	if string(body) != wantBody {
		t.Fatalf("golden body changed:\n got %q\nwant %q", body, wantBody)
	}

	fact := run.InputAccepted{Input: run.AgentInput{ID: "in-1", Digest: "sha256:e7b995efa755c5ff3b84d2188b58cb4ae916a59470eb3761df8a814f11763500"}}
	fbody, err := (wire.Facts{}).EncodeFact("input_accepted", fact)
	if err != nil {
		t.Fatal(err)
	}
	// The input body is not in the fact (RUN-WIR-4): the Run records the
	// input's identity and content digest, the chatlog holds the body.
	wantFact := `v1:14:input_accepted:{"input":{"digest":"sha256:e7b995efa755c5ff3b84d2188b58cb4ae916a59470eb3761df8a814f11763500","id":"in-1"}}`
	if string(fbody) != wantFact {
		t.Fatalf("golden fact body changed:\n got %q\nwant %q", fbody, wantFact)
	}
}
