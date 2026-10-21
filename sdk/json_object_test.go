package sdk_test

import (
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/sdk"
)

// Keys chosen so that sorting them changes the order: sorted they come out
// account, billing, urgent.
func TestJSONObject_MarshalPreservesOrder(t *testing.T) {
	obj := sdk.JSONObject{}.
		Set("urgent", true).
		Set("billing", "payments").
		Set("account", 3)

	got, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"urgent":true,"billing":"payments","account":3}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// The same data in a map is what this type exists to avoid: encoding/json
	// sorts map keys, so the object reaches the model in a different order.
	viaMap, err := json.Marshal(map[string]any{"urgent": true, "billing": "payments", "account": 3})
	if err != nil {
		t.Fatalf("Marshal map: %v", err)
	}
	if string(viaMap) == want {
		t.Fatal("a map preserved insertion order; this test no longer proves anything")
	}
}

func TestJSONObject_MarshalNested(t *testing.T) {
	obj := sdk.JSONObject{}.
		Set("ticket", sdk.JSONObject{}.Set("subject", "Duplicate charge").Set("author", "customer")).
		Set("order", sdk.JSONObject{}.Set("id", "A-104").Set("amount_usd", 49))

	got, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"ticket":{"subject":"Duplicate charge","author":"customer"},"order":{"id":"A-104","amount_usd":49}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestJSONObject_MarshalStructMemberKeepsFieldOrder(t *testing.T) {
	// A struct already marshals in field-declaration order, so it needs no
	// special handling and composes with JSONObject.
	type order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount_usd"`
	}
	got, err := json.Marshal(sdk.JSONObject{}.Set("order", order{ID: "A-104", Amount: 49}))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"order":{"id":"A-104","amount_usd":49}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestJSONObject_MarshalDuplicateKey(t *testing.T) {
	_, err := json.Marshal(sdk.JSONObject{}.Set("billing", "a").Set("billing", "b"))
	if err == nil {
		t.Fatal("expected an error for a duplicate key")
	}
}

func TestJSONObject_MarshalNil(t *testing.T) {
	got, err := json.Marshal(sdk.JSONObject(nil))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(got) != "null" {
		t.Errorf("got %s, want null", got)
	}
}

func TestJSONObject_RoundTrip(t *testing.T) {
	const input = `{"urgent":true,"nested":{"z":1,"a":[2,{"y":3,"b":null}]},"big":10000000000000000001}`

	var obj sdk.JSONObject
	if err := json.Unmarshal([]byte(input), &obj); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if keys := obj.Keys(); len(keys) != 3 || keys[0] != "urgent" || keys[1] != "nested" || keys[2] != "big" {
		t.Errorf("keys = %v, want [urgent nested big]", keys)
	}

	got, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(got) != input {
		t.Errorf("round trip changed the document:\n got %s\nwant %s", got, input)
	}
}

func TestJSONObject_UnmarshalRejectsNonObject(t *testing.T) {
	for _, input := range []string{`[1,2]`, `"text"`, `7`} {
		var obj sdk.JSONObject
		if err := json.Unmarshal([]byte(input), &obj); err == nil {
			t.Errorf("Unmarshal(%s): expected an error", input)
		}
	}
}

func TestJSONObject_Get(t *testing.T) {
	obj := sdk.JSONObject{}.Set("a", 1)
	if v, ok := obj.Get("a"); !ok || v != 1 {
		t.Errorf("Get(a) = %v, %v; want 1, true", v, ok)
	}
	if _, ok := obj.Get("missing"); ok {
		t.Error("Get(missing) reported a hit")
	}
}
