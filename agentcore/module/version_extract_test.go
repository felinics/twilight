package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

func legacySplitVersionReference(payload jsonstable.Value) (jsonstable.Value, PayloadVersion, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload.Bytes(), &m); err != nil {
		return jsonstable.Value{}, 0, fmt.Errorf("payload is not an object: %w", err)
	}
	raw, ok := m["v"]
	if !ok {
		return jsonstable.Value{}, 0, errors.New("payload has no \"v\" field")
	}
	var v uint16
	if err := json.Unmarshal(raw, &v); err != nil || v == 0 {
		return jsonstable.Value{}, 0, errors.New("payload \"v\" is not a positive integer")
	}
	delete(m, "v")
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return jsonstable.Value{}, 0, err
	}
	return body, PayloadVersion(v), nil
}

func compareSplitVersion(t testing.TB, value jsonstable.Value) {
	t.Helper()
	wantBody, wantVersion, wantErr := legacySplitVersionReference(value)
	body, version, err := splitVersion(value)
	if fmt.Sprint(err) != fmt.Sprint(wantErr) || version != wantVersion || !body.Equal(wantBody) || body.IsZero() != wantBody.IsZero() {
		t.Fatalf("splitVersion(%s): (%s,%d,%v), want (%s,%d,%v)", value, body, version, err, wantBody, wantVersion, wantErr)
	}
}

func TestSplitVersionLegacyParity(t *testing.T) {
	for _, raw := range []string{
		`{"v":1}`, `{"v":2,"z":true}`, `{"a":{},"v":65535,"z":[]}`,
		`{"a":[{"v":99,"x":"}],[\\\""}],"\u0076":2,"😀":-0}`,
		`{"v":1e0}`, `{"v":0}`, `{"v":null}`, `{"v":-1}`, `{"v":1.1}`,
		`{"v":65536}`, `{"v":"2"}`, `{"v":[]}`, `{"v":{}}`, `{"v":true}`,
		`{"nested":{"v":1}}`, `{}`, `null`, `[]`, `"v"`, `true`, `123`,
	} {
		t.Run(raw, func(t *testing.T) { compareSplitVersion(t, jsonstable.MustParse(raw)) })
	}
	compareSplitVersion(t, jsonstable.Value{})
}

func FuzzSplitVersionLegacyParity(f *testing.F) {
	for _, raw := range []string{`{"v":1}`, `{"v":2,"body":[{},"v"]}`, `{"v":0}`, `null`, `[]`, `{}`} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		value, err := jsonstable.Parse([]byte(raw))
		if err != nil {
			return
		}
		compareSplitVersion(t, value)
	})
}

func BenchmarkSplitVersion(b *testing.B) {
	for _, size := range []int{32, 4096, 65536} {
		value, err := jsonstable.FromValue(map[string]any{"a": []any{true, nil, "\\\"{}[]"}, "text": strings.Repeat("x", size), "v": 2, "z": false})
		if err != nil {
			b.Fatal(err)
		}
		for _, impl := range []struct {
			name string
			fn   func(jsonstable.Value) (jsonstable.Value, PayloadVersion, error)
		}{{"legacy", legacySplitVersionReference}, {"extract", splitVersion}} {
			b.Run(fmt.Sprintf("%d/%s", size, impl.name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(value.String())))
				for b.Loop() {
					body, v, err := impl.fn(value)
					if err != nil || v != 2 || body.IsZero() {
						b.Fatalf("%d %v", v, err)
					}
				}
			})
		}
	}
}
