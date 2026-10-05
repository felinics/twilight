package sdk_test

import (
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/sdk"
)

func TestUsageAddPreservesCacheReporting(t *testing.T) {
	decode := func(raw string) sdk.Usage {
		t.Helper()
		var usage sdk.Usage
		if err := json.Unmarshal([]byte(raw), &usage); err != nil {
			t.Fatal(err)
		}
		return usage
	}
	known := decode(`{"inputTokens":100,"totalTokens":100,"cachedInputTokens":10,"inputTokenDetails":{"noCacheTokens":90,"cacheReadTokens":10},"cacheReadTokensReported":true}`)
	unknown := decode(`{"inputTokens":900,"totalTokens":900,"inputTokenDetails":{"noCacheTokens":900},"cacheReadTokensReported":false}`)
	for _, tt := range []struct {
		name        string
		left, right sdk.Usage
		input, read int
		reported    bool
	}{
		{name: "unknown zero first", right: known, input: 100, read: 10},
		{name: "unknown zero last", left: known, input: 100, read: 10},
		{name: "all reported", left: known, right: known, input: 200, read: 20, reported: true},
		{name: "mixed reporting", left: known, right: unknown, input: 1000, read: 10},
		{name: "unknown first", left: unknown, right: known, input: 1000, read: 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.left.Add(tt.right)
			if got.InputTokens != tt.input || got.InputTokenDetails.CacheReadTokens != tt.read {
				t.Fatalf("usage=%+v, want input=%d read=%d", got, tt.input, tt.read)
			}
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Reported *bool `json:"cacheReadTokensReported"`
			}
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.Reported == nil || *wire.Reported != tt.reported {
				t.Fatalf("cache reporting in %s, want explicit %t", raw, tt.reported)
			}
		})
	}
}
