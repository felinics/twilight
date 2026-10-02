package module

import (
	"encoding/json"
	"testing"
)

// A version's wire form names its track: pre.N for a prerelease version, N
// for a stable one; a zero version is written by nothing and read as an
// error, and so is anything that is not a canonical decimal.
func TestPayloadVersionWire(t *testing.T) {
	cases := []struct {
		wire string
		want PayloadVersion
		ok   bool
	}{
		{`"pre.1"`, Pre(1), true},
		{`"pre.42"`, Pre(42), true},
		{`"1"`, Stable(1), true},
		{`"65535"`, Stable(65535), true},
		{`"0"`, PayloadVersion{}, false},
		{`"pre.0"`, PayloadVersion{}, false},
		{`"pre."`, PayloadVersion{}, false},
		{`"01"`, PayloadVersion{}, false},
		{`"65536"`, PayloadVersion{}, false},
		{`"v1"`, PayloadVersion{}, false},
		{`1`, PayloadVersion{}, false},
		{`""`, PayloadVersion{}, false},
	}
	for _, tc := range cases {
		var got PayloadVersion
		err := json.Unmarshal([]byte(tc.wire), &got)
		if (err == nil) != tc.ok || got != tc.want {
			t.Fatalf("unmarshal %s = %+v %v, want %+v ok=%v", tc.wire, got, err, tc.want, tc.ok)
		}
		if !tc.ok {
			continue
		}
		back, err := json.Marshal(got)
		if err != nil || string(back) != tc.wire {
			t.Fatalf("marshal %+v = %s %v, want %s", got, back, err, tc.wire)
		}
	}
	if _, err := json.Marshal(PayloadVersion{}); err == nil {
		t.Fatal("a zero version marshalled")
	}
}

// No version orders before every version, every prerelease version before
// every stable one, and within a track the number orders.
func TestPayloadVersionOrder(t *testing.T) {
	order := []PayloadVersion{{}, Pre(1), Pre(2), Pre(65535), Stable(1), Stable(2)}
	for i := range order {
		for j := range order {
			if got, want := order[i].Less(order[j]), i < j; got != want {
				t.Fatalf("%s < %s = %v, want %v", order[i], order[j], got, want)
			}
		}
	}
}
