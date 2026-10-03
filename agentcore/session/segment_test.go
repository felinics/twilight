package session

import (
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

// TestKernelExtSlots covers: header extension slots are keyed by
// module and hold JSON the kernel does not interpret; a malformed key or
// value is ErrInvalid.
func TestKernelExtSlots(t *testing.T) {
	run := module.ModuleKey{Source: "twilight", ID: "run"}
	cases := []struct {
		name    string
		ext     module.Extensions
		invalid bool
	}{
		{name: "absent"},
		{name: "object", ext: module.Extensions{run: module.RawValue(`{"k":1}`)}},
		{name: "scalar", ext: module.Extensions{run: module.RawValue(`1`)}},
		{name: "two modules", ext: module.Extensions{run: module.RawValue(`{}`), {Source: "acme", ID: "audit"}: module.RawValue(`[1]`)}},
		{name: "empty value", ext: module.Extensions{run: nil}, invalid: true},
		{name: "not JSON", ext: module.Extensions{run: module.RawValue(`{`)}, invalid: true},
		{name: "source with separator", ext: module.Extensions{{Source: "a/b", ID: "x"}: module.RawValue(`1`)}, invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			herr := (SegmentHeader{ID: "seg", Ext: tc.ext}).Validate()
			if tc.invalid {
				if herr == nil {
					t.Fatal("invalid ext accepted")
				}
				return
			}
			if herr != nil {
				t.Fatalf("valid ext rejected: %v", herr)
			}
		})
	}
}

func TestValidateHeader(t *testing.T) {
	cases := []struct {
		name string
		h    SegmentHeader
		ok   bool
	}{
		{"root", SegmentHeader{ID: "seg"}, true},
		{"child", SegmentHeader{ID: "child", Parent: &CommitRef{Segment: "seg", Seq: 3}}, true},
		{"missing id", SegmentHeader{}, false},
		{"edge without segment", SegmentHeader{ID: "child", Parent: &CommitRef{Seq: 3}}, false},
		{"extension key without ID", SegmentHeader{ID: "seg", Ext: module.Extensions{{Source: "twilight"}: module.RawValue(`1`)}}, false},
		{"extension is not JSON", SegmentHeader{ID: "seg", Ext: module.Extensions{{Source: "twilight", ID: "run"}: module.RawValue(`{`)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.h.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("ValidateHeader = %v, want ok=%v", err, tc.ok)
			}
		})
	}
	if seed := (SegmentHeader{ID: "seg"}).Seed(); seed != (ledger.Head{}) {
		t.Fatalf("root seed = %+v", seed)
	}
	if seed := (SegmentHeader{ID: "c", Parent: &CommitRef{Segment: "seg", Seq: 3}}).Seed(); seed != (ledger.Head{Next: 4}) {
		t.Fatalf("child seed = %+v", seed)
	}
}
