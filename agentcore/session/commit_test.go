package session

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// rootHeader builds a root segment header with identity id.
func rootHeader(id string) SegmentHeader {
	return SegmentHeader{ID: SegmentID(id)}
}

func oneEventBatch(stream StreamRef, typ, payload string) StreamBatch {
	return StreamBatch{Stream: stream, Events: []Event{
		{Type: EventType(typ), RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(payload)},
	}}
}

func TestValidateStreamRef(t *testing.T) {
	cases := []struct {
		name string
		ref  StreamRef
		want string // error substring; "" means valid
	}{
		{"singleton stream", StreamRef{Domain: "chat"}, ""},
		{"keyed stream", StreamRef{Domain: "run", ID: "r7"}, ""},
		{"empty domain", StreamRef{ID: "r7"}, "stream domain is empty"},
		{"domain with separator", StreamRef{Domain: "run/r7"}, `contains "/"`},
		{"invalid UTF-8 domain", StreamRef{Domain: string([]byte{0xff})}, "not valid UTF-8"},
		{"invalid UTF-8 ID", StreamRef{Domain: "run", ID: string([]byte{0xff})}, "not valid UTF-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStreamRef(tc.ref)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ValidateStreamRef(%v) = %v", tc.ref, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateStreamRef(%v) = %v, want substring %q", tc.ref, err, tc.want)
			}
		})
	}
}

func TestValidateBatches(t *testing.T) {
	chat := StreamRef{Domain: "chat"}
	run := StreamRef{Domain: "run", ID: "r7"}
	cases := []struct {
		name    string
		batches []StreamBatch
		want    string
	}{
		{"one batch", []StreamBatch{oneEventBatch(chat, "twilight/x/a", `{"a":1}`)}, ""},
		{"two streams in one commit", []StreamBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(run, "twilight/run/created", `{"runId":"r7"}`),
		}, ""},
		{"no batches", nil, "without batches"},
		{"batch without events", []StreamBatch{{Stream: chat}}, "no events"},
		{"same stream twice in one commit", []StreamBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(chat, "twilight/x/b", `{"b":2}`),
		}, "appears twice"},
		{"stream domain with separator", []StreamBatch{
			oneEventBatch(StreamRef{Domain: "run/r7"}, "twilight/run/created", `{"runId":"r7"}`),
		}, `contains "/"`},
		{"empty event type", []StreamBatch{{Stream: chat, Events: []Event{
			{RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}}}, "EventType"},
		{"array payload", []StreamBatch{oneEventBatch(chat, "twilight/x/a", `[1]`)}, "object"},
		{"zero payload", []StreamBatch{{Stream: chat, Events: []Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1},
		}}}, "empty payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBatches(tc.batches)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ValidateBatches = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateBatches = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidateCommit(t *testing.T) {
	batches := []StreamBatch{
		oneEventBatch(StreamRef{Domain: "chat"}, "twilight/x/a", `{"a":1}`),
		oneEventBatch(StreamRef{Domain: "run", ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
	}
	cases := []struct {
		name string
		c    Commit
		want string // error substring; "" means valid
	}{
		{"well formed", Commit{CommitID: "c1", Batches: batches}, ""},
		{"empty CommitID", Commit{Batches: batches}, "CommitID is empty"},
		{"no batches", Commit{CommitID: "c2"}, "commit without batches"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCommit(&tc.c)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateCommit = %v, want %q", err, tc.want)
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
		{"root", rootHeader("seg"), true},
		{"child", SegmentHeader{ID: "child", Parent: &LedgerRef{Segment: "seg", Seq: 3}}, true},
		{"missing id", SegmentHeader{}, false},
		{"edge without segment", SegmentHeader{ID: "child", Parent: &LedgerRef{Seq: 3}}, false},
		{"extension key without ID", SegmentHeader{ID: "seg", Ext: Extensions{{Source: "twilight"}: RawValue(`1`)}}, false},
		{"extension is not JSON", SegmentHeader{ID: "seg", Ext: Extensions{{Source: "twilight", ID: "run"}: RawValue(`{`)}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHeader(tc.h)
			if (err == nil) != tc.ok {
				t.Fatalf("ValidateHeader = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !IsCode(err, ErrInvalid) {
				t.Fatalf("ValidateHeader = %v, want code invalid", err)
			}
		})
	}
	if seed := LedgerSeed(rootHeader("seg")); seed != (Head{}) {
		t.Fatalf("root seed = %+v", seed)
	}
	if seed := LedgerSeed(SegmentHeader{ID: "c", Parent: &LedgerRef{Segment: "seg", Seq: 3}}); seed != (Head{Next: 4}) {
		t.Fatalf("child seed = %+v", seed)
	}
}
