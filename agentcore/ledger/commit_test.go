package ledger

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

func oneEventBatch(stream Domain, typ, payload string) EventBatch {
	return EventBatch{Domain: stream, Events: []Event{
		{Type: EventType(typ), RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(payload)},
	}}
}

func TestValidateStreamRef(t *testing.T) {
	cases := []struct {
		name string
		ref  Domain
		want string // error substring; "" means valid
	}{
		{"singleton stream", Domain{Name: "chat"}, ""},
		{"keyed stream", Domain{Name: "run", Id: "r7"}, ""},
		{"empty domain", Domain{Id: "r7"}, "stream domain is empty"},
		{"domain with separator", Domain{Name: "run/r7"}, `contains "/"`},
		{"invalid UTF-8 domain", Domain{Name: string([]byte{0xff})}, "not valid UTF-8"},
		{"invalid UTF-8 ID", Domain{Name: "run", Id: string([]byte{0xff})}, "not valid UTF-8"},
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
	chat := Domain{Name: "chat"}
	run := Domain{Name: "run", Id: "r7"}
	cases := []struct {
		name    string
		batches []EventBatch
		want    string
	}{
		{"one batch", []EventBatch{oneEventBatch(chat, "twilight/x/a", `{"a":1}`)}, ""},
		{"two streams in one commit", []EventBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(run, "twilight/run/created", `{"runId":"r7"}`),
		}, ""},
		{"no batches", nil, "without batches"},
		{"batch without events", []EventBatch{{Domain: chat}}, "no events"},
		{"same stream twice in one commit", []EventBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(chat, "twilight/x/b", `{"b":2}`),
		}, "appears twice"},
		{"stream domain with separator", []EventBatch{
			oneEventBatch(Domain{Name: "run/r7"}, "twilight/run/created", `{"runId":"r7"}`),
		}, `contains "/"`},
		{"empty event type", []EventBatch{{Domain: chat, Events: []Event{
			{RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}}}, "EventType"},
		{"array payload", []EventBatch{oneEventBatch(chat, "twilight/x/a", `[1]`)}, "object"},
		{"zero payload", []EventBatch{{Domain: chat, Events: []Event{
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
	batches := []EventBatch{
		oneEventBatch(Domain{Name: "chat"}, "twilight/x/a", `{"a":1}`),
		oneEventBatch(Domain{Name: "run", Id: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
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
			err := tc.c.Validate()
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

func TestProposalAt(t *testing.T) {
	batch := oneEventBatch(Domain{Name: "chat"}, "twilight/x/a", `{"a":1}`)
	got := Proposal{CommitID: "c1", Batches: []EventBatch{batch}}.At(4)
	if got.Seq != 4 || got.CommitID != "c1" || len(got.Batches) != 1 {
		t.Fatalf("Proposal.At = %+v", got)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
}
