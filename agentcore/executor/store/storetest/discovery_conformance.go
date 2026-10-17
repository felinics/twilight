package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// Directory is optional. Every adapter that exposes it runs this scenario
// through Run, including a second handle when the adapter can reopen.
func testDirectory(t *testing.T, f Fixture) {
	ctx := context.Background()
	d, ok := f.Store.(executionstore.Directory)
	if !ok {
		t.Skip("adapter has no optional execution Directory")
	}
	s, ok := f.Store.(Seeder)
	if !ok {
		t.Fatal("Directory conformance requires Seeder")
	}
	// This order is explicitly the tuple order. Slash-containing identities
	// catch implementations that sort a delimiter-concatenated string.
	rows := []struct {
		key    effect.AssignmentKey
		status effect.ExecutionStatus
	}{
		{effect.AssignmentKey{Session: "a", OwnerKind: effect.OwnerCompaction, OwnerID: "r", Effect: "z"}, effect.ExecutionAccepted},
		{effect.AssignmentKey{Session: "a", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "z"}, effect.ExecutionAccepted},
		{effect.AssignmentKey{Session: "a", OwnerKind: effect.OwnerTurn, OwnerID: string("r/"), Effect: "a"}, effect.ExecutionDispatching},
		{effect.AssignmentKey{Session: "a/", OwnerKind: effect.OwnerTurn, OwnerID: string("a"), Effect: "a"}, effect.ExecutionRunning},
		{effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "a"}, effect.ExecutionCancelRequested},
		{effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "b"}, effect.ExecutionCompleted},
		{effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "c"}, effect.ExecutionFailed},
		{effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "d"}, effect.ExecutionCancelled},
		{effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "e"}, effect.ExecutionUnknown},
	}
	want := make([]effect.ExecutionSummary, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		a := assignment(row.key.Effect)
		a.Session, a.OwnerKind, a.OwnerID = row.key.Session, row.key.OwnerKind, row.key.OwnerID
		state := executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: a, State: row.status}}
		if row.status == effect.ExecutionRunning {
			state.Lease = executionstore.Lease{Key: row.key, Owner: "expired-worker", Epoch: 3, UntilUnixMilli: 1}
		}
		if err := s.Seed(ctx, state); err != nil {
			t.Fatal(err)
		}
		_, head, found, err := f.Store.Load(ctx, row.key)
		if err != nil || !found {
			t.Fatalf("load seeded row: found=%v err=%v", found, err)
		}
		want[i] = effect.ExecutionSummary{Key: row.key, Status: row.status, Revision: uint64(head.Next), Owner: state.Lease.Owner, LeaseUntilUnixMilli: state.Lease.UntilUnixMilli}
	}
	for _, excluded := range []struct {
		id     string
		status effect.ExecutionStatus
		acked  bool
	}{{"aa-aborted", effect.ExecutionAborted, false}, {"bb-acked", effect.ExecutionCompleted, true}} {
		a := assignment(turn.EffectID(excluded.id))
		a.Session = "b"
		if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: a, State: excluded.status, Acknowledged: excluded.acked}}); err != nil {
			t.Fatal(err)
		}
	}
	if f.Reopen != nil {
		other := f.Reopen(t)
		d, ok = other.(executionstore.Directory)
		if !ok {
			t.Fatal("reopened store lost Directory capability")
		}
	}
	checkDirectoryPagination(t, d, want)
	// after need not still exist (or ever have existed).
	after := effect.AssignmentKey{Session: "b", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), Effect: "c-between"}
	page, err := d.ListOutstanding(ctx, after, 1)
	if err != nil || len(page.Items) != 1 || page.Items[0].Key != rows[7].key || !page.More {
		t.Fatalf("exclusive absent cursor=%+v err=%v", page, err)
	}
	for _, limit := range []int{0, -1} {
		if _, err := d.ListOutstanding(ctx, effect.AssignmentKey{}, limit); !errors.Is(err, effect.ErrInvalidOutstandingLimit) {
			t.Fatalf("limit=%d err=%v", limit, err)
		}
	}
	// Renew changes only lease metadata, not the ledger revision.
	key := rows[0].key
	lease, held, err := f.Store.Acquire(ctx, key, "live-worker", time.Minute)
	if err != nil || !held {
		t.Fatalf("acquire=%v err=%v", held, err)
	}
	_, head, _, err := f.Store.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Store.Renew(ctx, lease, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	page, err = d.ListOutstanding(ctx, effect.AssignmentKey{}, 1)
	current, _, _, loadErr := f.Store.Load(ctx, key)
	if err != nil || loadErr != nil || len(page.Items) != 1 || page.Items[0].Revision != uint64(head.Next) || page.Items[0].Owner != lease.Owner || page.Items[0].LeaseUntilUnixMilli != current.Lease.UntilUnixMilli {
		t.Fatalf("renew summary=%+v err=%v loadErr=%v", page, err, loadErr)
	}
}

func checkDirectoryPagination(t *testing.T, d executionstore.Directory, want []effect.ExecutionSummary) {
	t.Helper()
	ctx := context.Background()
	for size := 1; size <= len(want)+1; size++ {
		var after effect.AssignmentKey
		var got []effect.ExecutionSummary
		for pages := 0; ; pages++ {
			if pages > len(want) {
				t.Fatal("pagination failed to terminate")
			}
			page, err := d.ListOutstanding(ctx, after, size)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Items) == 0 || len(page.Items) > size || page.Next != page.Items[len(page.Items)-1].Key {
				t.Fatalf("invalid page size=%d: %+v", size, page)
			}
			got = append(got, page.Items...)
			if page.More != (len(got) < len(want)) {
				t.Fatalf("More lost lookahead: size=%d page=%+v", size, page)
			}
			after = page.Next
			if !page.More {
				break
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("size=%d scan=%+v want=%+v", size, got, want)
		}
		page, err := d.ListOutstanding(ctx, after, size)
		if err != nil || len(page.Items) != 0 || page.More || page.Next != after {
			t.Fatalf("end page=%+v err=%v", page, err)
		}
	}
}
