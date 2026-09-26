package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

// IndexCutter is the optional adapter capability that cuts the tip segment's
// persisted CommitIndex to its first keep entries while the commits stay,
// or removes it for a negative keep. It lets the suite prove that Open
// detects a lagging or absent index and repairs it (SES-REP-5). An adapter
// whose index cannot diverge from its commits (one transaction) need not
// implement it, and the case is then skipped.
type IndexCutter interface {
	CutIndex(session.SessionID, int) error
}

// SES-REP-5: the CommitIndex is a component of the segment. Open uses it as
// found when it passes the two checks against the head, rebuilds it from the
// commits otherwise, and either way the handle answers Committed, StreamHead,
// LookupCommit and Head exactly as from the commits.
func testIndex(t *testing.T, f Fixture) {
	cutter, ok := f.Store.(IndexCutter)
	if !ok {
		t.Skip("adapter's index cannot diverge from its commits")
	}
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	appendCommit(t, w, "c1", batch(chatStream(), "twilight/x/a", `{"a":1}`))
	appendCommit(t, w, "c2", batch(chatStream(), "twilight/x/b", `{"b":2}`), batch(runStream("r7"), "twilight/run/run_created", `{"runId":"r7"}`))
	c3 := appendCommit(t, w, "c3", batch(runStream("r7"), "twilight/run/x", `{"runId":"r7"}`))
	want := w.Head()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		keep int
	}{
		{name: "current", keep: 3},
		{name: "lagging by one", keep: 2},
		{name: "lagging by all", keep: 0},
		{name: "absent", keep: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := cutter.CutIndex("s", tc.keep); err != nil {
				t.Fatalf("cut index: %v", err)
			}
			w := open(t, f.Store, "s", false)
			defer func() { _ = w.Close(ctx) }()
			if got := w.Head(); got != want {
				t.Fatalf("head = %+v, want %+v", got, want)
			}
			for _, id := range []session.CommitID{"c1", "c2", "c3"} {
				if !w.Committed(id) {
					t.Fatalf("%s not committed after index cut", id)
				}
			}
			if w.Committed("absent") {
				t.Fatal("absent commit reported committed")
			}
			if n, ok := w.StreamHead(chatStream()); !ok || n != 2 {
				t.Fatalf("chat stream head = %d, %v; want 2", n, ok)
			}
			if n, ok := w.StreamHead(runStream("r7")); !ok || n != 2 {
				t.Fatalf("run stream head = %d, %v; want 2", n, ok)
			}
			if c, ok, err := w.LookupCommit("c3"); err != nil || !ok || c.Seq != c3.Seq {
				t.Fatalf("lookup c3 = %+v, %v, %v", c, ok, err)
			}
			if _, err := w.Append(ctx, session.Proposal{CommitID: "c2", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/z", `{}`)}}); !session.IsCode(err, session.ErrConflict) {
				t.Fatalf("duplicate CommitID after index cut = %v, want conflict", err)
			}
			page, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
			if err != nil || len(page.Commits) != 3 || page.Head != want {
				t.Fatalf("read after index cut = %d commits, head %+v, %v", len(page.Commits), page.Head, err)
			}
		})
	}
	// A fork's inherited membership is answered from the parent's index.
	if _, err := f.Store.Create(ctx, session.CreateRequest{SessionID: "child", CreatedAtUnixMilli: 2,
		Fork: &session.ForkOrigin{Session: "s", Seq: 1}}); err != nil {
		t.Fatal(err)
	}
	cw := open(t, f.Store, "child", false)
	defer func() { _ = cw.Close(ctx) }()
	if !cw.Committed("c1") || !cw.Committed("c2") || cw.Committed("c3") {
		t.Fatal("inherited membership: want c1, c2 inherited and c3 not")
	}
}
