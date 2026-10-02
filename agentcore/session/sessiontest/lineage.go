package sessiontest

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"testing"
)

// SES-GC-1/2: Sessions are roots into a forest of immutable segments. Delete
// tombstones a root, drops its path spans, and reclaims along that path: a
// segment stays through the greatest end any remaining path still names, and
// no remaining span removes it. Collect applies the same bounds and is a
// no-op once Delete has reclaimed.
//
//	A: a0 a1 a2 a3        B -> A@1        C -> A@2        D -> C@(c3)
func testLineage(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.Store
	headerA := create(t, store, "A")
	segA := headerA.ID
	aw := open(t, store, "A", false)
	var a []ledger.Commit
	for i := 0; i < 4; i++ {
		a = append(a, appendCommit(t, aw, "a"+string(rune('0'+i)), batch(chatStream(), "twilight/x/a", `{"n":`+string(rune('0'+i))+`}`)))
	}
	fork := func(child, parent session.SessionID, at ledger.Commit) session.SegmentHeader {
		t.Helper()
		h, err := forkAt(t, store, child, parent, at.Seq)
		if err != nil {
			t.Fatalf("fork %s: %v", child, err)
		}
		return h
	}
	headerB := fork("B", "A", a[1])
	headerC := fork("C", "A", a[2])
	cw := open(t, store, "C", false)
	c3 := appendCommit(t, cw, "c3", batch(chatStream(), "twilight/x/c", `{"n":3}`))
	_ = cw.Close(ctx)
	headerD := fork("D", "C", c3)
	segB, segC, segD := headerB.ID, headerC.ID, headerD.ID

	// Delete refuses an owned Session and an unknown one, and reclaims nothing
	// in either case.
	if _, err := store.Delete(ctx, "A"); !session.IsOwned(err) {
		t.Fatalf("delete owned = %v", err)
	}
	if _, err := store.Delete(ctx, "ghost"); !session.IsNotFound(err) {
		t.Fatalf("delete unknown = %v", err)
	}
	_ = aw.Close(ctx)
	// A's segment stays through the furthest remaining span (C and D at
	// a2) and drops a3. B, C and D are untouched.
	report, err := store.Delete(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 || report.Truncated[segA] != a[2].Seq+1 || len(report.Dropped[segA]) != 1 || report.Dropped[segA][0] != a[3].CommitID {
		t.Fatalf("delete A = %+v, want A's segment truncated after %d", report, a[2].Seq)
	}
	// A is no Session any more: not found, not openable, not forkable; a
	// second Delete is not found. Its identity is never reused (SES-GC-1):
	// a Create under the old name is ErrDeleted, so nothing addressed to
	// the old A can land on a new one.
	if _, err := store.Header(ctx, "A"); !session.IsNotFound(err) {
		t.Fatalf("header after delete = %v", err)
	}
	if _, err := store.Open(ctx, "A", session.OpenOptions{}); !session.IsNotFound(err) {
		t.Fatalf("open after delete = %v", err)
	}
	if _, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"}); !session.IsNotFound(err) {
		t.Fatalf("read after delete = %v", err)
	}
	if _, err := forkAt(t, store, "E", "A", a[0].Seq); !session.IsNotFound(err) {
		t.Fatalf("fork of a deleted session = %v", err)
	}
	if _, err := store.Delete(ctx, "A"); !session.IsNotFound(err) {
		t.Fatalf("second delete = %v", err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "A", CreatedAtUnixMilli: 9}); !session.IsDeleted(err) {
		t.Fatalf("create under a deleted id = %v, want deleted", err)
	}
	if _, err := store.Record(ctx, "A"); !session.IsNotFound(err) {
		t.Fatalf("record of a deleted session = %v, want not found", err)
	}
	// B, C and D still read their prefixes, and D through A's segment.
	for sid, want := range map[session.SessionID]string{"B": "a0,a1", "C": "a0,a1,a2,c3", "D": "a0,a1,a2,c3"} {
		page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if err != nil || ids(page.Commits) != want {
			t.Fatalf("%s after deleting A = %s %v, want %s", sid, ids(page.Commits), err, want)
		}
	}
	// Delete already reclaimed. Collect sees the same spans and changes nothing.
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("collect after deleting A = %+v %v, want nothing", report, err)
	}
	for sid, want := range map[session.SessionID]string{"B": "a0,a1", "C": "a0,a1,a2,c3", "D": "a0,a1,a2,c3"} {
		page, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if ids(page.Commits) != want {
			t.Fatalf("%s after collect = %s, want %s", sid, ids(page.Commits), want)
		}
	}
	// A live fork of an ownerless segment opens, looks up inherited commits
	// and appends as before.
	dw := open(t, store, "D", false)
	if !committed(t, dw, "a0") || committed(t, dw, "a3") {
		t.Fatal("D prefix membership wrong after collect")
	}
	if c, ok, _ := dw.LookupCommit("a2"); !ok || c.Seq != a[2].Seq {
		t.Fatalf("D lookup a2 = %+v %v", c, ok)
	}
	d4 := appendCommit(t, dw, "d4", batch(chatStream(), "twilight/x/d", `{"n":4}`))
	if d4.Seq != c3.Seq+1 {
		t.Fatalf("D own commit = %+v", d4)
	}
	_ = dw.Close(ctx)
	// Collect is idempotent.
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("second collect = %+v %v", report, err)
	}
	// A fresh Session is unrelated to the old segment. Deleted before any
	// commit, its empty segment is removed with the root.
	newA, err := store.Create(ctx, session.CreateRequest{SessionID: "A2", CreatedAtUnixMilli: 9})
	if err != nil {
		t.Fatalf("create A2: %v", err)
	}
	if newA.ID == segA {
		t.Fatal("A2 reused the old segment")
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A2"}); err != nil || len(page.Commits) != 0 {
		t.Fatalf("A2 = %+v %v", page, err)
	}
	report, err = store.Delete(ctx, "A2")
	if err != nil || len(report.Removed) != 1 || report.Removed[0] != newA.ID || len(report.Truncated) != 0 {
		t.Fatalf("delete A2 = %+v %v, want its empty segment removed", report, err)
	}
	// SES-GC-4: the adapter refuses to remove a node something still
	// reaches, whatever the caller computed. C's segment is D's parent and
	// B's is B's tip.
	if be, ok := store.(interface {
		RemoveSegment(context.Context, session.SegmentID) error
	}); ok {
		for _, seg := range []session.SegmentID{segC, segB} {
			if err := be.RemoveSegment(ctx, seg); !session.IsReferenced(err) {
				t.Fatalf("remove reached segment %s = %v, want referenced", seg, err)
			}
		}
	}
	// Deleting C (still covered by D) keeps its segment. Deleting B removes
	// B's own segment; A's segment stays because D still covers it through a2.
	// A2's segment was removed with A2.
	report, err = store.Delete(ctx, "C")
	if err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("delete C = %+v %v, want C's segment kept", report, err)
	}
	report, err = store.Delete(ctx, "B")
	if err != nil || len(report.Removed) != 1 || report.Removed[0] != segB || len(report.Truncated) != 0 {
		t.Fatalf("delete B = %+v %v, want B's segment removed", report, err)
	}
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("collect after deleting B and C = %+v %v, want nothing", report, err)
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "D"}); err != nil || ids(page.Commits) != "a0,a1,a2,c3,d4" {
		t.Fatalf("D after collect = %s %v", ids(page.Commits), err)
	}
	report, err = store.Delete(ctx, "D")
	if err != nil {
		t.Fatal(err)
	}
	removed := map[session.SegmentID]bool{}
	for _, id := range report.Removed {
		removed[id] = true
	}
	if len(removed) != 3 || !removed[segA] || !removed[segC] || !removed[segD] || len(report.Truncated) != 0 {
		t.Fatalf("delete D = %+v, want A, C, D removed", report)
	}
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("final collect = %+v %v, want nothing", report, err)
	}
	testSpanBound(t, store)
}

// testSpanBound: two children fork the same segment at different commits.
// Deleting the child with the greater end lowers the bound to the other
// child's end; deleting the last span removes the segment.
func testSpanBound(t *testing.T, store session.Stores) {
	t.Helper()
	ctx := context.Background()
	header := create(t, store, "P")
	seg := header.ID
	w := open(t, store, "P", false)
	p0 := appendCommit(t, w, "p0", batch(chatStream(), "twilight/x/p", `{"n":0}`))
	p1 := appendCommit(t, w, "p1", batch(chatStream(), "twilight/x/p", `{"n":1}`))
	p2 := appendCommit(t, w, "p2", batch(chatStream(), "twilight/x/p", `{"n":2}`))
	low, err := forkAt(t, store, "low", "P", p0.Seq)
	if err != nil {
		t.Fatal(err)
	}
	high, err := forkAt(t, store, "high", "P", p1.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	report, err := store.Delete(ctx, "P")
	if err != nil || len(report.Removed) != 0 || report.Truncated[seg] != p1.Seq+1 || len(report.Dropped[seg]) != 1 || report.Dropped[seg][0] != p2.CommitID {
		t.Fatalf("delete P = %+v %v, want p2 dropped and the segment kept through p1", report, err)
	}
	for sid, want := range map[session.SessionID]string{"low": "p0", "high": "p0,p1"} {
		page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if err != nil || ids(page.Commits) != want {
			t.Fatalf("%s after deleting P = %s %v, want %s", sid, ids(page.Commits), err, want)
		}
	}
	report, err = store.Delete(ctx, "high")
	if err != nil || len(report.Removed) != 1 || report.Removed[0] != high.ID || report.Truncated[seg] != p0.Seq+1 || len(report.Dropped[seg]) != 1 || report.Dropped[seg][0] != p1.CommitID {
		t.Fatalf("delete high = %+v %v, want high's segment removed and p1 dropped", report, err)
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "low"}); err != nil || ids(page.Commits) != "p0" {
		t.Fatalf("low after deleting high = %s %v", ids(page.Commits), err)
	}
	report, err = store.Delete(ctx, "low")
	if err != nil {
		t.Fatal(err)
	}
	removed := map[session.SegmentID]bool{}
	for _, id := range report.Removed {
		removed[id] = true
	}
	if len(removed) != 2 || !removed[low.ID] || !removed[seg] || len(report.Truncated) != 0 {
		t.Fatalf("delete low = %+v, want low's segment and P's segment removed", report)
	}
}
