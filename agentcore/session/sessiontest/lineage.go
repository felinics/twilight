package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

// SES-GC-1/2: Sessions are roots into a forest of immutable segments. Delete
// drops a root and nothing else; Collect keeps every commit a root still
// reaches through fork edges and reclaims the rest, transitively.
//
//	A: a0 a1 a2 a3        B -> A@1        C -> A@2        D -> C@(c3)
func testLineage(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.Store
	headerA := create(t, store, "A")
	segA := headerA.ID
	aw := open(t, store, "A", false)
	var a []session.Commit
	for i := 0; i < 4; i++ {
		a = append(a, appendCommit(t, aw, "a"+string(rune('0'+i)), batch(chatStream(), "twilight/x/a", `{"n":`+string(rune('0'+i))+`}`)))
	}
	fork := func(child, parent session.SessionID, at session.Commit) session.SegmentHeader {
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

	// Delete refuses an owned Session and an unknown one.
	if err := store.Delete(ctx, "A"); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("delete owned = %v", err)
	}
	if err := store.Delete(ctx, "ghost"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("delete unknown = %v", err)
	}
	_ = aw.Close(ctx)
	if err := store.Delete(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	// A is no Session any more: not found, not openable, not forkable; a
	// second Delete is not found. Its identity is free again at once, and a
	// recreated A is a new root on a new segment.
	if _, err := store.Header(ctx, "A"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("header after delete = %v", err)
	}
	if _, err := store.Open(ctx, "A", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open after delete = %v", err)
	}
	if _, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("read after delete = %v", err)
	}
	if _, err := forkAt(t, store, "E", "A", a[0].Seq); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("fork of a deleted session = %v", err)
	}
	if err := store.Delete(ctx, "A"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("second delete = %v", err)
	}
	// B, C and D still read their prefixes, and D through A's segment.
	for sid, want := range map[session.SessionID]string{"B": "a0,a1", "C": "a0,a1,a2,c3", "D": "a0,a1,a2,c3"} {
		page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if err != nil || ids(page.Commits) != want {
			t.Fatalf("%s after deleting A = %s %v, want %s", sid, ids(page.Commits), err, want)
		}
	}
	// Collect keeps A's segment up to the furthest live edge (C@2) and drops
	// a3; B, C, D are untouched.
	report, err := store.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 || report.Truncated[segA] != a[2].Seq+1 {
		t.Fatalf("collect = %+v, want A's segment truncated after %d", report, a[2].Seq)
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
	if !dw.Committed("a0") || dw.Committed("a3") {
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
	// The freed identity can be recreated meanwhile; the new A is unrelated
	// to the old segment.
	newA, err := store.Create(ctx, session.CreateRequest{SessionID: "A", CreatedAtUnixMilli: 9})
	if err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
	if newA.ID == segA {
		t.Fatal("recreated A reused the old segment")
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"}); err != nil || len(page.Commits) != 0 {
		t.Fatalf("recreated A = %+v %v", page, err)
	}
	if err := store.Delete(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	// Deleting C (still reached by D) keeps its segment; deleting B, whose
	// edge ends at A@1, removes B's own segment; the new A's empty segment
	// goes too. A's old segment stays because D reaches it through C.
	if err := store.Delete(ctx, "C"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	report, _ = store.Collect(ctx)
	removed := map[session.SegmentID]bool{}
	for _, id := range report.Removed {
		removed[id] = true
	}
	if len(removed) != 2 || !removed[segB] || !removed[newA.ID] || len(report.Truncated) != 0 {
		t.Fatalf("collect after deleting B and C = %+v, want B's and the new A's segments removed", report)
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "D"}); err != nil || ids(page.Commits) != "a0,a1,a2,c3,d4" {
		t.Fatalf("D after collect = %s %v", ids(page.Commits), err)
	}
	if err := store.Delete(ctx, "D"); err != nil {
		t.Fatal(err)
	}
	report, _ = store.Collect(ctx)
	removed = map[session.SegmentID]bool{}
	for _, id := range report.Removed {
		removed[id] = true
	}
	if len(removed) != 3 || !removed[segA] || !removed[segC] || !removed[segD] || len(report.Truncated) != 0 {
		t.Fatalf("final collect = %+v, want A, C, D removed", report)
	}
}
