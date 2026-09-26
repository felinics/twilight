// Package sessiontest is the Store-parameterized conformance suite of the
// Session kernel (agent-session.md section 7). Memory and durable adapters
// run the same suite.
package sessiontest

import (
	"context"
	"math"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// Fixture is one adapter under test.
type Fixture struct {
	Store session.Store
}

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("wire", func(t *testing.T) { testWire(t, factory(t)) })
	t.Run("streams", func(t *testing.T) { testStreams(t, factory(t)) })
	t.Run("ownership", func(t *testing.T) { testOwnership(t, factory(t)) })
	t.Run("append", func(t *testing.T) { testAppend(t, factory(t)) })
	t.Run("crash", func(t *testing.T) { testCrashTail(t, factory(t)) })
	t.Run("read", func(t *testing.T) { testRead(t, factory(t)) })
	t.Run("query", func(t *testing.T) { testQuery(t, factory(t)) })
	t.Run("scope", func(t *testing.T) { testScope(t, factory(t)) })
	t.Run("fork", func(t *testing.T) { testFork(t, factory(t)) })
	t.Run("lineage", func(t *testing.T) { testLineage(t, factory(t)) })
	t.Run("index", func(t *testing.T) { testIndex(t, factory(t)) })
	t.Run("lease", func(t *testing.T) { testLease(t, factory(t)) })
}

func create(t *testing.T, store session.Store, sid session.SessionID) session.SegmentHeader {
	t.Helper()
	h, err := store.Create(context.Background(), session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return h
}

func open(t *testing.T, store session.Store, sid session.SessionID, takeover bool) session.Handle {
	t.Helper()
	w, err := store.Open(context.Background(), sid, session.OpenOptions{Takeover: takeover})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func chatStream() session.StreamRef { return session.StreamRef{Domain: "chat"} }

func runStream(id string) session.StreamRef {
	return session.StreamRef{Domain: "run", ID: id}
}

// batch builds one single-event batch for stream.
func batch(stream session.StreamRef, typ, payload string) session.StreamBatch {
	return session.StreamBatch{Stream: stream, Events: []session.Event{
		{Type: session.EventType(typ), Payload: jsonstable.MustParse(payload), RecordedAtUnixMilli: 1},
	}}
}

// appendCommit appends one commit and returns it as stored.
func appendCommit(t *testing.T, w session.Handle, id string, batches ...session.StreamBatch) session.Commit {
	t.Helper()
	c, err := w.Append(context.Background(), session.Proposal{CommitID: session.CommitID(id), Batches: batches})
	if err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
	return c
}

// SES-WIR-1/3: contiguous CommitSeq, one CommitID per commit, unique
// CommitID, canonical payload.
func testWire(t *testing.T, f Fixture) {
	ctx := context.Background()
	h := create(t, f.Store, "s")
	if h.ID == "" {
		t.Fatalf("header = %+v", h)
	}
	// A retried Create is a replay whatever clock it carries; a request
	// that describes another segment is a conflict (SES-CRT-1).
	if again, err := f.Store.Create(ctx, session.CreateRequest{SessionID: "s", CreatedAtUnixMilli: 2}); err != nil || again.ID != h.ID {
		t.Fatalf("retried create is not idempotent: %+v %v", again, err)
	}
	if rec, err := f.Store.Record(ctx, "s"); err != nil || rec.CreatedAtUnixMilli != 1 {
		t.Fatalf("record after retry = %+v %v, want the first creation time", rec, err)
	}
	if _, err := f.Store.Create(ctx, session.CreateRequest{SessionID: "s", CreatedAtUnixMilli: 1, CausationID: "other"}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("different create = %v, want conflict", err)
	}
	w := open(t, f.Store, "s", false)
	if head := w.Head(); head.Next != 0 {
		t.Fatalf("empty head = %+v", head)
	}
	c1 := appendCommit(t, w, "c1",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/x/a", Payload: jsonstable.MustParse(`{"a":1}`), RecordedAtUnixMilli: 1},
			{Type: "twilight/y/b", Payload: jsonstable.MustParse(`{"b":2}`), RecordedAtUnixMilli: 1},
		}})
	c2 := appendCommit(t, w, "c2", batch(chatStream(), "twilight/x/c", `{}`))
	if c1.Seq != 0 || c2.Seq != 1 {
		t.Fatalf("seq not contiguous: %v %v", c1.Seq, c2.Seq)
	}
	if c1.CommitID != "c1" || c2.CommitID != "c2" {
		t.Fatalf("commit identity = %+v %+v", c1, c2)
	}
	if _, err := w.Append(ctx, session.Proposal{CommitID: "c1", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("duplicate CommitID = %v, want conflict", err)
	}
	if head := w.Head(); head.Next != 2 {
		t.Fatalf("head = %+v", head)
	}
	page, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 2 {
		t.Fatalf("read = %+v %v", page, err)
	}
	if page.Header.ID != h.ID || page.Head.Next != 2 {
		t.Fatalf("page header/head = %+v", page)
	}
}

// Streams: one commit may span several logical streams atomically; ReadStream
// returns one stream's events in CommitSeq order with From counting events
// inside the stream.
func testStreams(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	appendCommit(t, w, "c1", batch(chatStream(), "twilight/chat/a", `{"n":1}`))
	appendCommit(t, w, "c2",
		batch(chatStream(), "twilight/chat/b", `{"n":2}`),
		session.StreamBatch{Stream: runStream("r7"), Events: []session.Event{
			{Type: "twilight/run/run_created", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"runId":"r7"}`)},
			{Type: "twilight/run/model_step_completed", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"runId":"r7"}`)},
		}})
	appendCommit(t, w, "c3", batch(runStream("r7"), "twilight/run/run_ended", `{"runId":"r7"}`))
	appendCommit(t, w, "c4", batch(chatStream(), "twilight/chat/c", `{"n":3}`))

	page, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: chatStream(), Lineage: session.LineageSession})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("chat stream = %d events, err %v", len(page.Events), err)
	}
	runs, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: runStream("r7"), Lineage: session.LineageSegment})
	if err != nil || len(runs.Events) != 3 {
		t.Fatalf("run stream = %d events, err %v", len(runs.Events), err)
	}
	for i, want := range []string{"twilight/chat/a", "twilight/chat/b", "twilight/chat/c"} {
		if page.Events[i].Type != session.EventType(want) {
			t.Fatalf("session event %d = %s, want %s", i, page.Events[i].Type, want)
		}
	}
	for i, want := range []string{"twilight/run/run_created", "twilight/run/model_step_completed", "twilight/run/run_ended"} {
		if runs.Events[i].Type != session.EventType(want) {
			t.Fatalf("run event %d = %s, want %s", i, runs.Events[i].Type, want)
		}
	}
	// From counts inside the stream: it skips a stream's own events only.
	tail, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: chatStream(), Lineage: session.LineageSession, From: 1})
	if err != nil || len(tail.Events) != 2 || tail.Events[0].Type != "twilight/chat/b" {
		t.Fatalf("chat stream from 1 = %+v, err %v", tail.Events, err)
	}
	runTail, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: runStream("r7"), Lineage: session.LineageSegment, From: 2})
	if err != nil || len(runTail.Events) != 1 || runTail.Events[0].Type != "twilight/run/run_ended" {
		t.Fatalf("run stream from 2 = %+v, err %v", runTail.Events, err)
	}
	limited, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: chatStream(), Lineage: session.LineageSession, Limit: 2})
	if err != nil || len(limited.Events) != 2 || !limited.HasMore {
		t.Fatalf("limited stream = %d more=%v, err %v", len(limited.Events), limited.HasMore, err)
	}
	// The spanning commit is whole from every angle: both streams see their
	// side and ReadCommits returns it with both batches.
	commits, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", From: 1, Limit: 1})
	if err != nil || len(commits.Commits) != 1 || len(commits.Commits[0].Batches) != 2 {
		t.Fatalf("spanning commit = %+v, err %v", commits, err)
	}
	// Malformed stream refs and a read that declares no lineage are rejected
	// before anything is read.
	if _, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: session.StreamRef{}, Lineage: session.LineageSession}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("empty stream ref = %v, want invalid", err)
	}
	if _, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: session.StreamRef{Domain: "run/r7"}, Lineage: session.LineageSegment}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("stream domain with separator = %v, want invalid", err)
	}
	if _, err := f.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: "s", Stream: chatStream()}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("read without lineage = %v, want invalid", err)
	}
}

// SES-OWN-1/2: second Open is ErrOwned; Close then Open bumps Epoch; an Open
// with Takeover supersedes a live owner; a superseded Writer's Append fails
// without writing.
func testOwnership(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w1 := open(t, f.Store, "s", false)
	if w1.Epoch() != 1 {
		t.Fatalf("first epoch = %d", w1.Epoch())
	}
	if _, err := f.Store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("second open = %v, want owned", err)
	}
	appendCommit(t, w1, "c1", batch(chatStream(), "twilight/x/a", `{}`))
	if err := w1.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w2 := open(t, f.Store, "s", false)
	if w2.Epoch() != 2 {
		t.Fatalf("epoch after reopen = %d, want 2", w2.Epoch())
	}
	if _, err := w1.Append(ctx, session.Proposal{CommitID: "c2", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("old writer append = %v, want ownership_lost", err)
	}
	page, _ := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if len(page.Commits) != 1 {
		t.Fatalf("fenced append wrote commits: %d", len(page.Commits))
	}
	appendCommit(t, w2, "c2", batch(chatStream(), "twilight/x/a", `{}`))
	if err := w1.Close(ctx); err != nil {
		t.Fatalf("closing a superseded writer must be a no-op: %v", err)
	}
	if _, err := f.Store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatal("closing a superseded writer released the current owner")
	}
	// Takeover supersedes the live owner: the crashed-process recovery path.
	w3 := open(t, f.Store, "s", true)
	if w3.Epoch() != w2.Epoch()+1 {
		t.Fatalf("takeover epoch = %d, want %d", w3.Epoch(), w2.Epoch()+1)
	}
	if _, err := w2.Append(ctx, session.Proposal{CommitID: "late", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("superseded writer append = %v, want ownership_lost", err)
	}
	appendCommit(t, w3, "c3", batch(chatStream(), "twilight/x/a", `{}`))
	page, _ = f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if len(page.Commits) != 3 {
		t.Fatalf("ledger after takeover = %d commits, want 3", len(page.Commits))
	}
	// Read never needs ownership (SES-OWN-4): already exercised above while owned.
}

// SES-APP-1/3: whole-commit visibility and the rejection list, none writing.
func testAppend(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	rejects := []struct {
		name string
		p    session.Proposal
		code session.ErrorCode
	}{
		{"no batches", session.Proposal{CommitID: "c"}, session.ErrInvalid},
		{"empty commit id", session.Proposal{Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `{}`)}}, session.ErrInvalid},
		{"batch without events", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{{Stream: chatStream()}}}, session.ErrInvalid},
		{"empty type", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{{Stream: chatStream(), Events: []session.Event{{Payload: jsonstable.MustParse(`{}`), RecordedAtUnixMilli: 1}}}}}, session.ErrInvalid},
		{"non-object payload", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `[1]`)}}, session.ErrInvalid},
		{"zero payload", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{{Stream: chatStream(), Events: []session.Event{{Type: "twilight/x/a", RecordedAtUnixMilli: 1}}}}}, session.ErrInvalid},
		{"empty stream domain", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{batch(session.StreamRef{ID: "r7"}, "twilight/run/a", `{}`)}}, session.ErrInvalid},
		{"stream domain with separator", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{batch(session.StreamRef{Domain: "run/r7"}, "twilight/run/a", `{}`)}}, session.ErrInvalid},
		{"same stream twice", session.Proposal{CommitID: "c", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/a", `{}`), batch(chatStream(), "twilight/x/b", `{}`)}}, session.ErrInvalid},
	}
	for _, tc := range rejects {
		if _, err := w.Append(ctx, tc.p); !session.IsCode(err, tc.code) {
			t.Fatalf("%s: err = %v, want %s", tc.name, err, tc.code)
		}
	}
	if head := w.Head(); head.Next != 0 {
		t.Fatalf("rejections wrote commits: head %+v", head)
	}
	c1 := appendCommit(t, w, "c1",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/x/a", Payload: jsonstable.MustParse(`{"i":0}`), RecordedAtUnixMilli: 1},
			{Type: "twilight/x/a", Payload: jsonstable.MustParse(`{"i":1}`), RecordedAtUnixMilli: 1},
			{Type: "twilight/x/a", Payload: jsonstable.MustParse(`{"i":2}`), RecordedAtUnixMilli: 1},
		}})
	page, _ := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if len(page.Commits) != 1 || len(page.Commits[0].Batches) != 1 || len(page.Commits[0].Batches[0].Events) != 3 || page.Commits[0].CommitID != c1.CommitID {
		t.Fatalf("commit not visible as a whole: %+v", page.Commits)
	}
	// A spanning commit lands every batch or none: both sides are visible.
	appendCommit(t, w, "c2",
		batch(chatStream(), "twilight/chat/a", `{}`),
		batch(runStream("r9"), "twilight/run/run_created", `{"runId":"r9"}`))
	page, _ = f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", From: 1})
	if len(page.Commits) != 1 || len(page.Commits[0].Batches) != 2 {
		t.Fatalf("spanning commit not whole: %+v", page.Commits)
	}
}

// TailCrasher is the optional adapter capability that simulates a crash
// inside Append by dropping every durable commit after the first keep. An
// adapter whose writes cannot tear (a database transaction) need not
// implement it, and the crash case is then skipped rather than silently
// passing.
type TailCrasher interface {
	CrashTail(session.SessionID, int) error
}

// SES-APP-2: a crash leaves at most one torn commit. Open must recover to
// the last whole commit, so no reader ever sees a partial commit and the
// next Append cannot extend a commit that never became durable.
func testCrashTail(t *testing.T, f Fixture) {
	crash, ok := f.Store.(TailCrasher)
	if !ok {
		t.Skip("adapter has no tail to tear: writes are atomic by construction")
	}
	ctx := context.Background()
	header := create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	c1 := appendCommit(t, w, "c1",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
			{Type: "twilight/x/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"b":2}`)},
		}})
	appendCommit(t, w, "c2",
		batch(chatStream(), "twilight/x/c", `{"c":3}`),
		batch(runStream("r7"), "twilight/run/run_created", `{"runId":"r7"}`))
	if err := w.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Only c1 reached durable storage.
	if err := crash.CrashTail("s", 1); err != nil {
		t.Fatalf("crash injection: %v", err)
	}

	w2 := open(t, f.Store, "s", false)
	if got, want := w2.Head(), (session.Head{Next: 1}); got != want {
		t.Fatalf("head after a torn tail = %+v, want %+v (the torn commit must be dropped, not continued)", got, want)
	}

	page, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 || page.Commits[0].CommitID != c1.CommitID {
		t.Fatalf("commits after a torn tail = %+v, err %v; want the 1 whole commit", page.Commits, err)
	}

	// The commit that never became durable must be admissible again, and it
	// must follow the recovered head rather than the torn bytes.
	re := appendCommit(t, w2, "c2", batch(chatStream(), "twilight/x/c", `{"c":3}`))
	if re.Seq != 1 {
		t.Fatalf("re-appended commit = seq %d, want seq 1", re.Seq)
	}
	page, err = f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 2 || page.Header.ID != header.ID {
		t.Fatalf("commits after re-append = %d, err %v; want 2", len(page.Commits), err)
	}
	for i := range page.Commits {
		if page.Commits[i].Seq != session.CommitSeq(i) {
			t.Fatalf("seq gap at %d: %+v", i, page.Commits[i])
		}
	}
}

// SES-REP-1/2: order, From, Limit at commit boundaries.
func testRead(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	appendCommit(t, w, "c1",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/run/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
			{Type: "twilight/chat/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}},
		batch(runStream("r1"), "twilight/run/model_step", `{"runId":"r1"}`))
	appendCommit(t, w, "c2", batch(chatStream(), "twilight/chat/b", `{}`))
	appendCommit(t, w, "c3",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/run/c", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
			{Type: "twilight/run/d", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
			{Type: "twilight/chat/e", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}})
	all, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(all.Commits) != 3 || all.HasMore {
		t.Fatalf("read all = %d %v %v", len(all.Commits), all.HasMore, err)
	}
	for i, c := range all.Commits {
		if c.Seq != session.CommitSeq(i) {
			t.Fatalf("order broken at %d: %+v", i, c)
		}
	}
	from, _ := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", From: 2})
	if len(from.Commits) != 1 || from.Commits[0].Seq != 2 {
		t.Fatalf("from = %+v", from.Commits)
	}
	// A From at or past the head is an empty page up to the largest CommitSeq;
	// an int conversion of that value would wrap negative and index the log.
	for _, from := range []session.CommitSeq{3, 4, 99, math.MaxUint64} {
		beyond, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", From: from})
		if err != nil || len(beyond.Commits) != 0 || beyond.HasMore || beyond.Head.Next != 3 {
			t.Fatalf("from %d beyond head = %+v err=%v", from, beyond, err)
		}
	}
	limited, _ := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", Limit: 1})
	if len(limited.Commits) != 1 || !limited.HasMore || limited.Commits[0].Seq != 0 {
		t.Fatalf("limit must cut at a commit boundary: %+v more=%v", limited.Commits, limited.HasMore)
	}
	two, _ := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s", Limit: 2})
	if len(two.Commits) != 2 || !two.HasMore {
		t.Fatalf("limit 2 = %d more=%v", len(two.Commits), two.HasMore)
	}
	if _, err := f.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "nope"}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("unknown session = %v", err)
	}
}

// SES-REP-3/4: the kernel answers which CommitIDs it holds and what commit
// they carry, from the index Append already needs (SES-APP-3). Both answers
// must hold for a commit appended by the current handle and again after a
// reopen, which is where a durable adapter rebuilds that index from the log;
// and a caller that mutates the returned commit must not reach the stored
// one.
func testQuery(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	first := appendCommit(t, w, "c1",
		session.StreamBatch{Stream: chatStream(), Events: []session.Event{
			{Type: "twilight/run/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"n":1}`)},
			{Type: "twilight/run/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"n":2}`)},
		}})
	appendCommit(t, w, "c2", batch(chatStream(), "twilight/run/c", `{"n":3}`))

	if w.Committed("absent") {
		t.Fatal("an unknown CommitID was reported committed")
	}
	if !w.Committed("c1") || !w.Committed("c2") {
		t.Fatal("an appended CommitID was not reported committed")
	}
	got, ok, err := w.LookupCommit("c1")
	if err != nil || !ok || len(got.Batches) != len(first.Batches) {
		t.Fatalf("lookup c1 = %+v, ok=%v, err=%v", got, ok, err)
	}
	if got.Seq != first.Seq || got.CommitID != "c1" {
		t.Fatalf("lookup c1 = %+v, want %+v", got, first)
	}
	if got.Batches[0].Events[0].Type != first.Batches[0].Events[0].Type {
		t.Fatalf("lookup event = %+v, want %+v", got.Batches[0].Events[0], first.Batches[0].Events[0])
	}
	if _, ok, err := w.LookupCommit("absent"); ok || err != nil {
		t.Fatalf("unknown lookup = ok=%v, err=%v", ok, err)
	}
	got.Batches[0].Events[0].Type = "twilight/tampered/x"
	if again, _, _ := w.LookupCommit("c1"); again.Batches[0].Events[0].Type != first.Batches[0].Events[0].Type {
		t.Fatal("mutating the returned commit reached the stored one")
	}

	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w = open(t, f.Store, "s", false)
	if !w.Committed("c2") {
		t.Fatal("a reopened handle lost a committed CommitID")
	}
	if got, ok, err := w.LookupCommit("c2"); err != nil || !ok || len(got.Batches) != 1 {
		t.Fatalf("reopened lookup c2 = %+v, ok=%v, err=%v", got, ok, err)
	}
	last := appendCommit(t, w, "c3", batch(chatStream(), "twilight/run/d", `{"n":4}`))
	if got, ok, err := w.LookupCommit("c3"); err != nil || !ok || got.Seq != last.Seq {
		t.Fatalf("lookup after append = %+v, ok=%v, err=%v", got, ok, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// SES-SCP-3: an unknown Session is ErrNotFound, not an implicitly created
// stream.
func testScope(t *testing.T, f Fixture) {
	ctx := context.Background()
	if _, err := f.Store.Open(ctx, "missing", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open unknown session = %v", err)
	}
	if _, err := f.Store.Header(ctx, "missing"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("header unknown session = %v", err)
	}
}
