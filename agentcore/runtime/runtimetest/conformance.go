package runtimetest

import (
	"errors"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/reconcile"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
	"testing"
)

// Run executes the Turn conformance suite (docs/architecture/core.md) against
// fixtures made by factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"Start", testStart},
		{"Deliver", testDeliver},
		{"Stop", testStop},
		{"Status", testStatus},
		{"Projection", testProjection},
		{"Recovery", testRecovery},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, factory) })
	}
}

const (
	typeCreated   = sessionstore.Prefix + "turn_started"
	typeAccepted  = sessionstore.Prefix + "input_accepted"
	typeEnded     = sessionstore.Prefix + "turn_ended"
	changedDigest = "sha256:changed"
)

// --- Start -----------------------------

func testStart(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	submitted := h.submit("in-1", "in-2")
	before := h.head()

	// every rejection leaves the ledger untouched.
	altered := input("in-1")
	altered.Digest = changedDigest
	rejects := []struct {
		name     string
		req      sessionkernel.StartRequest
		conflict bool // ErrConflict, otherwise a validation error
	}{
		{"missing preset", sessionkernel.StartRequest{Ref: h.ref("t1")}, false},
		{"duplicate input ids", h.startRequest("t1", submitted[0], submitted[0]), false},
		{"input never submitted", h.startRequest("t1", input("ghost")), true},
		{"payload differs from submitted content", h.startRequest("t1", altered), true},
	}
	for _, tc := range rejects {
		_, err := h.c.Start(h.ctx, h.writer(), tc.req)
		if err == nil || errors.Is(err, turn.ErrConflict) != tc.conflict {
			t.Fatalf("%s: err = %v, want conflict=%v", tc.name, err, tc.conflict)
		}
	}
	if h.head() != before {
		t.Fatal("a rejected Start wrote to the ledger")
	}

	// one atomic group in the specified order under the derived
	// CommitID; the response reflects the committed state.
	resp, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t1", submitted...))
	if err != nil {
		t.Fatal(err)
	}
	turnID := turn.TurnID("t1")
	if resp.Status != turn.TurnActive || resp.Ref.TurnID != turnID || resp.Disposition != "" || resp.End != nil {
		t.Fatalf("start response = %+v", resp)
	}
	plan := sessionstore.PlanDigest("t1", presetRef, submitted)
	group := h.group(ledger.CommitID(sessionstore.StartOperationDigest(sid, "t1", plan)))
	if !sameTypes(group, typeCreated, typeAccepted, typeAccepted) {
		t.Fatalf("start group = %v", eventTypes(group))
	}
	started := decode[sessionstore.Event](t, h.registry, &group[0])
	start, ok := started.Fact.(turn.TurnStarted)
	if !ok || start.TurnID != "t1" || start.Preset != presetRef {
		t.Fatalf("started = %+v", started)
	}
	chat := h.chat()
	for _, id := range []chatlog.InputID{"in-1", "in-2"} {
		if v, _ := chat.Inputs.Get(id); v.Status != chatlog.InputDelivered || v.Input.TurnID != "t1" {
			t.Fatalf("input %s = %+v, want delivered to t1", id, v)
		}
	}
	view := h.surface().Turns["t1"]
	if len(view.InputIDs) != 2 || view.TurnID != turnID || view.End != nil {
		t.Fatalf("turn view = %+v", view)
	}
	snap := h.load(turnID)
	if snap.State.Status != turn.TurnActive || len(snap.State.PendingInputs) != 2 {
		t.Fatalf("Turn after start = %+v, want Open with both inputs pending", snap.State)
	}

	// a replay at a later time is already-applied and writes nothing.
	after := h.head()
	h.now += 60_000
	again, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t1", submitted...))
	if err != nil || again.Ref.TurnID != turnID || h.head() != after {
		t.Fatalf("replay = %+v %v, head moved=%v", again, err, h.head() != after)
	}

	// A Turn identity cannot be replayed with a different immutable preset
	// or different content under the same input identity.
	changedPreset := h.startRequest("t1", submitted...)
	changedPreset.Preset.ID = "another-preset"
	changedInputs := append([]turn.AgentInput(nil), submitted...)
	changedInputs[0].Digest = changedDigest
	for _, req := range []sessionkernel.StartRequest{changedPreset, h.startRequest("t1", changedInputs...)} {
		if _, err := h.c.Start(h.ctx, h.writer(), req); !errors.Is(err, turn.ErrConflict) || h.head() != after {
			t.Fatalf("changed start replay: err=%v moved=%v", err, h.head() != after)
		}
	}

	// the same TurnID with another plan, and a second
	// Turn while one is active, are conflicts.
	extra := h.submit("in-3")
	if _, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t1", extra...)); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("restart with a different plan = %v, want conflict", err)
	}
	if _, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t2", extra...)); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("second active turn = %v, want conflict", err)
	}
	surface := h.surface()
	if active, ok := surface.Active(); !ok || active.TurnID != "t1" {
		t.Fatalf("active = %+v %v", active, ok)
	}
	if v, _ := h.chat().Inputs.Get("in-3"); v.Status != chatlog.InputSubmitted {
		t.Fatalf("in-3 after rejected starts = %s, want submitted", v.Status)
	}
}

// --- Deliver --------------------------------------------------------------

func testDeliver(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")
	turnID := resp.Ref.TurnID

	// input_accepted is the sole durable delivery fact. Its CommitID is the
	// Turn input CommandID and the machine queues the accepted input.
	in2 := h.submit("in-2")
	dresp, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: in2})
	if err != nil || dresp.Status != turn.TurnActive || dresp.Ref.TurnID != turnID {
		t.Fatalf("deliver = %+v %v", dresp, err)
	}
	group := h.group(ledger.CommitID(schema.Identity().DeriveInputCommandID(turnID, "in-2")))
	if !sameTypes(group, typeAccepted) {
		t.Fatalf("deliver group = %v, want input_accepted", eventTypes(group))
	}
	if v, _ := h.chat().Inputs.Get("in-2"); v.Status != chatlog.InputDelivered || v.Input.TurnID != "t1" {
		t.Fatalf("in-2 = %+v", v)
	}
	if ids := h.surface().Turns["t1"].InputIDs; len(ids) != 2 || ids[1] != "in-2" {
		t.Fatalf("InputIDs after deliver = %v", ids)
	}
	if pending := h.load(turnID).State.PendingInputs; len(pending) != 2 || pending[1].ID != "in-2" {
		t.Fatalf("pending after deliver = %+v", pending)
	}
	// Replay of the same delivery writes nothing.
	head := h.head()
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: in2}); err != nil || h.head() != head {
		t.Fatalf("deliver replay: err=%v moved=%v", err, h.head() != head)
	}

	changed := append([]turn.AgentInput(nil), in2...)
	changed[0].Digest = changedDigest
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: changed}); !errors.Is(err, turn.ErrConflict) || h.head() != head {
		t.Fatalf("changed delivery replay: err=%v moved=%v", err, h.head() != head)
	}

	// an unknown Turn and a Turn that is not active are conflicts;
	// the input stays submitted.
	in3 := h.submit("in-3")
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("nope"), Inputs: in3}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("deliver to unknown turn = %v", err)
	}
	h.appCancel(turnID)
	if st := h.status("t1"); st.Status != turn.TurnStopped {
		t.Fatalf("after app cancel status = %v", st.Status)
	}
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: in3}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("deliver to a stopped turn = %v, want conflict", err)
	}
	if v, _ := h.chat().Inputs.Get("in-3"); v.Status != chatlog.InputSubmitted {
		t.Fatalf("in-3 after refused deliver = %s, want submitted", v.Status)
	}

	// Deliver validates like Start. An unsubmitted input, or a
	// payload that differs from the submitted content, is a conflict and the
	// batch writes nothing -- including its valid members.
	h.start("t2", "in-4")
	run2 := h.surface().Turns["t2"].TurnID
	in5 := h.submit("in-5")
	before := h.head()
	pendingBefore := len(h.load(run2).State.PendingInputs)
	rejects := []struct {
		name   string
		inputs []turn.AgentInput
	}{
		{"unsubmitted second input", []turn.AgentInput{in5[0], input("never-submitted")}},
		{"digest differs", []turn.AgentInput{{ID: in5[0].ID, Digest: "sha256:other"}}},
	}
	for _, tc := range rejects {
		if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t2"), Inputs: tc.inputs}); !errors.Is(err, turn.ErrConflict) {
			t.Fatalf("%s: deliver = %v, want conflict", tc.name, err)
		}
		if h.head() != before {
			t.Fatalf("%s: a refused Deliver wrote rows", tc.name)
		}
		if v, _ := h.chat().Inputs.Get("in-5"); v.Status != chatlog.InputSubmitted {
			t.Fatalf("%s: in-5 = %s, want submitted", tc.name, v.Status)
		}
		if got := len(h.load(run2).State.PendingInputs); got != pendingBefore {
			t.Fatalf("%s: pending inputs changed to %d", tc.name, got)
		}
	}
	if h.chat().Inputs.Has("never-submitted") {
		t.Fatal("an unsubmitted input entered the chatlog")
	}

	// a batch is one group under the batch CommandID -- every
	// input_accepted facts together -- and its replay writes nothing.
	in6 := h.submit("in-6")
	batch := []turn.AgentInput{in5[0], in6[0]}
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t2"), Inputs: batch}); err != nil {
		t.Fatalf("batch deliver = %v", err)
	}
	group = h.group(ledger.CommitID(schema.Identity().DeriveInputCommandID(run2, "in-5", "in-6")))
	if !sameTypes(group, typeAccepted, typeAccepted) {
		t.Fatalf("batch group = %v", eventTypes(group))
	}
	for _, id := range []chatlog.InputID{"in-5", "in-6"} {
		if v, _ := h.chat().Inputs.Get(id); v.Status != chatlog.InputDelivered || v.Input.TurnID != "t2" {
			t.Fatalf("%s after batch = %+v", id, v)
		}
	}
	if pending := h.load(run2).State.PendingInputs; len(pending) != pendingBefore+2 {
		t.Fatalf("pending after batch = %+v", pending)
	}
	head = h.head()
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t2"), Inputs: batch}); err != nil || h.head() != head {
		t.Fatalf("batch replay: err=%v moved=%v", err, h.head() != head)
	}
}

// --- Retry --------------------------------------------------

// --- Stop --------------------------------------------------

func testStop(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	if _, err := h.c.Stop(h.ctx, h.writer(), sessionkernel.StopRequest{Ref: h.ref("nope")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("stop unknown = %v", err)
	}

	// CancelTurn and turn/failed{stopped} land in one commit.
	resp := h.start("t1", "in-1")
	sresp, err := h.c.Stop(h.ctx, h.writer(), sessionkernel.StopRequest{Ref: h.ref("t1"), Reason: "user"})
	if err != nil || sresp.Status != turn.TurnStopped || sresp.Disposition != sessionkernel.ResumeFinished || sresp.End == nil {
		t.Fatalf("stop = %+v %v", sresp, err)
	}
	group := h.group(ledger.CommitID(sessionstore.CancelCommandID(sid, "t1")))
	if !sameTypes(group, typeEnded) {
		t.Fatalf("stop facts = %v", eventTypes(group))
	}
	decoded := decode[sessionstore.Event](t, h.registry, &group[0])
	ended, ok := decoded.Fact.(turn.TurnEnded)
	if !ok {
		t.Fatalf("stop event = %T, want turn.TurnEnded", decoded.Fact)
	}
	stopped, ok := ended.End.(turn.TurnStoppedEnd)
	if !ok || stopped.Message != "user" {
		t.Fatalf("stop outcome = %+v", ended)
	}
	// A settled Turn admits nothing else.
	for name, call := range map[string]func() error{
		"stop": func() error {
			_, err := h.c.Stop(h.ctx, h.writer(), sessionkernel.StopRequest{Ref: h.ref("t1")})
			return err
		},
		"deliver": func() error {
			_, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: h.submit("late")})
			return err
		},
	} {
		if err := call(); !errors.Is(err, turn.ErrConflict) {
			t.Fatalf("%s after stop = %v, want conflict", name, err)
		}
	}

	// A Turn that ends without completing settles its Turn as failed from
	// its own turn_ended: the Turn and its Turn are one, and the
	// failure is the Turn's outcome, shown to the user; the next input opens
	// a new Turn with the whole history, including this one, in view.
	resp = h.start("t2", "in-2")
	h.appCancel(resp.Ref.TurnID)
	st := h.status("t2")
	if st.Status != turn.TurnStopped || st.Disposition != sessionkernel.ResumeFinished || st.End == nil || st.Ref.TurnID != resp.Ref.TurnID {
		t.Fatalf("status after a non-completed end = %+v", st)
	}
	if _, stopped := st.End.(turn.TurnStoppedEnd); !stopped {
		t.Fatalf("end = %T, want TurnStoppedEnd", st.End)
	}
	if _, err := h.c.Stop(h.ctx, h.writer(), sessionkernel.StopRequest{Ref: h.ref("t2")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("stop after failure = %v, want conflict", err)
	}
	if surf := h.surface(); func() bool { _, ok := surf.Active(); return ok }() {
		t.Fatal("a failed turn still reports as active")
	}
	next := h.start("t3", "in-3")
	if next.Status != turn.TurnActive {
		t.Fatalf("turn after a failed one = %+v", next)
	}

	// A completed Turn settles through turn_ended in its own
	// group; no turn event is written, and every Coordinator transition then
	// conflicts.
	res := h.complete(next.Ref.TurnID)
	types := eventTypes(res.Events)
	if types[len(types)-1] != sessionstore.Prefix+"turn_ended" {
		t.Fatalf("completion group = %v, want turn_ended last and no turn event", types)
	}
	st = h.status("t3")
	if st.Status != turn.TurnCompleted || st.Disposition != sessionkernel.ResumeFinished || st.End == nil {
		t.Fatalf("completed status = %+v", st)
	}
	if _, ok := (st.End).(turn.TurnCompletedEnd); !ok {
		t.Fatalf("end = %T", st.End)
	}
	if _, err := h.c.Stop(h.ctx, h.writer(), sessionkernel.StopRequest{Ref: h.ref("t3")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("stop after completion = %v, want conflict", err)
	}
}

// --- Status ----------------------------------------------------

func testStatus(t *testing.T, factory Factory) {
	cases := []struct {
		name        string
		arrange     func(h *harness, turnID turn.TurnID)
		status      turn.TurnStatus
		disposition sessionkernel.ResumeDisposition
		waiting     int
		ended       bool
	}{
		{"open Turn has no disposition", func(*harness, turn.TurnID) {}, turn.TurnActive, "", 0, false},
		{"executing model needs recovery", func(h *harness, r turn.TurnID) { h.executingModel(r) }, turn.TurnActive, sessionkernel.ResumeWaitingForRecovery, 0, false},
		{"approval call waits for a response", func(h *harness, r turn.TurnID) { h.waitingTool(r) }, turn.TurnActive, sessionkernel.ResumeWaitingForResponse, 1, false},
		{"completed Turn is finished", func(h *harness, r turn.TurnID) { h.complete(r) }, turn.TurnCompleted, sessionkernel.ResumeFinished, 0, true},
		{"cancelled Turn is finished and failed", func(h *harness, r turn.TurnID) { h.appCancel(r) }, turn.TurnStopped, sessionkernel.ResumeFinished, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, factory(t))
			resp := h.start("t1", "in-1")
			tc.arrange(h, resp.Ref.TurnID)
			st := h.status("t1")
			if st.Status != tc.status || st.Disposition != tc.disposition || len(st.Waiting) != tc.waiting || (st.End != nil) != tc.ended || st.Ref.TurnID != resp.Ref.TurnID {
				t.Fatalf("status = %+v", st)
			}
			if tc.waiting > 0 && st.Waiting[0].Kind != turn.ResponseApproval {
				t.Fatalf("waiting = %+v", st.Waiting)
			}
		})
	}
	h := newHarness(t, factory(t))
	if _, err := h.c.Status(h.ctx, h.ref("nope")); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("status of unknown turn = %v, want conflict", err)
	}
}

// --- surface 投影 ------------------------------------

func testProjection(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")

	// Duplicate starts and terminal facts outside an active lifetime are invalid.
	h.appCancel(resp.Ref.TurnID)
	before := h.head()
	rejects := []struct {
		name string
		id   turn.TurnID
		fact turn.Fact
	}{
		{"started twice", "t1", turn.TurnStarted{TurnID: "t1", Preset: presetRef}},
		{"ended twice", "t1", turn.TurnEnded{End: turn.TurnStoppedEnd{Reason: turn.ReasonCancelled}}},
		{"unknown turn ended", "ghost", turn.TurnEnded{End: turn.TurnStoppedEnd{Reason: turn.ReasonCancelled}}},
	}
	for i, tc := range rejects {
		res := h.commit(writer.SemanticGroup{CommitID: ledger.CommitID("reject-" + string(rune('a'+i))), Batches: []writer.TypedBatch{
			{Domain: sessionstore.Stream(tc.id), Events: []writer.TypedEvent{{Type: sessionstore.EventType(tc.fact), RecordedAtUnixMilli: h.now, Value: sessionstore.Event{TurnID: tc.id, Fact: tc.fact}}}},
		}})
		if res.Outcome != writer.CommitInvalid {
			t.Fatalf("%s: %s", tc.name, res.Outcome)
		}
	}
	if h.head() != before {
		t.Fatal("invalid fact changed ledger")
	}
	v := h.surface().Turns["t1"]
	if v.Status != turn.TurnStopped || v.End == nil {
		t.Fatalf("terminal view = %+v", v)
	}
	// A completed Turn settles from its own turn_ended.
	resp2 := h.start("t2", "in-2")
	h.complete(resp2.Ref.TurnID)
	if v := h.surface().Turns["t2"]; v.Status != turn.TurnCompleted || v.End == nil {
		t.Fatalf("completed view = %+v", v)
	}
	settled := h.surface()
	if _, active := settled.Active(); active {
		t.Fatal("a settled session still reports an active turn")
	}
	if order := h.surface().Order; len(order) != 2 || order[0] != "t1" || order[1] != "t2" {
		t.Fatalf("order = %v", order)
	}
}

// --- recovery -----------------------------------------------

func testRecovery(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")

	// started committed, process gone before driving: the new owner has
	// nothing to dispose and the Turn is still active from the projections
	// alone.
	h.takeover()
	if n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true}); err != nil || n != 0 {
		t.Fatalf("recover after start = %d %v", n, err)
	}
	if st := h.status("t1"); st.Status != turn.TurnActive || st.Ref.TurnID != resp.Ref.TurnID || st.Disposition != "" {
		t.Fatalf("status after takeover = %+v", st)
	}

	// Model executing when the owner dies: the takeover disposes it and the
	// Turn stays active; the superseded Coordinator is fenced.
	h.executingModel(resp.Ref.TurnID)
	if st := h.status("t1"); st.Disposition != sessionkernel.ResumeWaitingForRecovery {
		t.Fatalf("before takeover disposition = %s", st.Disposition)
	}
	// The input is submitted before the takeover so the superseded Writer's
	// projections know it: its Deliver then reaches Append and is fenced there.
	// (A superseded Writer whose projections are stale rejects the group in
	// its own pre-fold instead and never learns of the ownership loss.)
	late := h.submit("late")
	old, oldWriter := h.takeover()
	if n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true}); err != nil || n != 1 {
		t.Fatalf("recover executing model = %d %v", n, err)
	}
	st := h.status("t1")
	if st.Status != turn.TurnActive || st.Disposition == sessionkernel.ResumeWaitingForRecovery {
		t.Fatalf("status after recovery = %+v", st)
	}
	if _, err := old.Deliver(h.ctx, oldWriter, sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: late}); !errors.Is(err, store.ErrOwnershipLost) {
		t.Fatalf("superseded coordinator deliver = %v, want ownership lost", err)
	}
	if v, _ := h.chat().Inputs.Get("late"); v.Status != chatlog.InputSubmitted {
		t.Fatalf("fenced deliver changed the input: %s", v.Status)
	}
	if _, err := h.c.Deliver(h.ctx, h.writer(), sessionkernel.DeliverRequest{Ref: h.ref("t1"), Inputs: late}); err != nil {
		t.Fatalf("owner deliver after takeover: %v", err)
	}
}
