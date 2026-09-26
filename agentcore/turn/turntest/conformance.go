package turntest

import (
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Run executes the Turn conformance suite (agent-turn.md section 8) against
// fixtures made by factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"Start", testStart},
		{"Deliver", testDeliver},
		{"Retry", testRetry},
		{"StopAndSettle", testStopAndSettle},
		{"Status", testStatus},
		{"Projection", testProjection},
		{"Recovery", testRecovery},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, factory) })
	}
}

const (
	typeCreated  = runmod.Prefix + "run_created"
	typeAccepted = runmod.Prefix + "input_accepted"
	typeEnded    = runmod.Prefix + "run_ended"
)

// --- Start（TRN-STR-1/2/3/4、TRN-ID-3/4、TRN-EVT-2、TRN-SCP-2） -----------------------------

func testStart(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	submitted := h.submit("in-1", "in-2")
	before := h.head()

	// TRN-STR-1: every rejection leaves the ledger untouched.
	altered := input("in-1")
	altered.Digest = "sha256:changed"
	rejects := []struct {
		name     string
		req      turn.StartRequest
		conflict bool // ErrConflict, otherwise a validation error
	}{
		{"missing preset", turn.StartRequest{Ref: h.ref("t1")}, false},
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

	// TRN-STR-2/3: one atomic group in the specified order under the derived
	// CommitID; the response reflects the committed state (TRN-STR-4).
	resp, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t1", submitted...))
	if err != nil {
		t.Fatal(err)
	}
	runID := turn.DeriveRunID(sid, "t1", 1)
	if resp.Status != turn.TurnActive || resp.Attempt != 1 || resp.RunID != runID || resp.Disposition != "" || resp.End != nil {
		t.Fatalf("start response = %+v", resp)
	}
	plan := turn.PlanDigest("t1", preset.Digest, []chatlog.InputID{"in-1", "in-2"})
	group := h.group(session.CommitID(turn.StartOperationDigest(sid, "t1", plan)))
	if !sameTypes(group, turn.TypeStarted, attempt.TypeStarted, chatlog.TypeInputDelivered, chatlog.TypeInputDelivered, typeCreated, typeAccepted, typeAccepted) {
		t.Fatalf("start group = %v", eventTypes(group))
	}
	started := decode[turn.StartedPayload](t, h.registry, &group[0])
	if started.TurnID != "t1" || len(started.InputIDs) != 2 || started.Preset != preset {
		t.Fatalf("started payload = %+v", started)
	}
	created := decode[runmod.Event](t, h.registry, &group[4])
	if _, ok := created.Fact.(run.RunCreated); !ok || created.RunID != runID {
		t.Fatalf("created fact = %+v", created)
	}
	// The Turn a Run serves is the attempt module's fact, in the same commit.
	if att := decode[attempt.StartedPayload](t, h.registry, &group[1]); att.TurnID != "t1" || att.RunID != runID || att.Attempt != 1 {
		t.Fatalf("attempt started = %+v", att)
	}
	chat := h.chat()
	for _, id := range []chatlog.InputID{"in-1", "in-2"} {
		if v, _ := chat.Inputs.Get(id); v.Status != chatlog.InputDelivered || v.Input.TurnID != "t1" {
			t.Fatalf("input %s = %+v, want delivered to t1", id, v)
		}
	}
	view := h.surface().Turns["t1"]
	if len(view.InputIDs) != 2 || len(view.Attempts) != 1 || view.Attempts[0].RunID != runID || view.ActiveRun != runID {
		t.Fatalf("turn view = %+v", view)
	}
	snap := h.load(runID)
	if snap.State.Status != run.RunActive || len(snap.State.PendingInputs) != 2 {
		t.Fatalf("run after start = %+v, want Open with both inputs pending", snap.State)
	}

	// TRN-EVT-2: a replay at a later time is already-applied and writes nothing.
	after := h.head()
	h.now += 60_000
	again, err := h.c.Start(h.ctx, h.writer(), h.startRequest("t1", submitted...))
	if err != nil || again.RunID != runID || h.head() != after {
		t.Fatalf("replay = %+v %v, head moved=%v", again, err, h.head() != after)
	}

	// TRN-EVT-3 / TRN-SCP-2: the same TurnID with another plan, and a second
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

// --- Deliver（TRN-DLV-1/2） --------------------------------------------------------------

func testDeliver(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")
	runID := resp.RunID

	// TRN-DLV-2: input_accepted and input_delivered share one commit whose
	// CommitID is the Run's input CommandID; the Run queues the input.
	in2 := h.submit("in-2")
	dresp, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: in2})
	if err != nil || dresp.Status != turn.TurnActive || dresp.RunID != runID {
		t.Fatalf("deliver = %+v %v", dresp, err)
	}
	group := h.group(session.CommitID(schema.Identity().DeriveInputCommandID(runID, "in-2")))
	if !sameTypes(group, typeAccepted, chatlog.TypeInputDelivered) {
		t.Fatalf("deliver group = %v, want input_accepted then input_delivered", eventTypes(group))
	}
	if v, _ := h.chat().Inputs.Get("in-2"); v.Status != chatlog.InputDelivered || v.Input.TurnID != "t1" {
		t.Fatalf("in-2 = %+v", v)
	}
	if ids := h.surface().Turns["t1"].InputIDs; len(ids) != 2 || ids[1] != "in-2" {
		t.Fatalf("InputIDs after deliver = %v", ids)
	}
	if pending := h.load(runID).State.PendingInputs; len(pending) != 2 || pending[1].ID != "in-2" {
		t.Fatalf("pending after deliver = %+v", pending)
	}
	// Replay of the same delivery writes nothing.
	head := h.head()
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: in2}); err != nil || h.head() != head {
		t.Fatalf("deliver replay: err=%v moved=%v", err, h.head() != head)
	}

	// TRN-DLV-1: an unknown Turn and a Turn that is not active are conflicts;
	// the input stays submitted.
	in3 := h.submit("in-3")
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("nope"), Inputs: in3}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("deliver to unknown turn = %v", err)
	}
	h.appCancel(runID)
	if st := h.status("t1"); st.Status != turn.TurnAttemptFailed {
		t.Fatalf("after app cancel status = %s", st.Status)
	}
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: in3}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("deliver to attempt_failed turn = %v, want conflict", err)
	}
	if v, _ := h.chat().Inputs.Get("in-3"); v.Status != chatlog.InputSubmitted {
		t.Fatalf("in-3 after refused deliver = %s, want submitted", v.Status)
	}

	// TRN-DLV-1: Deliver validates like Start. An unsubmitted input, or a
	// payload that differs from the submitted content, is a conflict and the
	// batch writes nothing -- including its valid members (TRN-DLV-2).
	h.start("t2", "in-4")
	run2 := h.surface().Turns["t2"].ActiveRun
	in5 := h.submit("in-5")
	before := h.head()
	pendingBefore := len(h.load(run2).State.PendingInputs)
	rejects := []struct {
		name   string
		inputs []run.AgentInput
	}{
		{"unsubmitted second input", []run.AgentInput{in5[0], input("never-submitted")}},
		{"digest differs", []run.AgentInput{{ID: in5[0].ID, Digest: "sha256:other"}}},
	}
	for _, tc := range rejects {
		if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t2"), Inputs: tc.inputs}); !errors.Is(err, turn.ErrConflict) {
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

	// TRN-DLV-2: a batch is one group under the batch CommandID -- every
	// input_accepted and input_delivered together -- and its replay writes nothing.
	in6 := h.submit("in-6")
	batch := []run.AgentInput{in5[0], in6[0]}
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t2"), Inputs: batch}); err != nil {
		t.Fatalf("batch deliver = %v", err)
	}
	group = h.group(session.CommitID(schema.Identity().DeriveInputCommandID(run2, "in-5", "in-6")))
	if !sameTypes(group, typeAccepted, typeAccepted, chatlog.TypeInputDelivered, chatlog.TypeInputDelivered) {
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
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t2"), Inputs: batch}); err != nil || h.head() != head {
		t.Fatalf("batch replay: err=%v moved=%v", err, h.head() != head)
	}
}

// --- Retry（TRN-RTY-1/2/3、TRN-ID-4） --------------------------------------------------

func testRetry(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")
	run1 := resp.RunID
	in2 := h.submit("in-2")
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: in2}); err != nil {
		t.Fatal(err)
	}
	// TRN-RTY-1: only an attempt_failed Turn may be retried.
	if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: run1}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("retry of an active turn = %v, want conflict", err)
	}
	if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("nope"), PreviousRunID: run1}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("retry of an unknown turn = %v, want conflict", err)
	}
	h.appCancel(run1)
	st := h.status("t1")
	if st.Status != turn.TurnAttemptFailed || st.Disposition != turn.ResumeFinished || st.End == nil {
		t.Fatalf("status after app cancel = %+v", st)
	}
	if _, stopped := st.End.(run.RunStoppedEnd); !stopped {
		t.Fatalf("end = %T, want RunStoppedEnd", st.End)
	}
	rowsBefore := len(h.rows())
	for _, previous := range []run.RunID{"", "unrelated-run"} {
		if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: previous}); !errors.Is(err, turn.ErrConflict) {
			t.Fatalf("retry with previous run %q = %v, want conflict", previous, err)
		}
	}

	// TRN-RTY-1/2: attempt n+1 under the derived CommitID, replaying every
	// delivered input in InputIDs order with its original payload.
	req := turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: run1, Reason: "test"}
	rresp, err := h.c.Retry(h.ctx, h.writer(), req)
	if err != nil {
		t.Fatal(err)
	}
	run2 := turn.DeriveRunID(sid, "t1", 2)
	if rresp.Status != turn.TurnActive || rresp.Attempt != 2 || rresp.RunID != run2 || run2 == run1 {
		t.Fatalf("retry response = %+v", rresp)
	}
	group := h.group(turn.RetryCommitID(sid, "t1", 2))
	if !sameTypes(group, attempt.TypeStarted, typeCreated, typeAccepted, typeAccepted) {
		t.Fatalf("retry group = %v", eventTypes(group))
	}
	if att := decode[attempt.StartedPayload](t, h.registry, &group[0]); att.Attempt != 2 || att.TurnID != "t1" || att.RunID != run2 {
		t.Fatalf("retry attempt started = %+v", att)
	}
	pending := h.load(run2).State.PendingInputs
	if len(pending) != 2 || pending[0].ID != "in-1" || pending[1].ID != "in-2" || pending[1].Digest != in2[0].Digest {
		t.Fatalf("retry replayed inputs = %+v", pending)
	}
	view := h.surface().Turns["t1"]
	if len(view.Attempts) != 2 || view.Attempts[0].End == nil || view.Attempts[1].End != nil || view.ActiveRun != run2 || len(view.InputIDs) != 2 {
		t.Fatalf("attempts after retry = %+v", view.Attempts)
	}
	// TRN-RTY-3: nothing of the failed attempt left the ledger.
	if len(h.rows()) != rowsBefore+len(group) {
		t.Fatalf("retry changed earlier rows: %d -> %d", rowsBefore, len(h.rows()))
	}
	if _, err := h.rt.Record(h.ctx, sid, run1); err != nil {
		t.Fatalf("failed attempt record: %v", err)
	}
	// Replaying the operation confirms the same attempt, even after it ends
	// and another Turn has acquired the Session's active slot.
	head := h.head()
	if again, err := h.c.Retry(h.ctx, h.writer(), req); err != nil || again.RunID != run2 || h.head() != head {
		t.Fatalf("active retry replay = %+v %v, moved=%v", again, err, h.head() != head)
	}
	if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: run2}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("retry of the retried turn = %v, want conflict", err)
	}
	h.appCancel(run2)
	other := h.start("t2", "in-3")
	head = h.head()
	if again, err := h.c.Retry(h.ctx, h.writer(), req); err != nil || again.RunID != run2 || again.Attempt != 2 || h.head() != head {
		t.Fatalf("ended retry replay = %+v %v, moved=%v", again, err, h.head() != head)
	}
	if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: run2}); !errors.Is(err, turn.ErrConflict) || h.head() != head {
		t.Fatalf("retry with another active turn = %v, moved=%v", err, h.head() != head)
	}
	h.appCancel(other.RunID)
	third, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: run2})
	if err != nil || third.RunID != turn.DeriveRunID(sid, "t1", 3) || third.Attempt != 3 {
		t.Fatalf("next explicit retry = %+v %v", third, err)
	}
	head = h.head()
	if again, err := h.c.Retry(h.ctx, h.writer(), req); err != nil || again.RunID != run2 || again.Attempt != 2 || h.head() != head {
		t.Fatalf("old retry replay after later attempt = %+v %v, moved=%v", again, err, h.head() != head)
	}
	h.takeover()
	if again, err := h.c.Retry(h.ctx, h.writer(), req); err != nil || again.RunID != run2 || again.Attempt != 2 || h.head() != head {
		t.Fatalf("retry replay after takeover = %+v %v, moved=%v", again, err, h.head() != head)
	}
}

// --- Stop 与 Settle（TRN-STP-1/2、TRN-STL-1、TRN-EVT-3） ---------------------------------

func testStopAndSettle(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	if _, err := h.c.Stop(h.ctx, h.writer(), turn.StopRequest{Ref: h.ref("nope")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("stop unknown = %v", err)
	}

	// TRN-STP-1: CancelRun and turn/failed{stopped} land in one commit.
	resp := h.start("t1", "in-1")
	sresp, err := h.c.Stop(h.ctx, h.writer(), turn.StopRequest{Ref: h.ref("t1"), Reason: "user"})
	if err != nil || sresp.Status != turn.TurnStopped || sresp.Disposition != turn.ResumeFinished || sresp.End == nil {
		t.Fatalf("stop = %+v %v", sresp, err)
	}
	group := h.group(session.CommitID(turn.CancelCommandID(sid, "t1", resp.RunID)))
	var failed *turn.FailedPayload
	sawEnded := false
	for i := range group {
		switch group[i].Type {
		case turn.TypeFailed:
			p := decode[turn.FailedPayload](t, h.registry, &group[i])
			failed = &p
		case typeEnded:
			sawEnded = true
		}
	}
	if !sawEnded || failed == nil || failed.Settlement != turn.SettlementStopped || failed.FailureClass != "cancelled" || failed.RunID != resp.RunID {
		t.Fatalf("stop group = %v, failed=%+v", eventTypes(group), failed)
	}
	// A settled Turn admits nothing else (TRN-EVT-3).
	for name, call := range map[string]func() error{
		"stop": func() error { _, err := h.c.Stop(h.ctx, h.writer(), turn.StopRequest{Ref: h.ref("t1")}); return err },
		"retry": func() error {
			_, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t1"), PreviousRunID: resp.RunID})
			return err
		},
		"settle": func() error {
			_, err := h.c.Settle(h.ctx, h.writer(), turn.SettleRequest{Ref: h.ref("t1")})
			return err
		},
		"deliver": func() error {
			_, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: h.submit("late")})
			return err
		},
	} {
		if err := call(); !errors.Is(err, turn.ErrConflict) {
			t.Fatalf("%s after stop = %v, want conflict", name, err)
		}
	}

	// TRN-STL-1: Settle needs attempt_failed and writes failed{failed, class}.
	resp = h.start("t2", "in-2")
	if _, err := h.c.Settle(h.ctx, h.writer(), turn.SettleRequest{Ref: h.ref("t2")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("settle of an active turn = %v, want conflict", err)
	}
	h.appCancel(resp.RunID)
	tresp, err := h.c.Settle(h.ctx, h.writer(), turn.SettleRequest{Ref: h.ref("t2"), FailureClass: "gave_up"})
	if err != nil || tresp.Status != turn.TurnFailed || tresp.Disposition != turn.ResumeFinished {
		t.Fatalf("settle = %+v %v", tresp, err)
	}
	group = h.group(turn.SettleCommitID(sid, "t2", resp.RunID))
	if !sameTypes(group, turn.TypeFailed) {
		t.Fatalf("settle group = %v", eventTypes(group))
	}
	if p := decode[turn.FailedPayload](t, h.registry, &group[0]); p.Settlement != turn.SettlementFailed || p.FailureClass != "gave_up" {
		t.Fatalf("settle payload = %+v", p)
	}
	if _, err := h.c.Settle(h.ctx, h.writer(), turn.SettleRequest{Ref: h.ref("t2")}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("second settle = %v, want conflict", err)
	}
	if _, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t2"), PreviousRunID: resp.RunID}); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("retry after settle = %v, want conflict", err)
	}

	// A completed Run settles the Turn through the run_ended of its own
	// group; no turn event is written, and every Coordinator transition then
	// conflicts.
	resp = h.start("t3", "in-3")
	res := h.complete(resp.RunID)
	types := eventTypes(res.Events)
	if types[len(types)-1] != runmod.Prefix+"run_ended" {
		t.Fatalf("completion group = %v, want run_ended last and no turn event", types)
	}
	st := h.status("t3")
	if st.Status != turn.TurnCompleted || st.Disposition != turn.ResumeFinished || st.End == nil {
		t.Fatalf("completed status = %+v", st)
	}
	if _, ok := (st.End).(run.RunCompletedEnd); !ok {
		t.Fatalf("end = %T", st.End)
	}
	for name, call := range map[string]func() error{
		"stop": func() error { _, err := h.c.Stop(h.ctx, h.writer(), turn.StopRequest{Ref: h.ref("t3")}); return err },
		"retry": func() error {
			_, err := h.c.Retry(h.ctx, h.writer(), turn.RetryRequest{Ref: h.ref("t3"), PreviousRunID: resp.RunID})
			return err
		},
		"settle": func() error {
			_, err := h.c.Settle(h.ctx, h.writer(), turn.SettleRequest{Ref: h.ref("t3")})
			return err
		},
	} {
		if err := call(); !errors.Is(err, turn.ErrConflict) {
			t.Fatalf("%s after completion = %v, want conflict", name, err)
		}
	}
}

// --- Status（TRN-STA-1、TRN-API-3） ----------------------------------------------------

func testStatus(t *testing.T, factory Factory) {
	cases := []struct {
		name        string
		arrange     func(h *harness, runID run.RunID)
		status      turn.TurnStatus
		disposition turn.ResumeDisposition
		waiting     int
		ended       bool
	}{
		{"open run has no disposition", func(*harness, run.RunID) {}, turn.TurnActive, "", 0, false},
		{"executing model needs recovery", func(h *harness, r run.RunID) { h.executingModel(r) }, turn.TurnActive, turn.ResumeWaitingForRecovery, 0, false},
		{"approval call waits for a response", func(h *harness, r run.RunID) { h.waitingTool(r) }, turn.TurnActive, turn.ResumeWaitingForResponse, 1, false},
		{"completed run is finished", func(h *harness, r run.RunID) { h.complete(r) }, turn.TurnCompleted, turn.ResumeFinished, 0, true},
		{"cancelled run is finished and unsettled", func(h *harness, r run.RunID) { h.appCancel(r) }, turn.TurnAttemptFailed, turn.ResumeFinished, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, factory(t))
			resp := h.start("t1", "in-1")
			tc.arrange(h, resp.RunID)
			st := h.status("t1")
			if st.Status != tc.status || st.Disposition != tc.disposition || len(st.Waiting) != tc.waiting || (st.End != nil) != tc.ended || st.RunID != resp.RunID {
				t.Fatalf("status = %+v", st)
			}
			if tc.waiting > 0 && st.Waiting[0].Kind != run.ResponseApproval {
				t.Fatalf("waiting = %+v", st.Waiting)
			}
		})
	}
	h := newHarness(t, factory(t))
	if _, err := h.c.Status(h.ctx, h.ref("nope")); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("status of unknown turn = %v, want conflict", err)
	}
}

// --- surface 投影（TRN-PRJ-1、TRN-EVT-3、TRN-SCP-3） ------------------------------------

func testProjection(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")

	// A Run no attempt of this Session's Turns binds is not folded.
	foreign, err := run.BuildNewRun("r-foreign", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(foreign, nil)
	if err != nil {
		t.Fatal(err)
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: h.now, Value: runmod.Event{RunID: "r-foreign", Fact: f}})
	}
	h.mustApply(writer.SemanticGroup{CommitID: "foreign-run", Batches: []writer.TypedBatch{
		{Stream: runmod.Stream("r-foreign"), Events: runEvents},
	}})
	surface := h.surface()
	if len(surface.Turns) != 1 || surface.RunOwner["r-foreign"] != "" {
		t.Fatalf("foreign run entered the surface: %+v", surface)
	}

	// TRN-EVT-3: a second started, a settlement of an unknown Turn and a second
	// settlement are refused by the fold before anything is written.
	failed := func(turnID turn.TurnID, s turn.Settlement) writer.TypedEvent {
		return writer.TypedEvent{Type: turn.TypeFailed, RecordedAtUnixMilli: h.now,
			Value: turn.FailedPayload{TurnID: turnID, RunID: resp.RunID, Settlement: s, FailureClass: "x"}}
	}
	h.appCancel(resp.RunID)
	h.mustApply(writer.SemanticGroup{CommitID: "settle-t1", Batches: []writer.TypedBatch{
		{Stream: turn.Stream("t1"), Events: []writer.TypedEvent{failed("t1", turn.SettlementFailed)}},
	}})
	before := h.head()
	rejects := []struct {
		name   string
		turnID turn.TurnID
		event  writer.TypedEvent
	}{
		{"started twice", "t1", writer.TypedEvent{Type: turn.TypeStarted, RecordedAtUnixMilli: h.now,
			Value: turn.StartedPayload{TurnID: "t1", Preset: preset}}},
		{"failed for an unknown turn", "ghost", failed("ghost", turn.SettlementFailed)},
		{"settled twice", "t1", failed("t1", turn.SettlementStopped)},
	}
	for i, tc := range rejects {
		res := h.commit(writer.SemanticGroup{CommitID: session.CommitID("reject-" + string(rune('a'+i))), Batches: []writer.TypedBatch{
			{Stream: turn.Stream(tc.turnID), Events: []writer.TypedEvent{tc.event}},
		}})
		if res.Outcome != writer.CommitInvalid {
			t.Fatalf("%s: outcome = %s, want invalid", tc.name, res.Outcome)
		}
	}
	if h.head() != before {
		t.Fatal("a refused turn event was written")
	}
	if v := h.surface().Turns["t1"]; v.Status != turn.TurnFailed || v.ActiveRun != "" || v.Attempts[0].End == nil {
		t.Fatalf("view after run_ended and settlement = %+v", v)
	}
	if _, stopped := h.surface().Turns["t1"].Attempts[0].End.End.(run.RunStoppedEnd); !stopped {
		t.Fatalf("end = %T", h.surface().Turns["t1"].Attempts[0].End.End)
	}

	// A completed Run settles the Turn from its own run_ended.
	resp2 := h.start("t2", "in-2")
	h.complete(resp2.RunID)
	if v := h.surface().Turns["t2"]; v.Status != turn.TurnCompleted || v.ActiveRun != "" {
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

// --- recovery（TRN-REC-1/2、TRN-SCP-3） -----------------------------------------------

func testRecovery(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	resp := h.start("t1", "in-1")

	// started committed, process gone before driving: the new owner has
	// nothing to dispose and the Turn is still active from the projections
	// alone (TRN-SCP-3).
	h.takeover()
	if n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true}); err != nil || n != 0 {
		t.Fatalf("recover after start = %d %v", n, err)
	}
	if st := h.status("t1"); st.Status != turn.TurnActive || st.RunID != resp.RunID || st.Disposition != "" {
		t.Fatalf("status after takeover = %+v", st)
	}

	// Model executing when the owner dies: the takeover disposes it and the
	// Turn stays active; the superseded Coordinator is fenced.
	h.executingModel(resp.RunID)
	if st := h.status("t1"); st.Disposition != turn.ResumeWaitingForRecovery {
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
	if st.Status != turn.TurnActive || st.Disposition == turn.ResumeWaitingForRecovery {
		t.Fatalf("status after recovery = %+v", st)
	}
	if _, err := old.Deliver(h.ctx, oldWriter, turn.DeliverRequest{Ref: h.ref("t1"), Inputs: late}); !errors.Is(err, runtime.ErrOwnershipLost) {
		t.Fatalf("superseded coordinator deliver = %v, want ownership lost", err)
	}
	if v, _ := h.chat().Inputs.Get("late"); v.Status != chatlog.InputSubmitted {
		t.Fatalf("fenced deliver changed the input: %s", v.Status)
	}
	if _, err := h.c.Deliver(h.ctx, h.writer(), turn.DeliverRequest{Ref: h.ref("t1"), Inputs: late}); err != nil {
		t.Fatalf("owner deliver after takeover: %v", err)
	}
}
