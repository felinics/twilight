package loop_test

import (
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// RequireWaiting checks Loop yielded and a call is waiting for kind.
func (f *Feature) RequireWaiting(kind run.ResponseKind) {
	f.t.Helper()
	if f.last.Disposition != loop.LoopWaiting || f.last.ExecutionRecovery {
		f.t.Fatalf("loop = %+v, want Waiting", f.last)
	}
	w := f.waiting()
	if w.Kind != kind {
		f.t.Fatalf("waiting kind = %s, want %s", w.Kind, kind)
	}
	want := schema.Identity().DeriveResponseID(w.RunID, w.StepID, w.CallID, w.Kind)
	if w.ID != want {
		f.t.Fatalf("ResponseID = %q, want derived %q", w.ID, want)
	}
}

// RequireWaitingProvider checks the waiting call is the one the model issued
// under providerID.
func (f *Feature) RequireWaitingProvider(providerID string) {
	f.t.Helper()
	if got, want := f.waiting().CallID, f.callByProvider(providerID); got != want {
		f.t.Fatalf("waiting call = %s, want %s (provider %s)", got, want, providerID)
	}
}

// RequireCompleted checks the Run finished and that the last ModelStepCompleted
// names the model result carrying text: the fact keeps only the digest, so the
// check digests the result the scripted invoker actually returned.
func (f *Feature) RequireCompleted(text string) {
	f.t.Helper()
	s := f.state()
	if s.Status != run.RunCompleted || s.Result == nil {
		f.t.Fatalf("state = %+v", s)
	}
	if f.loop != nil && f.last.Disposition != loop.LoopFinished {
		f.t.Fatalf("loop = %+v, want Finished", f.last)
	}
	if f.invoker == nil {
		return
	}
	last, ok := f.invoker.lastResult()
	if !ok || last.Text != text {
		f.t.Fatalf("last model result = %+v, want text %q", last, text)
	}
	frozen, err := sdkconv.FreezeModelResult(last)
	if err != nil {
		f.t.Fatal(err)
	}
	want, err := schema.Canonical().DigestModelResult(frozen)
	if err != nil {
		f.t.Fatal(err)
	}
	var got run.Digest
	for _, fact := range f.facts() {
		if c, ok := fact.(run.ModelStepCompleted); ok {
			got = c.ResultDigest
		}
	}
	if got != want {
		f.t.Fatalf("last ModelStepCompleted.ResultDigest = %s, want digest of %q (%s)", got, text, want)
	}
}

// RequireStopped checks the Run stopped as cancelled.
func (f *Feature) RequireStopped() {
	f.t.Helper()
	s := f.state()
	if s.Status != run.RunStopped || s.Result == nil || s.Result.Reason != run.ReasonCancelled {
		f.t.Fatalf("state = %+v, want stopped cancelled", s)
	}
}

// RequireActive checks the Run is still active.
func (f *Feature) RequireActive() {
	f.t.Helper()
	if f.state().Status != run.RunActive {
		f.t.Fatalf("status = %v, want active", f.state().Status)
	}
}

// RequirePrepared checks the current step is a prepared ModelStep.
func (f *Feature) RequirePrepared() {
	f.t.Helper()
	ms, ok := f.state().Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelPrepared {
		f.t.Fatalf("current = %+v, want prepared model", f.state().Current)
	}
}

// RequireOpen checks the Run is at the enterable Open interval.
func (f *Feature) RequireOpen() {
	f.t.Helper()
	if _, ok := f.state().Current.(run.Open); !ok {
		f.t.Fatalf("current = %+v, want Open", f.state().Current)
	}
}

// RequireCallPending checks the named call on the current ToolStep is Pending.
func (f *Feature) RequireCallPending(id run.CallID) {
	f.t.Helper()
	id = f.callByProvider(string(id))
	ts, ok := f.state().Current.(run.ToolStep)
	if !ok {
		f.t.Fatalf("current = %T, want ToolStep", f.state().Current)
	}
	for _, call := range ts.Calls {
		if call.CallID == id {
			if call.Status != run.ToolPending {
				f.t.Fatalf("call %s status = %v, want Pending", id, call.Status)
			}
			return
		}
	}
	f.t.Fatalf("call %s not on current ToolStep", id)
}

// RequireNotRan checks the tool has not executed.
func (f *Feature) RequireNotRan(name string) {
	f.t.Helper()
	tool := f.tools[run.ToolRef(name)]
	if tool == nil {
		f.t.Fatalf("tool %q not registered", name)
		return
	}
	if tool.ran.Load() != 0 {
		f.t.Fatalf("tool %q ran %d times", name, tool.ran.Load())
	}
}

// RequireRan checks the tool executed at least once.
func (f *Feature) RequireRan(name string) {
	f.t.Helper()
	tool := f.tools[run.ToolRef(name)]
	if tool == nil {
		f.t.Fatalf("tool %q not registered", name)
		return
	}
	if tool.ran.Load() == 0 {
		f.t.Fatalf("tool %q never ran", name)
	}
}

// RequireModelCalls checks how many times the scripted invoker was called.
func (f *Feature) RequireModelCalls(n int) {
	f.t.Helper()
	if f.invoker == nil {
		f.t.Fatal("no Loop invoker")
	}
	if got := int(f.invoker.calls.Load()); got != n {
		f.t.Fatalf("model calls = %d, want %d", got, n)
	}
}

// RequireUsage checks accumulated total tokens on the Run result.
func (f *Feature) RequireUsage(total int) {
	f.t.Helper()
	s := f.state()
	if s.Result == nil || s.Result.Usage.TotalTokens != total {
		f.t.Fatalf("usage = %+v, want %d", s.Result, total)
	}
}

// RequireBuilderSawTool checks the next Plan was positioned after the ToolStep
// on which callID completed with output. The hint carries only the boundary
// (SourceStep); the completed call's OutputDigest is read from the Run state.
func (f *Feature) RequireBuilderSawTool(callID run.CallID, output string) {
	f.t.Helper()
	callID = f.callByProvider(string(callID))
	if f.builder == nil || f.builder.lastHint.SourceStep == "" {
		f.t.Fatal("builder hint has no SourceStep")
	}
	s := f.state()
	if s.LastToolStep == nil || s.LastToolStep.RefValue.ID != f.builder.lastHint.SourceStep {
		f.t.Fatalf("hint SourceStep = %s, LastToolStep = %+v", f.builder.lastHint.SourceStep, s.LastToolStep)
	}
	want, err := schema.Canonical().DigestToolOutput(run.MustParseCanonicalJSON(output))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, call := range s.LastToolStep.Calls {
		if call.CallID == callID && call.Status == run.ToolCompleted && call.Result != nil && call.Result.OutputDigest == want {
			return
		}
	}
	f.t.Fatalf("LastToolStep = %+v, want completed %s with output %s", s.LastToolStep.Calls, callID, output)
}

// RequireCallFailed checks a ToolCallFailed fact for this call and outcome.
func (f *Feature) RequireCallFailed(id run.CallID, outcome run.ToolFailureOutcome) {
	f.t.Helper()
	id = f.callByProvider(string(id))
	for _, fact := range f.facts() {
		failed, ok := fact.(run.ToolCallFailed)
		if ok && failed.CallID == id && failed.Outcome == outcome {
			return
		}
	}
	f.t.Fatalf("no ToolCallFailed for %s with outcome %v", id, outcome)
}

// RequireFailureClass checks a ToolCallFailed fact with class was committed.
func (f *Feature) RequireFailureClass(class string) {
	f.t.Helper()
	for _, fact := range f.facts() {
		failed, ok := fact.(run.ToolCallFailed)
		if ok && failed.Failure.Class == class {
			return
		}
	}
	f.t.Fatalf("no ToolCallFailed with class %s", class)
}

// RequireFactOpened checks ToolStepOpened was committed.
func (f *Feature) RequireFactOpened() {
	f.t.Helper()
	for _, fact := range f.facts() {
		if _, ok := fact.(run.ToolStepOpened); ok {
			return
		}
	}
	f.t.Fatal("no ToolStepOpened fact")
}

// RequireNoUncertain checks Cancel recorded no in-flight effects.
func (f *Feature) RequireNoUncertain() {
	f.t.Helper()
	s := f.state()
	if s.Result == nil {
		f.t.Fatal("no result")
	}
	if len(s.Result.UncertainCalls) != 0 || s.Result.UncertainModel != "" {
		f.t.Fatalf("uncertain = %+v", s.Result)
	}
}

// RequireUncertainCall checks Cancel projected this executing tool call.
func (f *Feature) RequireUncertainCall(id run.CallID) {
	f.t.Helper()
	id = f.callByProvider(string(id))
	s := f.state()
	if s.Result == nil {
		f.t.Fatal("no result")
	}
	for _, got := range s.Result.UncertainCalls {
		if got == id {
			return
		}
	}
	f.t.Fatalf("UncertainCalls = %v, want %s", s.Result.UncertainCalls, id)
}

// RequireUncertainModel checks Cancel projected the executing ModelStep.
func (f *Feature) RequireUncertainModel() {
	f.t.Helper()
	s := f.state()
	if s.Result == nil || s.Result.UncertainModel == "" {
		f.t.Fatalf("result = %+v, want UncertainModel", s.Result)
	}
}
