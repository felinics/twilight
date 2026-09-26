package effect

import "testing"

func TestExecutionStatusTerminal(t *testing.T) {
	for _, status := range []ExecutionStatus{ExecutionCompleted, ExecutionFailed, ExecutionCancelled, ExecutionUnknown} {
		if !status.Terminal() {
			t.Errorf("%q is not terminal", status)
		}
	}
	for _, status := range []ExecutionStatus{ExecutionNotFound, ExecutionAccepted, ExecutionDispatching, ExecutionRunning, ExecutionCancelRequested} {
		if status.Terminal() {
			t.Errorf("%q is unexpectedly terminal", status)
		}
	}
}

func TestAttachmentStateVocabulary(t *testing.T) {
	for _, state := range []AttachmentState{AttachmentMissing, AttachmentActive, AttachmentOrphaned, AttachmentTerminal} {
		if !state.Valid() {
			t.Errorf("%q is not valid", state)
		}
	}
	if !AttachmentTerminal.Terminal() || AttachmentActive.Terminal() {
		t.Fatal("attachment terminal predicate is incorrect")
	}
	if state := AttachmentState("unknown"); state.Valid() || state.Terminal() {
		t.Fatalf("unknown attachment state accepted: %q", state)
	}
}
