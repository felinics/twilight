package owner

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

// A generation that is still closing keeps Open out until its release has
// completed (OWN-HDL-1): the table entry, not the resources, decides.
func TestOpenRefusesWhileClosing(t *testing.T) {
	a := &Owner{open: map[session.SessionID]*openSession{"s": {state: closing}}}
	if _, err := a.Open(context.Background(), "s"); !errors.Is(err, ErrSessionOpen) {
		t.Fatalf("open during closing = %v, want ErrSessionOpen", err)
	}
	a.open["s"].state = opening
	if _, err := a.Open(context.Background(), "s"); !errors.Is(err, ErrSessionOpen) {
		t.Fatalf("open during opening = %v, want ErrSessionOpen", err)
	}
	// A Handle of a generation that is not the table's current one releases
	// nothing, and neither does one whose generation is already closing.
	stale := &openSession{state: open}
	if got := a.beginClose("s", stale); got != nil {
		t.Fatal("stale generation began a close")
	}
	a.open["s"].state = closing
	if got := a.beginClose("s", a.open["s"]); got != nil {
		t.Fatal("a closing generation began a second close")
	}
}
