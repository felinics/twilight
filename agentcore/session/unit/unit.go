// Package unit is the one place a cross-module commit of a Session is
// assembled (SES-ATM). A Work names a CommitID and the Parts that write under
// it; every Part prepares its own module's events against the same
// transactional View, and the Writer appends them as one commit or nothing.
// No module builds another module's events: the Turn does not encode Run
// facts, the Run does not carry chatlog events. A Part that has to refuse
// (a precondition of its module fails) refuses the whole unit.
package unit

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Part is one module's contribution to a unit of work.
type Part interface {
	// Prepare runs inside the Writer's critical section against the View
	// every other Part of the unit sees. It returns the batches its module
	// writes for this commit, possibly none, or an error, which refuses the
	// unit and writes nothing. now is the commit's RecordedAtUnixMilli.
	Prepare(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error)
}

// PartFunc adapts a function to Part.
type PartFunc func(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error)

func (f PartFunc) Prepare(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	return f(ctx, view, now)
}

// Work is one atomic commit: its identity and the Parts that write under it,
// in the order their batches are laid out.
type Work struct {
	// CommitID names the operation: the writer derives it so that one
	// identity means one operation, and a second unit under it is the same
	// operation replayed (EXT-WRT-2).
	CommitID session.CommitID
	Parts    []Part
}

// Commit appends the unit through w. A CommitID the log already holds is
// CommitAlreadyApplied with the stored commit, without preparing any Part.
// Otherwise every Part prepares against one View and their batches are
// merged by stream in Part order, so the commit holds at most one batch per
// stream. A Part error is returned as is with nothing written; the Writer's
// own verdicts (CommitConflict, CommitInvalid) come back in the result.
func Commit(ctx context.Context, w writer.Writer, now int64, work Work) (writer.CommitResult, error) {
	if w == nil {
		return writer.CommitResult{}, errors.New("unit: nil writer")
	}
	if work.CommitID == "" {
		return writer.CommitResult{}, errors.New("unit: empty CommitID")
	}
	var replay *session.Commit
	res, err := w.Commit(ctx, func(view writer.View) (*writer.SemanticGroup, error) {
		if existing, found, err := view.LookupCommit(work.CommitID); err != nil {
			return nil, err
		} else if found {
			replay = &existing
			return nil, nil
		}
		group := &writer.SemanticGroup{CommitID: work.CommitID}
		index := map[session.StreamRef]int{}
		for _, p := range work.Parts {
			batches, err := p.Prepare(ctx, view, now)
			if err != nil {
				return nil, err
			}
			for _, b := range batches {
				if len(b.Events) == 0 {
					continue
				}
				if i, ok := index[b.Stream]; ok {
					group.Batches[i].Events = append(group.Batches[i].Events, b.Events...)
					continue
				}
				index[b.Stream] = len(group.Batches)
				group.Batches = append(group.Batches, writer.TypedBatch{Stream: b.Stream, Events: append([]writer.TypedEvent(nil), b.Events...)})
			}
		}
		if len(group.Batches) == 0 {
			return nil, errors.New("unit: no part wrote an event")
		}
		return group, nil
	})
	if err != nil {
		return writer.CommitResult{}, err
	}
	if replay != nil {
		return writer.CommitResult{Outcome: writer.CommitAlreadyApplied, Commit: *replay}, nil
	}
	return res, nil
}

// Events returns the events of a commit in batch order.
func Events(c session.Commit) []session.Event {
	var out []session.Event
	for _, b := range c.Batches {
		out = append(out, b.Events...)
	}
	return out
}
