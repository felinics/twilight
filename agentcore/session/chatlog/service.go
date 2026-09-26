package chatlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Commands are the chatlog's canonical commands -- submitting and
// withdrawing inputs, committing compactions -- each through the Writer the
// caller owns (CHT-EVT, OWN-HDL-2). They are the only writer of chatlog
// facts: callers go through these methods instead of building chatlog
// TypedEvents by hand.
type Commands struct {
	Now func() time.Time
}

// Guard is a caller-supplied precondition evaluated inside a command's
// commit critical section, on the same View the command reads. Compaction
// uses it for the "no Turn may be active" rule (APP-CKP-1), which is turn
// domain policy this package cannot import.
type Guard func(v writer.View) error

// ErrNotSubmitted reports a withdraw of an input that is not in the
// submitted state (CHT-EVT-2).
var ErrNotSubmitted = errors.New("chatlog: input is not submitted")

// Submit records one user input as submitted (CHT-EVT-1): content is the
// input's body, opaque to this module and digested as given (the agent
// decides its shape, DEC-INP-1). Idempotency rides on the CommitID, so a
// retried submission replays.
func (s *Commands) Submit(ctx context.Context, w writer.Writer, id run.InputID, content run.CanonicalJSON) (run.AgentInput, error) {
	if content.IsZero() {
		return run.AgentInput{}, errors.New("chatlog: submit requires input content")
	}
	digest, err := DigestInput(InputID(id), content)
	if err != nil {
		return run.AgentInput{}, err
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)),
			Batches: []writer.TypedBatch{{Stream: Stream, Events: []writer.TypedEvent{{
				Type: TypeInputSubmitted, RecordedAtUnixMilli: s.Now().UnixMilli(),
				Value: InputSubmittedPayload{InputID: InputID(id), Content: content, SubmittedAtUnixMilli: s.Now().UnixMilli()},
			}}}}}, nil
	})
	if err != nil {
		return run.AgentInput{}, err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return run.AgentInput{ID: id, Digest: digest}, nil
	default:
		return run.AgentInput{}, fmt.Errorf("chatlog: submit input: %s: %s", res.Outcome, res.Detail)
	}
}

// Withdraw marks a submitted, undelivered input as withdrawn
// (CHT-EVT-2), for example the original input of a Turn the caller forked
// before in order to edit it (OWN-FRK-2).
func (s *Commands) Withdraw(ctx context.Context, w writer.Writer, id run.InputID, reason string) error {
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		state, err := v.Projection(SurfaceProjectionID, SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		surface, ok := state.(Surface)
		if !ok {
			return nil, fmt.Errorf("chatlog: surface projection is %T", state)
		}
		view, ok := surface.Inputs.Get(InputID(id))
		if !ok || view.Status != InputSubmitted {
			return nil, fmt.Errorf("%w: input %s is not a submitted input", ErrNotSubmitted, id)
		}
		return &writer.SemanticGroup{CommitID: session.CommitID("input-withdrawn/" + string(id)),
			Batches: []writer.TypedBatch{{Stream: Stream, Events: []writer.TypedEvent{{
				Type: TypeInputWithdrawn, RecordedAtUnixMilli: s.Now().UnixMilli(),
				Value: InputWithdrawnPayload{InputID: InputID(id), Reason: reason},
			}}}}}, nil
	})
	if err != nil {
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return nil
	default:
		return fmt.Errorf("chatlog: withdraw input: %s: %s", res.Outcome, res.Detail)
	}
}

// Compaction commits a summary and its compaction in one group (CHT-EVT-3).
// The base is read inside the commit's critical section, so the digest pins
// exactly the context being replaced. guard (when non-nil) runs in the same
// critical section first; callers pass the active-Turn rule there.
// retain names entries of the current context (compaction.RetainLast builds
// a pair-closed suffix, which CheckRetainClosure verifies).
func (s *Commands) Compact(ctx context.Context, w writer.Writer, summaryText string, retain []EntryDigestPair, guard Guard) (CompactionID, error) {
	if strings.TrimSpace(summaryText) == "" {
		return "", errors.New("chatlog: compaction requires a summary text")
	}
	var compactionID CompactionID
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		if guard != nil {
			if err := guard(v); err != nil {
				return nil, err
			}
		}
		cstate, err := v.Projection(ContextProjectionID, ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		cctx, ok := cstate.(Context)
		if !ok {
			return nil, fmt.Errorf("chatlog: context projection is %T", cstate)
		}
		entries := cctx.Entries
		if len(entries) == 0 {
			return nil, errors.New("chatlog: compaction over an empty context")
		}
		if err := CheckRetainClosure(entries, retain); err != nil {
			return nil, err
		}
		pairs := make([]EntryDigestPair, len(entries))
		for i := range entries {
			pairs[i] = entries[i].Pair()
		}
		baseDigest, err := DigestBaseContext(pairs)
		if err != nil {
			return nil, err
		}
		var summaryID SummaryID
		if compactionID, summaryID, err = compactionIDs(w.SessionID(), baseDigest, summaryText); err != nil {
			return nil, err
		}
		summary := Summary{ID: summaryID, Parts: Parts{TextPart{Text: summaryText}}}
		if summary.Digest, err = DigestSummary(&summary); err != nil {
			return nil, err
		}
		payload := CompactionCreatedPayload{
			CompactionID: compactionID, CoveredThrough: entries[len(entries)-1].Position,
			BaseContextDigest: baseDigest, SummaryID: summaryID, SummaryDigest: summary.Digest,
			Retained: retain,
		}
		if payload.Digest, err = DigestCompaction(&payload); err != nil {
			return nil, err
		}
		now := s.Now().UnixMilli()
		return &writer.SemanticGroup{CommitID: session.CommitID("compaction/" + string(compactionID)),
			Batches: []writer.TypedBatch{{Stream: Stream, Events: []writer.TypedEvent{
				{Type: TypeSummary, RecordedAtUnixMilli: now, Value: SummaryPayload{Summary: summary}},
				{Type: TypeCompactionCreated, RecordedAtUnixMilli: now, Value: payload},
			}}}}, nil
	})
	if err != nil {
		return "", err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return compactionID, nil
	default:
		return "", fmt.Errorf("chatlog: compaction: %s: %s", res.Outcome, res.Detail)
	}
}

// compactionIDs derives the compaction and summary identifiers from what the
// compaction replaces: the Session, the base context digest and the summary
// text. A Compaction retried over the same base therefore carries the same
// CommitID and is answered as already applied instead of writing a second
// compaction (APP-CKP-1).
// compactionDerivationVersion versions this preimage; it is chatlog's own,
// not a kernel version: the kernel has none (SES-VER-2).
const compactionDerivationVersion uint16 = 1

func compactionIDs(sid session.SessionID, base es.Digest, summaryText string) (CompactionID, SummaryID, error) {
	raw, err := es.EncodeTypedPayload(compactionDerivationVersion, "twilight/chatlog/compaction", struct {
		SessionID session.SessionID `json:"sessionId"`
		Base      es.Digest         `json:"base"`
		Summary   string            `json:"summary"`
	}{sid, base, summaryText})
	if err != nil {
		return "", "", err
	}
	// The digest is "sha256:<hex>"; the IDs carry the first 16 hex digits.
	d := string(es.DigestBytes(raw))
	const prefix = "sha256:"
	if len(d) > len(prefix) && d[:len(prefix)] == prefix {
		d = d[len(prefix):]
	}
	if len(d) > 16 {
		d = d[:16]
	}
	return CompactionID("ckpt-" + d), SummaryID("sum-" + d), nil
}

// CheckRetainClosure requires retained tool results and their issuing
// assistants to travel together, and an assistant with a call whose result
// is not yet in the context to be retained, so the compacted context stays
// valid provider input now and when that result lands (APP-CKP-2). Subset
// and order are the fold's job.
func CheckRetainClosure(entries []Entry, retain []EntryDigestPair) error {
	kept := make(map[EntryDigestPair]bool, len(retain))
	for _, p := range retain {
		kept[p] = true
	}
	owner := map[CallID]*Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind != EntryAssistant || e.Assistant == nil {
			continue
		}
		for _, call := range e.Assistant.CallIDs {
			owner[call] = e
		}
	}
	results := map[CallID]*Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind == EntryToolResult && e.ToolResult != nil {
			results[e.ToolResult.CallID] = e
		}
	}
	for i := range entries {
		e := &entries[i]
		if e.Kind == EntryAssistant && e.Assistant != nil && !kept[e.Pair()] {
			for _, call := range e.Assistant.CallIDs {
				if results[call] == nil {
					return fmt.Errorf("chatlog: assistant %s has the unsettled call %s and must be retained", e.ID, call)
				}
			}
		}
		if !kept[e.Pair()] {
			continue
		}
		switch e.Kind {
		case EntryToolResult:
			if a := owner[e.ToolResult.CallID]; a != nil && !kept[a.Pair()] {
				return fmt.Errorf("chatlog: retained tool_result %s without its assistant", e.ID)
			}
		case EntryAssistant:
			for _, call := range e.Assistant.CallIDs {
				if r := results[call]; r != nil && !kept[r.Pair()] {
					return fmt.Errorf("chatlog: retained assistant %s without the result of call %s", e.ID, call)
				}
			}
		}
	}
	return nil
}

// DeliverInputs is the chatlog's Part of a Turn's start or delivery unit
// (CHT-EVT, TRN-DLV-1): it checks, on the unit's own View, that every input
// is a submitted chatlog Input whose content digest is the one the Run
// accepts, and writes one input_delivered per input. The body stays here;
// the Run carries only the digest. A failed check is
// ErrNotSubmitted and refuses the whole unit, so an input withdrawn between
// the caller's read and the commit is caught inside the critical section.
func DeliverInputs(turnID TurnID, inputs []run.AgentInput) unit.Part {
	return deliverInputs{turnID: turnID, inputs: inputs}
}

type deliverInputs struct {
	turnID TurnID
	inputs []run.AgentInput
}

func (d deliverInputs) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	if len(d.inputs) == 0 {
		return nil, nil
	}
	state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return nil, err
	}
	surface, ok := state.(Surface)
	if !ok {
		return nil, fmt.Errorf("chatlog: surface projection is %T", state)
	}
	events := make([]writer.TypedEvent, 0, len(d.inputs))
	for _, in := range d.inputs {
		v, ok := surface.Inputs.Get(InputID(in.ID))
		if !ok || v.Status != InputSubmitted {
			return nil, fmt.Errorf("%w: input %s is not a submitted input", ErrNotSubmitted, in.ID)
		}
		if v.Input.Digest != in.Digest {
			return nil, fmt.Errorf("%w: input %s digest differs from its submitted content", ErrNotSubmitted, in.ID)
		}
		events = append(events, writer.TypedEvent{Type: TypeInputDelivered, RecordedAtUnixMilli: now,
			Value: InputDeliveredPayload{InputID: InputID(in.ID), TurnID: d.turnID}})
	}
	return []writer.TypedBatch{{Stream: Stream, Events: events}}, nil
}
