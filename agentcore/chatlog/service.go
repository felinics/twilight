package chatlog

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"strings"
	"time"
)

// Commands are the chatlog's canonical commands -- submitting and
// withdrawing inputs, committing compactions -- each through the Writer the
// caller owns. They are the only writer of chatlog
// facts: callers go through these methods instead of building chatlog
// TypedEvents by hand.
type Commands struct {
	Now func() time.Time
}

// Guard is a caller-supplied precondition evaluated inside a command's
// commit critical section, on the same View the command reads. Compaction
// uses it for the "no Turn may be active" rule, which is turn
// domain policy this package cannot import.
type Guard func(v writer.View) error

// ErrNotSubmitted reports a withdraw of an input that is not in the
// submitted state.
var ErrNotSubmitted = errors.New("chatlog: input is not submitted")

// Submit records one user input as submitted: content is the
// input's body, opaque to this module and digested as given (the agent decides its shape). Idempotency rides on the CommitID, so a
// retried submission replays.
func (s *Commands) Submit(ctx context.Context, w writer.Writer, id turn.InputID, content turn.CanonicalJSON) (turn.AgentInput, error) {
	if content.IsZero() {
		return turn.AgentInput{}, errors.New("chatlog: submit requires input content")
	}
	digest, err := DigestInput(InputID(id), content)
	if err != nil {
		return turn.AgentInput{}, err
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: ledger.CommitID("input-submitted/" + string(id)),
			Batches: []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{
				Type: TypeInputSubmitted, RecordedAtUnixMilli: s.Now().UnixMilli(),
				Value: InputSubmittedPayload{InputID: InputID(id), Content: content, SubmittedAtUnixMilli: s.Now().UnixMilli()},
			}}}}}, nil
	})
	if err != nil {
		return turn.AgentInput{}, err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return turn.AgentInput{ID: id, Digest: digest}, nil
	default:
		return turn.AgentInput{}, fmt.Errorf("chatlog: submit input: %s: %s", res.Outcome, res.Detail)
	}
}

// Withdraw marks a submitted, undelivered input as withdrawn
// for example the original input of a Turn the caller forked
// before in order to edit it.
func (s *Commands) Withdraw(ctx context.Context, w writer.Writer, id turn.InputID, reason string) error {
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
		return &writer.SemanticGroup{CommitID: ledger.CommitID("input-withdrawn/" + string(id)),
			Batches: []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{
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

// Compact commits a summary against the plan captured before its generation.
// The guard and CompactContext prepare on the same transactional View. Reusing
// the same plan and summary replays the operation without revalidating its base.
func (s *Commands) Compact(ctx context.Context, w writer.Writer, summaryText string, plan CompactionPlan, guard Guard) (CompactionID, error) {
	if strings.TrimSpace(summaryText) == "" {
		return "", errors.New("chatlog: compaction requires a summary text")
	}
	if w == nil {
		return "", errors.New("chatlog: compaction requires a writer")
	}
	compactionID, summaryID, err := compactionIDs(w.SessionID(), plan, summaryText)
	if err != nil {
		return "", err
	}
	summary := Summary{ID: summaryID, Parts: Parts{TextPart{Text: summaryText}}}
	if summary.Digest, err = DigestSummary(&summary); err != nil {
		return "", err
	}
	guardPart := unit.PartFunc(func(_ context.Context, v writer.View, _ int64) ([]writer.TypedBatch, error) {
		if guard != nil {
			return nil, guard(v)
		}
		return nil, nil
	})
	res, err := unit.Commit(ctx, w, s.Now().UnixMilli(), unit.Work{
		CommitID: ledger.CommitID("compaction/" + string(compactionID)),
		Parts:    []unit.Part{guardPart, CompactContext(compactionID, summary, plan)},
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

// compactionIDs derives both identifiers from the Session, the complete plan
// (including retained entries), and summary text. An identical retry carries
// the same CommitID; a different retention policy is a different operation.
// compactionDerivationVersion versions this preimage; it is chatlog's own,
// not a kernel version: the kernel has none.
const compactionDerivationVersion uint16 = 2

func compactionIDs(sid session.SessionID, plan CompactionPlan, summaryText string) (CompactionID, SummaryID, error) {
	raw, err := jsonstable.EncodeTypedPayload(compactionDerivationVersion, "twilight/chatlog/compaction", struct {
		SessionID session.SessionID `json:"sessionId"`
		Plan      CompactionPlan    `json:"plan"`
		Summary   string            `json:"summary"`
	}{sid, plan, summaryText})
	if err != nil {
		return "", "", err
	}
	// The digest is "sha256:<hex>"; the IDs carry the first 16 hex digits.
	d := string(jsonstable.DigestBytes(raw))
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
// valid provider input now and when that result lands. Subset
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

// AdmitInputs validates submitted bodies on the same writer view as acceptance.
// InputAccepted is the sole durable fact that admits input to a Turn.
func AdmitInputs(inputs []turn.AgentInput) unit.Part { return admitInputs{inputs: inputs} }

type admitInputs struct{ inputs []turn.AgentInput }

func (d admitInputs) Prepare(_ context.Context, view writer.View, _ int64) ([]writer.TypedBatch, error) {
	state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return nil, err
	}
	surface, ok := state.(Surface)
	if !ok {
		return nil, fmt.Errorf("chatlog: surface projection is %T", state)
	}
	seen := map[turn.InputID]bool{}
	for _, in := range d.inputs {
		v, ok := surface.Inputs.Get(InputID(in.ID))
		if in.ID == "" || seen[in.ID] || !ok || v.Status != InputSubmitted || v.Input.Digest != in.Digest {
			return nil, fmt.Errorf("%w: input %s is duplicate, unavailable or has different content", ErrNotSubmitted, in.ID)
		}
		seen[in.ID] = true
	}
	return nil, nil
}
