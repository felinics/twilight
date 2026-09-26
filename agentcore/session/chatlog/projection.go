package chatlog

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

const (
	SurfaceProjectionID extension.ProjectionID = "twilight/chatlog/surface"
	ContextProjectionID extension.ProjectionID = "twilight/chatlog/context"
)

type InputStatus string

const (
	InputSubmitted InputStatus = "submitted"
	InputDelivered InputStatus = "delivered"
	InputWithdrawn InputStatus = "withdrawn"
	InputRejected  InputStatus = "rejected"
)

type InputView struct {
	Input  Input       `json:"input"`
	Status InputStatus `json:"status"`
	// Position is the ledger position of the input_submitted event; it orders
	// inputs by submission without a projection-owned counter.
	Position session.Position `json:"position"`
}

type EntryKind string

const (
	EntryInput      EntryKind = "input"
	EntryAssistant  EntryKind = "assistant"
	EntryToolResult EntryKind = "tool_result"
	EntrySummary    EntryKind = "summary"
)

type SurfaceEntry struct {
	Kind     EntryKind        `json:"kind"`
	ID       string           `json:"id"`
	Position session.Position `json:"position"`
}

type CompactionStatus string

const (
	CompactionActive      CompactionStatus = "active"
	CompactionInvalidated CompactionStatus = "invalidated"
)

// CompactionView records one compaction for readers; compaction never touches
// EntryOrder or the input queue (CHT-SUR-1).
type CompactionView struct {
	Compaction CompactionCreatedPayload `json:"compaction"`
	Status     CompactionStatus         `json:"status"`
	Reason     string                   `json:"reason,omitempty"`
	Position   session.Position         `json:"position"`
}

// Surface is the UI-facing read model (CHT-SUR-1). Every table is persistent
// (Table): a fold shares them between states and pays O(sqrt(n)) per write.
// Assistants and ToolResults are structural: they name frozen bodies by
// digest and are rendered through Materialize.
type Surface struct {
	Inputs      Table[InputID, InputView]           `json:"inputs"`
	Assistants  Table[AssistantID, Assistant]       `json:"assistants"`
	ToolResults Table[ToolResultID, ToolResult]     `json:"toolResults"`
	Summaries   Table[SummaryID, Summary]           `json:"summaries"`
	EntryOrder  []SurfaceEntry                      `json:"entryOrder"`
	Superseded  Table[ToolResultID, ToolResultID]   `json:"superseded,omitzero"`
	Compactions Table[CompactionID, CompactionView] `json:"compactions,omitzero"`
	// Runs maps each active Run of the Session to its Turn (attempt/started)
	// and schema version so the
	// entries folded from Run facts carry their TurnID.
	Runs Table[run.RunID, RunOwner] `json:"runs,omitzero"`
}

// SubmittedInputs returns inputs still awaiting delivery, in submission order.
func (s *Surface) SubmittedInputs() []Input {
	var out []InputView
	s.Inputs.Range(func(_ InputID, v InputView) bool {
		if v.Status == InputSubmitted {
			out = append(out, v)
		}
		return true
	})
	sortViews(out)
	inputs := make([]Input, len(out))
	for i := range out {
		inputs[i] = out[i].Input
	}
	return inputs
}

func sortViews(views []InputView) {
	for i := 1; i < len(views); i++ {
		for j := i; j > 0 && views[j].Position.Less(views[j-1].Position); j-- {
			views[j], views[j-1] = views[j-1], views[j]
		}
	}
}

// chatlogConsumes is what both projections fold: the module's own facts and
// the Run facts that produce assistant and tool_result entries, and the
// attempt binding that names their Turn (CHT-SCP-1).
var chatlogConsumes = func() []session.EventType {
	out := make([]session.EventType, 0, 9+len(consumedRunFacts))
	out = append(out, TypeInputSubmitted, TypeInputDelivered, TypeInputWithdrawn, TypeInputRejected,
		TypeToolResultSuperseded, TypeSummary, TypeCompactionCreated, TypeCompactionInvalidated, attempt.TypeStarted)
	for _, name := range consumedRunFacts {
		out = append(out, runmod.Type(name))
	}
	return out
}()

var SurfaceProjection = extension.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: chatlogConsumes,
	// Assistant and tool_result entries are projected from run facts
	// (CHT-SCP-1); a fork inherits that conversation content (EXT-PRJ-8).
	Inherits:      extension.InheritAll,
	Authoritative: true,
	Initial: func() (any, error) {
		return Surface{}, nil
	},
	Apply:      applySurface,
	StateCodec: extension.JSONStateCodec[Surface]{},
}

// applySurface is copy-on-write: the Surface value is copied, every table is
// shared with the previous state and Set returns a new one, and EntryOrder
// grows by append. Apply stays pure -- the previous state is never written.
// Every position an entry carries is the ledger Position of the event that
// produced it (EXT-PRJ-1): the projection keeps no counter and a snapshot
// restores without rescanning.
func applySurface(state any, e extension.DecodedEvent) (any, error) { //nolint:gocritic // hugeParam: projection Apply is copy-on-write over value states; DecodedEvent is the extension API shape
	s, ok := state.(Surface)
	if !ok {
		return nil, fmt.Errorf("chatlog surface: state is %T", state)
	}
	pos := e.Position
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		if s.Inputs.Has(p.InputID) {
			return nil, fmt.Errorf("input %s submitted twice", p.InputID)
		}
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		s.Inputs = s.Inputs.Set(p.InputID, InputView{Input: Input{ID: p.InputID, Content: p.Content, Digest: d}, Status: InputSubmitted, Position: pos})
	case InputDeliveredPayload:
		v, ok := s.Inputs.Get(p.InputID)
		if !ok || v.Status != InputSubmitted {
			return nil, fmt.Errorf("input %s delivered while %s", p.InputID, v.Status)
		}
		v.Status = InputDelivered
		v.Input.TurnID = p.TurnID
		s.Inputs = s.Inputs.Set(p.InputID, v)
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryInput, ID: string(p.InputID), Position: pos})
	case InputWithdrawnPayload:
		if err := terminateInput(&s, p.InputID, InputWithdrawn); err != nil {
			return nil, err
		}
	case InputRejectedPayload:
		if err := terminateInput(&s, p.InputID, InputRejected); err != nil {
			return nil, err
		}
	case attempt.StartedPayload:
		s.Runs = s.Runs.Set(p.RunID, RunOwner{TurnID: TurnID(p.TurnID)})
	case runmod.Event:
		return s.applyRun(p, e.Position)
	case ToolResultSupersededPayload:
		old, ok := s.ToolResults.Get(p.ToolResultID)
		if !ok {
			return nil, fmt.Errorf("superseded tool_result %s unknown", p.ToolResultID)
		}
		if s.Superseded.Has(p.ToolResultID) {
			return nil, fmt.Errorf("tool_result %s superseded twice", p.ToolResultID)
		}
		replacement, err := supersedingResult(&old, &p)
		if err != nil {
			return nil, err
		}
		s.ToolResults = s.ToolResults.Set(replacement.ID, replacement)
		s.Superseded = s.Superseded.Set(p.ToolResultID, replacement.ID)
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryToolResult, ID: string(replacement.ID), Position: pos})
	case SummaryPayload:
		if s.Summaries.Has(p.Summary.ID) {
			return nil, fmt.Errorf("summary %s created twice", p.Summary.ID)
		}
		s.Summaries = s.Summaries.Set(p.Summary.ID, p.Summary)
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntrySummary, ID: string(p.Summary.ID), Position: pos})
	case CompactionCreatedPayload:
		if s.Compactions.Has(p.CompactionID) {
			return nil, fmt.Errorf("compaction %s created twice", p.CompactionID)
		}
		sum, ok := s.Summaries.Get(p.SummaryID)
		if !ok || sum.Digest != p.SummaryDigest {
			return nil, fmt.Errorf("compaction %s names summary %s which does not match", p.CompactionID, p.SummaryID)
		}
		s.Compactions = s.Compactions.Set(p.CompactionID, CompactionView{Compaction: p, Status: CompactionActive, Position: pos})
	case CompactionInvalidatedPayload:
		v, ok := s.Compactions.Get(p.CompactionID)
		if !ok || v.Status != CompactionActive {
			return nil, fmt.Errorf("compaction %s invalidated while not active", p.CompactionID)
		}
		v.Status = CompactionInvalidated
		v.Reason = p.Reason
		s.Compactions = s.Compactions.Set(p.CompactionID, v)
	default:
		return nil, fmt.Errorf("chatlog surface: unexpected %T", e.Value)
	}
	return s, nil
}

// applyRun folds one Run fact into the Surface (CHT-ENT-1, CHT-ENT-2). The
// Run's Turn is remembered from run_created until run_ended: no fact of the
// Run follows its end, so the table is bounded by the Runs active now.
func (s Surface) applyRun(ev runmod.Event, pos session.Position) (any, error) { //nolint:gocritic // hugeParam: projection Apply is copy-on-write over value states; DecodedEvent is the extension API shape
	switch f := ev.Fact.(type) {
	case run.RunCreated:
		// The Run's Turn comes from attempt/started, not from the Run.
	case run.RunEnded:
		s.Runs = s.Runs.Delete(ev.RunID)
	case run.ModelStepCompleted:
		owner, _ := s.Runs.Get(ev.RunID)
		a, err := assistantOf(owner, ev.RunID, &f)
		if err != nil {
			return nil, err
		}
		if s.Assistants.Has(a.ID) {
			return nil, fmt.Errorf("assistant %s created twice", a.ID)
		}
		s.Assistants = s.Assistants.Set(a.ID, a)
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryAssistant, ID: string(a.ID), Position: pos})
	case run.ToolStepOpened:
		a, ok := s.Assistants.Get(AssistantIDFor(f.Source))
		if !ok {
			return nil, fmt.Errorf("tool step %s opened for unknown assistant %s", f.StepID, f.Source)
		}
		if err := attachCalls(&a, &f); err != nil {
			return nil, err
		}
		s.Assistants = s.Assistants.Set(a.ID, a)
	case run.ToolCallCompleted, run.ToolCallAnswered, run.ToolCallFailed:
		owner, _ := s.Runs.Get(ev.RunID)
		r, err := toolResultOf(owner.TurnID, ev.RunID, ev.Fact)
		if err != nil {
			return nil, err
		}
		if s.ToolResults.Has(r.ID) {
			return nil, fmt.Errorf("tool_result %s created twice", r.ID)
		}
		s.ToolResults = s.ToolResults.Set(r.ID, r)
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryToolResult, ID: string(r.ID), Position: pos})
	default:
		return nil, fmt.Errorf("chatlog surface: unexpected run fact %T", ev.Fact)
	}
	return s, nil
}

// assistantOf projects ModelStepCompleted into a structural entry.
func assistantOf(owner RunOwner, runID run.RunID, f *run.ModelStepCompleted) (Assistant, error) {
	a := Assistant{ID: AssistantIDFor(f.StepID), TurnID: owner.TurnID, RunID: runID, StepID: f.StepID,
		FinishReason: f.FinishReason, ResultDigest: f.ResultDigest}
	d, err := DigestAssistant(&a)
	if err != nil {
		return Assistant{}, err
	}
	a.Digest = d
	return a, nil
}

// attachCalls records the CallIDs ToolStepOpened assigned, in the result's
// ToolCalls order, on the assistant that issued them.
func attachCalls(a *Assistant, f *run.ToolStepOpened) error {
	if len(a.CallIDs) != 0 {
		return fmt.Errorf("assistant %s opened a tool step twice", a.ID)
	}
	a.CallIDs = make([]CallID, len(f.Calls))
	for i, c := range f.Calls {
		a.CallIDs[i] = CallID(c.CallID)
	}
	d, err := DigestAssistant(a)
	if err != nil {
		return err
	}
	a.Digest = d
	return nil
}

// toolResultOf projects a call's terminal fact into a structural entry
// (TRN-MAP-3, TRN-MAP-4 semantics: Unknown outcome or effect_unknown class
// is status unknown, every other failure is error).
func toolResultOf(turnID TurnID, runID run.RunID, fact run.Fact) (ToolResult, error) {
	var r ToolResult
	switch f := fact.(type) {
	case run.ToolCallCompleted:
		r = ToolResult{CallID: CallID(f.CallID), Status: ToolSuccess, Source: SourceToolOutput, OutputDigest: f.OutputDigest}
	case run.ToolCallAnswered:
		r = ToolResult{CallID: CallID(f.CallID), Status: ToolSuccess, Source: SourceToolResponse, OutputDigest: f.ResponseDigest}
	case run.ToolCallFailed:
		status := ToolError
		if f.Outcome == run.ToolOutcomeUnknown || f.Failure.Class == run.FailureEffectUnknown {
			status = ToolUnknown
		}
		failure := f.Failure
		r = ToolResult{CallID: CallID(f.CallID), Status: status, Failure: &failure}
	default:
		return ToolResult{}, fmt.Errorf("chatlog: %T is not a tool result fact", fact)
	}
	r.ID, r.TurnID, r.RunID = ToolResultIDFor(run.CallID(r.CallID)), turnID, runID
	d, err := DigestToolResult(&r)
	if err != nil {
		return ToolResult{}, err
	}
	r.Digest = d
	return r, nil
}

// supersedingResult is the entry an out-of-band verification substitutes for
// old (CHT-ENT-2): same call, the verified status and body.
func supersedingResult(old *ToolResult, p *ToolResultSupersededPayload) (ToolResult, error) {
	r := ToolResult{ID: SupersedingToolResultID(old.ID), TurnID: old.TurnID, RunID: old.RunID, CallID: old.CallID, Status: p.Status}
	if p.Status == ToolSuccess {
		r.Source, r.OutputDigest = SourceToolOutput, p.OutputDigest
	} else {
		r.Failure = &run.ToolFailure{Class: "superseded", Message: p.Reason}
	}
	d, err := DigestToolResult(&r)
	if err != nil {
		return ToolResult{}, err
	}
	r.Digest = d
	return r, nil
}

func terminateInput(s *Surface, id InputID, status InputStatus) error {
	v, ok := s.Inputs.Get(id)
	if !ok || v.Status != InputSubmitted {
		return fmt.Errorf("input %s %s while %s", id, status, v.Status)
	}
	v.Status = status
	s.Inputs = s.Inputs.Set(id, v)
	return nil
}

// cow returns a fresh copy of m for the one write that follows, so the
// previous state keeps its map untouched. Reads never copy.
func cow[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

// clip returns s with its capacity cut to its length, so an append by a later
// state cannot overwrite an element a holder of this state can still see.
// Plain appends never need it: a previous state only reads within its own
// length, so growth past it is invisible to it.
func clip[T any](s []T) []T { return s[:len(s):len(s)] }

// --- context ------------------------------------------------------------------

// Entry is one element of the model-facing conversation (CHT-CTX-1). Position is
// the projection-internal position of the entry; compactions split base from
// gap by it (CHT-EVT-3). An assistant or tool_result entry is structural: it
// names its frozen body by digest and is rendered through Materialize.
type Entry struct {
	Kind       EntryKind        `json:"kind"`
	ID         string           `json:"id"`
	Digest     es.Digest        `json:"digest"`
	Position   session.Position `json:"position"`
	Input      *Input           `json:"input,omitempty"`
	Assistant  *Assistant       `json:"assistant,omitempty"`
	ToolResult *ToolResult      `json:"toolResult,omitempty"`
	Summary    *Summary         `json:"summary,omitempty"`
}

// Pair names the entry for compaction base and retained sets.
func (e *Entry) Pair() EntryDigestPair {
	return EntryDigestPair{Kind: e.Kind, ID: e.ID, Digest: e.Digest}
}

// AppliedCompaction archives what a compaction replaced so an explicit
// invalidation restores it (CHT-EVT-3). Base excludes the compaction's own
// summary entry: invalidation drops the summary from the active context.
type AppliedCompaction struct {
	ID CompactionID `json:"id"`
	// Base is the active context the compaction covered, in order.
	Base []Entry `json:"base"`
	// PrefixLen is what the compaction contributed to Entries: the summary
	// plus the retained entries.
	PrefixLen int `json:"prefixLen"`
}

// Context is the projection state: the ordered entries plus the bookkeeping
// ContextFold needs (submitted inputs awaiting delivery, superseded results,
// applied compactions, the Turn of each Run).
type Context struct {
	Entries     []Entry                       `json:"entries"`
	Pending     map[InputID]Input             `json:"pending,omitempty"`
	Superseded  map[ToolResultID]ToolResultID `json:"superseded,omitempty"`
	Compactions []AppliedCompaction           `json:"compactions,omitempty"`
	Runs        map[run.RunID]RunOwner        `json:"runs,omitempty"`
}

var ContextProjection = extension.ProjectionDefinition{
	ID: ContextProjectionID, Version: 1,
	Consumes:      chatlogConsumes,
	Inherits:      extension.InheritAll,
	Authoritative: true,
	Initial: func() (any, error) {
		return Context{Pending: map[InputID]Input{}, Superseded: map[ToolResultID]ToolResultID{}, Runs: map[run.RunID]RunOwner{}}, nil
	},
	Apply:      applyContext,
	StateCodec: extension.JSONStateCodec[Context]{},
}

// applyContext is copy-on-write like applySurface: Entries and Compactions
// grow by append, a shrunk slice is clipped so a later append cannot reach an
// element the previous state still holds, and a map is copied only by the
// event that writes it. Entry positions are the ledger Positions of the
// events that produced them.
func applyContext(state any, e extension.DecodedEvent) (any, error) { //nolint:gocritic // hugeParam: projection Apply is copy-on-write over value states; DecodedEvent is the extension API shape
	c, ok := state.(Context)
	if !ok {
		return nil, fmt.Errorf("chatlog context: state is %T", state)
	}
	pos := e.Position
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		c.Pending = cow(c.Pending)
		c.Pending[p.InputID] = Input{ID: p.InputID, Content: p.Content, Digest: d}
	case InputDeliveredPayload:
		in, ok := c.Pending[p.InputID]
		if !ok {
			return nil, fmt.Errorf("input %s delivered before submission", p.InputID)
		}
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
		in.TurnID = p.TurnID
		c.Entries = append(c.Entries, Entry{Kind: EntryInput, ID: string(in.ID), Digest: in.Digest, Position: pos, Input: &in})
	case InputWithdrawnPayload:
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
	case InputRejectedPayload:
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
	case attempt.StartedPayload:
		c.Runs = cow(c.Runs)
		c.Runs[p.RunID] = RunOwner{TurnID: TurnID(p.TurnID)}
	case runmod.Event:
		return c.applyRun(p, e.Position)
	case ToolResultSupersededPayload:
		i := c.indexOf(EntryToolResult, string(p.ToolResultID))
		if i < 0 {
			// A result outside the active context was either never created or
			// compacted; its Turn completed, so superseding it violates
			// CHT-ENT-2 rather than invalidating the compaction.
			return nil, fmt.Errorf("tool_result %s superseded outside the active context", p.ToolResultID)
		}
		if _, twice := c.Superseded[p.ToolResultID]; twice {
			return nil, fmt.Errorf("tool_result %s superseded twice", p.ToolResultID)
		}
		replacement, err := supersedingResult(c.Entries[i].ToolResult, &p)
		if err != nil {
			return nil, err
		}
		c.Superseded = cow(c.Superseded)
		c.Superseded[p.ToolResultID] = replacement.ID
		// The verified result takes the superseded one's place, so the pairing
		// with its call is unchanged.
		entries := append([]Entry(nil), c.Entries...)
		entries[i] = Entry{Kind: EntryToolResult, ID: string(replacement.ID), Digest: replacement.Digest, Position: entries[i].Position, ToolResult: &replacement}
		c.Entries = entries
	case SummaryPayload:
		s := p.Summary
		c.Entries = append(c.Entries, Entry{Kind: EntrySummary, ID: string(s.ID), Digest: s.Digest, Position: pos, Summary: &s})
	case CompactionCreatedPayload:
		return applyCompaction(c, &p, e.Position)
	case CompactionInvalidatedPayload:
		n := len(c.Compactions)
		if n == 0 || c.Compactions[n-1].ID != p.CompactionID {
			return nil, fmt.Errorf("compaction %s is not the latest active compaction", p.CompactionID)
		}
		top := c.Compactions[n-1]
		if len(c.Entries) < top.PrefixLen {
			return nil, fmt.Errorf("compaction %s prefix exceeds the context", p.CompactionID)
		}
		c.Entries = append(append([]Entry(nil), top.Base...), c.Entries[top.PrefixLen:]...)
		c.Compactions = clip(c.Compactions[:n-1])
	default:
		return nil, fmt.Errorf("chatlog context: unexpected %T", e.Value)
	}
	return c, nil
}

// applyRun folds one Run fact into the Context (CHT-CTX-2). Runs is bounded
// like the Surface's: the Turn is forgotten at run_ended.
func (c Context) applyRun(ev runmod.Event, pos session.Position) (any, error) {
	switch f := ev.Fact.(type) {
	case run.RunCreated:
		// The Run's Turn comes from attempt/started, not from the Run.
	case run.RunEnded:
		c.Runs = cow(c.Runs)
		delete(c.Runs, ev.RunID)
	case run.ModelStepCompleted:
		a, err := assistantOf(c.Runs[ev.RunID], ev.RunID, &f)
		if err != nil {
			return nil, err
		}
		c.Entries = append(c.Entries, Entry{Kind: EntryAssistant, ID: string(a.ID), Digest: a.Digest, Position: pos, Assistant: &a})
	case run.ToolStepOpened:
		i := c.indexOf(EntryAssistant, string(AssistantIDFor(f.Source)))
		if i < 0 {
			return nil, fmt.Errorf("tool step %s opened for assistant %s outside the active context", f.StepID, f.Source)
		}
		a := *c.Entries[i].Assistant
		if err := attachCalls(&a, &f); err != nil {
			return nil, err
		}
		entries := append([]Entry(nil), c.Entries...)
		entries[i].Assistant, entries[i].Digest = &a, a.Digest
		c.Entries = entries
	case run.ToolCallCompleted, run.ToolCallAnswered, run.ToolCallFailed:
		r, err := toolResultOf(c.Runs[ev.RunID].TurnID, ev.RunID, ev.Fact)
		if err != nil {
			return nil, err
		}
		c.Entries = append(c.Entries, Entry{Kind: EntryToolResult, ID: string(r.ID), Digest: r.Digest, Position: pos, ToolResult: &r})
	default:
		return nil, fmt.Errorf("chatlog context: unexpected run fact %T", ev.Fact)
	}
	return c, nil
}

// indexOf finds the active entry of kind and id, searching from the end.
func (c *Context) indexOf(kind EntryKind, id string) int {
	for i := len(c.Entries) - 1; i >= 0; i-- {
		if c.Entries[i].Kind == kind && c.Entries[i].ID == id {
			return i
		}
	}
	return -1
}

// applyCompaction validates and applies one compaction_created (CHT-EVT-3).
// pos is the compaction's own position; CoveredThrough names an entry
// position, so it must precede the compaction event.
func applyCompaction(c Context, p *CompactionCreatedPayload, pos session.Position) (any, error) {
	if !p.CoveredThrough.Less(pos) {
		return nil, fmt.Errorf("compaction %s covers through %v at position %v", p.CompactionID, p.CoveredThrough, pos)
	}
	for _, ap := range c.Compactions {
		if ap.ID == p.CompactionID {
			return nil, fmt.Errorf("compaction %s created twice", p.CompactionID)
		}
	}
	cut := len(c.Entries)
	for cut > 0 && p.CoveredThrough.Less(c.Entries[cut-1].Position) {
		cut--
	}
	base, gap := c.Entries[:cut], c.Entries[cut:]
	if len(gap) != 1 || gap[0].Kind != EntrySummary || gap[0].ID != string(p.SummaryID) || gap[0].Digest != p.SummaryDigest {
		return nil, fmt.Errorf("compaction %s: the entries after coveredThrough must be exactly its summary", p.CompactionID)
	}
	pairs := make([]EntryDigestPair, len(base))
	for i := range base {
		pairs[i] = base[i].Pair()
	}
	wantBase, err := DigestBaseContext(pairs)
	if err != nil {
		return nil, err
	}
	if wantBase != p.BaseContextDigest {
		return nil, fmt.Errorf("compaction %s: base context digest mismatch", p.CompactionID)
	}
	retained, err := selectRetained(base, p.Retained)
	if err != nil {
		return nil, fmt.Errorf("compaction %s: %w", p.CompactionID, err)
	}
	// Base is clipped: it shares the covered prefix's storage, and no later
	// state may append into it.
	c.Compactions = append(c.Compactions, AppliedCompaction{ID: p.CompactionID, Base: clip(base), PrefixLen: 1 + len(retained)})
	c.Entries = append([]Entry{gap[0]}, retained...)
	return c, nil
}

// selectRetained resolves the retained pairs as an ordered subset of base.
func selectRetained(base []Entry, pairs []EntryDigestPair) ([]Entry, error) {
	out := make([]Entry, 0, len(pairs))
	i := 0
	for _, p := range pairs {
		for i < len(base) && base[i].Pair() != p {
			i++
		}
		if i == len(base) {
			return nil, fmt.Errorf("retained %s %s is not in the base context in order", p.Kind, p.ID)
		}
		out = append(out, base[i])
		i++
	}
	return out, nil
}

// ContextFold folds decoded chatlog and run events into entries (CHT-CTX-1).
func ContextFold(events []extension.DecodedEvent) ([]Entry, error) {
	state, _ := ContextProjection.Initial()
	for i := range events {
		e := &events[i]
		if e.Unknown || (e.Module != extension.TwilightModule(ModuleID) && e.Module != extension.TwilightModule(runmod.ModuleID)) {
			return nil, errors.New("chatlog: context fold requires decoded chatlog or run events")
		}
		next, err := applyContext(state, *e)
		if err != nil {
			return nil, err
		}
		state = next
	}
	c, ok := state.(Context)
	if !ok {
		return nil, fmt.Errorf("chatlog: context projection is %T", state)
	}
	return c.Entries, nil
}
