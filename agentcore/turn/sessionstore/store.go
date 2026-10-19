package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/agentcore/turn/wire"
	"time"
)

// SnapshotPolicy decides whether the machine projection is written to the
// projection cache after a commit. It sees the state before and after Evolve.
type SnapshotPolicy func(before, after *turn.MachineState) bool

// DefaultSnapshotPolicy writes when the Turn returns to Open or terminates.
func DefaultSnapshotPolicy(_, after *turn.MachineState) bool {
	if after.Status.Terminal() {
		return true
	}
	_, open := after.Current.(turn.Open)
	return open
}

// Config assembles a SessionTurnStore (agent-store.md 10).
type Config struct {
	Registry *module.Registry
	// Store is the read side: Record folds from it (through Cache) without
	// taking ownership; commands write through the Writer a port is bound
	// to.
	Store session.Store
	// Frozen holds the bodies facts name by digest. It is
	// required, and it must register each body's Binding for the Writers'
	// admission: FrozenValues over a ContentStore and a BindingStore does.
	Frozen frozen.Store
	// PrepareGuard enforces Session consistency before a new ModelStep is
	// prepared, inside the same Writer critical section as its Turn command.
	// Nil adds no Session-level constraint to the standalone Turn adapter.
	PrepareGuard func(writer.View) error
	// Snapshot decides when the machine projection is cached; nil selects
	// DefaultSnapshotPolicy.
	Snapshot SnapshotPolicy
	// Cache receives the machine projection per SnapshotPolicy; nil disables.
	Cache session.ProjectionCache
	Now   func() time.Time
}

// SessionTurnStore is the Session adapter of the Turn core: it realizes
// store.TurnStore over a Session Writer (Bind), reads TurnStore by SessionID without
// ownership (Record), and contributes the Turn module's Parts to a cross-module
// unit of work (Command, CreateTurn). It is the only code that encodes Turn
// facts as twilight/turn/ events.
type SessionTurnStore struct {
	cfg    Config
	reader session.ProjectionReader
}

func NewSessionTurnStore(cfg Config) (*SessionTurnStore, error) {
	if cfg.Registry == nil || cfg.Store == nil {
		return nil, errors.New("sessionstore: store requires registry and store")
	}
	if cfg.Frozen == nil {
		return nil, errors.New("sessionstore: store requires a frozen value store")
	}
	if cfg.Snapshot == nil {
		cfg.Snapshot = DefaultSnapshotPolicy
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &SessionTurnStore{cfg: cfg, reader: session.NewProjectionReader(cfg.Store, cfg.Registry, cfg.Cache)}, nil
}

func (s *SessionTurnStore) nowMilli() int64 { return s.cfg.Now().UnixMilli() }

// ownershipError maps the Writer's ownership loss onto the Turn sentinel.
func ownershipError(err error) error {
	if errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost) {
		return fmt.Errorf("%w: %w", store.ErrOwnershipLost, err)
	}
	return err
}

func loadMachine(view writer.View) (Machine, error) {
	state, err := view.Projection(MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return Machine{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return Machine{}, fmt.Errorf("sessionstore: machine projection is %T", state)
	}
	return m, nil
}

// --- bound port ---------------------------------------------------------------------

// Bind returns the store.TurnStore over one Session Writer: the caller's
// ownership capability, so every command of a drive lands on the same Writer,
// epoch and projection view.
func (s *SessionTurnStore) Bind(w writer.Writer) store.TurnStore { return &bound{s: s, w: w} }

type bound struct {
	s *SessionTurnStore
	w writer.Writer
}

func (b *bound) Scope() turn.Scope { return turn.Scope(b.w.SessionID()) }

// Writer is the ownership capability the store is bound to;
// a Loop hook that must commit through the same Writer takes it from here.
func (b *bound) Writer() writer.Writer { return b.w }

// Load reads the Writer's transactional projection; a Turn the
// projection no longer holds is folded from its own stream.
func (b *bound) Load(ctx context.Context, turnID turn.TurnID) (store.Snapshot, error) {
	if err := store.CheckContext(ctx); err != nil {
		return store.Snapshot{}, err
	}
	sid := b.w.SessionID()
	state, _, err := b.w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return store.Snapshot{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return store.Snapshot{}, fmt.Errorf("sessionstore: machine projection is %T", state)
	}
	if snap, ok := m.snapshot(turnID); ok {
		return snap, nil
	}
	record, err := b.s.record(ctx, sid, turnID, nil, ledger.Head{})
	if err != nil {
		return store.Snapshot{}, err
	}
	return record.Snapshot, nil
}

// Commit is one Turn command as a unit of work of its own: the Turn module's
// Part alone.
func (b *bound) Commit(ctx context.Context, req store.CommitRequest) (store.CommitResult, error) {
	if err := store.CheckContext(ctx); err != nil {
		return store.CommitResult{}, err
	}
	cmd, err := b.s.Command(ctx, req)
	if err != nil {
		return store.CommitResult{}, err
	}
	work := unit.Work{CommitID: ledger.CommitID(req.Command.ID), Parts: []unit.Part{cmd}}
	switch req.Command.Command.(type) {
	case turn.AcceptInput:
		work.ReplayCheck = cmd.CheckInputReplay
	case turn.ApproveToolCall, turn.RejectToolCall, turn.SubmitToolResponse:
		work.ReplayCheck = cmd.CheckResponseReplay
	}
	res, err := unit.Commit(ctx, b.w, b.s.nowMilli(), work)
	if err != nil {
		return store.CommitResult{}, ownershipError(err)
	}
	return cmd.Result(ctx, b.w, &res)
}

func (b *bound) FrozenRequest(ctx context.Context, digest turn.Digest) (model.ModelRequest, error) {
	return b.s.FrozenRequest(ctx, digest)
}

// FrozenRequest returns the request body a Prepared or Executing ModelStep
// names by RequestDigest.
func (s *SessionTurnStore) FrozenRequest(ctx context.Context, digest turn.Digest) (model.ModelRequest, error) {
	if err := store.CheckContext(ctx); err != nil {
		return model.ModelRequest{}, err
	}
	if digest == "" {
		return model.ModelRequest{}, errors.New("sessionstore: empty request digest")
	}
	raw, ok, err := s.cfg.Frozen.Get(ctx, digest)
	if err != nil {
		return model.ModelRequest{}, err
	}
	if !ok {
		return model.ModelRequest{}, fmt.Errorf("%w: request %s", frozen.ErrMissing, digest)
	}
	return frozen.DecodeRequest(raw, digest)
}

// --- Command part -------------------------------------------------------------------

// Command is the Turn module's Part for one command: constructing
// it freezes the bodies the command's facts will name, Prepare evaluates the
// command inside the Writer against the machine projection and contributes
// the facts to the Turn's stream, Result reads the accepted decision back
// once the unit has committed. Its rejections are the Turn sentinels:
// ErrTurnTerminal, ErrStaleRuntime, ErrCommandConflict, ErrTurnNotFound.
type Command struct {
	s   *SessionTurnStore
	req store.CommitRequest

	prepared bool
	before   turn.MachineState
	after    turn.MachineState
	position turn.TurnPosition
	facts    []turn.Fact
}

// Command builds the Part of one command. The bodies it names by digest are
// stored before the unit commits: Put is idempotent and content-addressed,
// so a rejected or replayed command leaves nothing inconsistent behind.
func (s *SessionTurnStore) Command(ctx context.Context, req store.CommitRequest) (*Command, error) {
	if req.Command.TurnID == "" || req.Command.ID == "" {
		return nil, errors.New("sessionstore: command requires TurnID and CommandID")
	}
	if err := s.freezeBodies(ctx, req.Command.Command); err != nil {
		return nil, err
	}
	return &Command{s: s, req: req}, nil
}

func (c *Command) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	env := &c.req.Command
	turnID := env.TurnID
	if _, preparing := env.Command.(turn.PrepareModelRequest); preparing && c.s.cfg.PrepareGuard != nil {
		if err := c.s.cfg.PrepareGuard(view); err != nil {
			return nil, err
		}
	}
	proj, err := loadMachine(view)
	if err != nil {
		return nil, err
	}
	state, active := proj.Active[turnID]
	if !active {
		// Terminal Turns leave the projection: tell terminal from
		// unknown by the Turn's own stream.
		if _, exists := view.StreamHead(Stream(turnID)); exists {
			return nil, turn.ErrTurnTerminal
		}
		return nil, store.ErrTurnNotFound
	}
	decision, err := store.EvaluateCommit(state, proj.Positions[turnID], c.req)
	if err != nil {
		return nil, err
	}
	switch decision.Kind {
	case store.DecisionConflict:
		return nil, turn.ErrCommandConflict
	case store.DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, turn.ErrStaleRuntime) {
			return nil, fmt.Errorf("%w: %w", turn.ErrStaleRuntime, decision.Reject)
		}
		return nil, turn.ErrStaleRuntime
	case store.DecisionTerminal:
		return nil, turn.ErrTurnTerminal
	}
	events := make([]writer.TypedEvent, 0, len(decision.Facts))
	for _, f := range decision.Facts {
		events = append(events, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{TurnID: turnID, Fact: f}})
	}
	c.prepared = true
	c.before, c.after = state, decision.NewState
	c.position = proj.Positions[turnID] + turn.TurnPosition(len(decision.Facts))
	c.facts = decision.Facts
	return []writer.TypedBatch{{Domain: Stream(turnID), Events: events}}, nil
}

// CheckInputReplay verifies accepted inputs including immutable content digests.
// Command identity contains ordered InputIDs and does not contain digests.
func (c *Command) CheckInputReplay(commit ledger.Commit) error {
	cmd, ok := c.req.Command.Command.(turn.AcceptInput)
	if !ok {
		return errors.New("sessionstore: input replay check requires AcceptInput")
	}
	facts, err := c.s.factsOf(commit, c.req.Command.TurnID)
	if err != nil {
		return err
	}
	if len(facts) != len(cmd.Inputs) {
		return turn.ErrCommandConflict
	}
	for i, f := range facts {
		accepted, ok := f.(turn.InputAccepted)
		if !ok || accepted.Input != cmd.Inputs[i] {
			return turn.ErrCommandConflict
		}
	}
	return nil
}

// CheckResponseReplay compares a response with its committed outcome. Response
// command IDs identify the request, so the decision and content need a separate
// check even after the call has moved on or the Turn has ended.
func (c *Command) CheckResponseReplay(commit ledger.Commit) error {
	facts, err := c.s.factsOf(commit, c.req.Command.TurnID)
	if err != nil {
		return err
	}
	if len(facts) != 1 {
		return turn.ErrCommandConflict
	}
	var step turn.StepID
	var call turn.CallID
	var response turn.ResponseID
	switch cmd := c.req.Command.Command.(type) {
	case turn.ApproveToolCall:
		step, call, response = cmd.StepID, cmd.CallID, cmd.ResponseID
		if fact, ok := facts[0].(turn.ToolCallApproved); !ok || fact != turn.ToolCallApproved(cmd) {
			return turn.ErrCommandConflict
		}
	case turn.SubmitToolResponse:
		step, call, response = cmd.StepID, cmd.CallID, cmd.ResponseID
		want := turn.ToolCallAnswered{StepID: step, CallID: call, ResponseID: response, ResponseDigest: cmd.ResponseDigest}
		if fact, ok := facts[0].(turn.ToolCallAnswered); !ok || fact != want {
			return turn.ErrCommandConflict
		}
	case turn.RejectToolCall:
		step, call, response = cmd.StepID, cmd.CallID, cmd.ResponseID
		fact, ok := facts[0].(turn.ToolCallFailed)
		if !ok || fact.StepID != step || fact.CallID != call || fact.Effect != "" ||
			fact.Outcome != turn.ToolOutcomeKnown || fact.Failure.Message != cmd.Reason {
			return turn.ErrCommandConflict
		}
		var kind turn.ResponseKind
		switch fact.Failure.Class {
		case turn.FailurePermissionDenied:
			kind = turn.ResponseApproval
		case turn.FailureResponseRejected:
			kind = turn.ResponseExternal
		default:
			return turn.ErrCommandConflict
		}
		digest, err := schema.Canonical().DigestToolResponseDecision(kind, turn.ResponseDecisionRejected, cmd.Reason)
		if err != nil {
			return err
		}
		if digest != cmd.ResponseDigest {
			return turn.ErrCommandConflict
		}
	default:
		return errors.New("sessionstore: response replay check requires a response command")
	}
	if c.req.Command.ID != schema.Identity().DeriveResponseCommandID(c.req.Command.TurnID, step, call, response) {
		return turn.ErrCommandConflict
	}
	return nil
}

// Result maps the unit's outcome onto the Turn's CommitResult. w is the Writer
// the unit committed through; a replay reads the current snapshot from it.
func (c *Command) Result(ctx context.Context, w writer.Writer, res *writer.CommitResult) (store.CommitResult, error) {
	switch res.Outcome {
	case writer.CommitApplied:
		if !c.prepared {
			return store.CommitResult{}, errors.New("sessionstore: commit applied without the Turn part")
		}
		c.s.afterCommit(ctx, w, &c.before, &c.after)
		return store.CommitResult{Status: store.CommitAccepted, Facts: c.facts,
			Snapshot: store.Snapshot{State: c.after, Position: c.position}}, nil
	case writer.CommitAlreadyApplied:
		// The Session (SessionID, CommitID) index identifies a replay.
		// Input and response commands additionally verify their content through
		// their ReplayCheck before reaching here.
		snapshot, err := c.s.Bind(w).Load(ctx, c.req.Command.TurnID)
		if err != nil {
			return store.CommitResult{}, err
		}
		facts, err := c.s.factsOf(res.Commit, c.req.Command.TurnID)
		if err != nil {
			return store.CommitResult{}, err
		}
		return store.CommitResult{Status: store.CommitAlreadyApplied, Snapshot: snapshot, Facts: facts}, nil
	case writer.CommitConflict:
		return store.CommitResult{}, turn.ErrCommandConflict
	default:
		return store.CommitResult{}, fmt.Errorf("sessionstore: commit: %s: %s", res.Outcome, res.Detail)
	}
}

// factsOf decodes the Turn facts of turnID a stored commit holds.
func (s *SessionTurnStore) factsOf(c ledger.Commit, turnID turn.TurnID) ([]turn.Fact, error) {
	var out []turn.Fact
	for _, b := range c.Batches {
		if b.Domain != Stream(turnID) {
			continue
		}
		for i := range b.Events {
			decoded, err := s.cfg.Registry.Decode(b.Events[i])
			if err != nil {
				return nil, err
			}
			if ev, ok := decoded.Value.(Event); ok {
				out = append(out, ev.Fact)
			}
		}
	}
	return out, nil
}

// freezeBodies stores the bodies a command's facts will name by digest
// the request of a Prepare, the result of a model settlement,
// the output of a tool settlement and the payload of an external response.
// Each body is encoded under the Turn's schema as the envelope its digest was
// computed from (digest rules and body encoding are versioned together with Decide and Evolve).
func (s *SessionTurnStore) freezeBodies(ctx context.Context, cmd turn.AgentCommand) error {
	var digest turn.Digest
	var body []byte
	var err error
	switch c := cmd.(type) {
	case turn.PrepareModelRequest:
		digest = c.RequestDigest
		body, err = schema.Bodies().EncodeRequest(&c.Request, digest)
	case turn.SubmitModelResult:
		if digest, err = schema.Canonical().DigestModelResult(c.Result); err == nil {
			body, err = schema.Bodies().EncodeModelResult(&c.Result, digest)
		}
	case turn.SubmitToolResult:
		if digest, err = schema.Canonical().DigestToolOutput(c.Result.Output); err == nil {
			body, err = schema.Bodies().EncodeToolOutput(c.Result.Output, digest)
		}
	case turn.SubmitToolResponse:
		digest = c.ResponseDigest
		body, err = schema.Bodies().EncodeToolResponse(c.Payload, digest)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %w", turn.ErrStaleRuntime, err)
	}
	return s.cfg.Frozen.Put(ctx, digest, body)
}

// afterCommit writes the machine projection to the cache when the policy asks
// for it. Cache failures never affect the commit.
func (s *SessionTurnStore) afterCommit(ctx context.Context, w writer.Writer, before, after *turn.MachineState) {
	if s.cfg.Cache == nil || !s.cfg.Snapshot(before, after) {
		return
	}
	sid := w.SessionID()
	state, head, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return
	}
	_ = session.SaveProjection(ctx, s.cfg.Cache, s.cfg.Registry, sid, MachineProjectionID, MachineProjection.Version, state, head)
}

// --- CreateTurn part ------------------------------------------------------------------

// ErrTurnExists reports a creation of a TurnID this Session already holds.
var ErrTurnExists = errors.New("sessionstore: turn already exists")

// CreateTurn is the Part that establishes a Turn: TurnStarted and
// one InputAccepted per initial input, in the Turn's own stream. The owning
// module places it in its creation unit beside its own facts. A TurnID whose
// stream this Session already wrote -- active or ended -- is ErrTurnExists.
func CreateTurn(newTurn turn.NewTurn, inputs []turn.AgentInput) unit.Part {
	return createTurn{newTurn: newTurn, inputs: inputs}
}

type createTurn struct {
	newTurn turn.NewTurn
	inputs  []turn.AgentInput
}

func (c createTurn) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	facts, err := schema.Machine().CreateGroup(c.newTurn, c.inputs)
	if err != nil {
		return nil, err
	}
	if _, exists := view.StreamHead(Stream(c.newTurn.TurnID)); exists {
		return nil, fmt.Errorf("%w: %s", ErrTurnExists, c.newTurn.TurnID)
	}
	events := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		events = append(events, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{TurnID: c.newTurn.TurnID, Fact: f}})
	}
	return []writer.TypedBatch{{Domain: Stream(c.newTurn.TurnID), Events: events}}, nil
}

// --- Record --------------------------------------------------------------------------

// Record is one verified read of a Turn: every twilight/turn/ event of the
// TurnID in stream order, folded and compared with the projection.
type Record struct {
	Created  ledger.StreamSeq
	Snapshot store.Snapshot
	Events   []ledger.Event
	Facts    []turn.Fact
}

// Record folds a Turn from the Store by SessionID; it needs no ownership
func (s *SessionTurnStore) Record(ctx context.Context, sid session.SessionID, turnID turn.TurnID) (Record, error) {
	if err := store.CheckContext(ctx); err != nil {
		return Record{}, err
	}
	state, head, err := s.reader.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return Record{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return Record{}, fmt.Errorf("sessionstore: machine projection is %T", state)
	}
	var expect *turn.MachineState
	if ms, ok := m.Active[turnID]; ok {
		expect = &ms
	}
	return s.record(ctx, sid, turnID, expect, head)
}

// record reads the Turn's events from the Store and folds them: the ledger
// fold is the authoritative read. When expect is given, the fold
// is compared with the projection state, but only if both were read at the
// same head: the two reads are separate round trips, and a commit landing
// between them makes both correct at different points, not divergent.
func (s *SessionTurnStore) record(ctx context.Context, sid session.SessionID, turnID turn.TurnID, expect *turn.MachineState, expectHead ledger.Head) (Record, error) {
	page, err := s.cfg.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: sid, Domain: Stream(turnID), Inheritance: module.Own})
	if err != nil {
		return Record{}, err
	}
	var record Record
	var position turn.TurnPosition
	for i := range page.Events {
		e := &page.Events[i]
		decoded, err := s.cfg.Registry.Decode(*e)
		if err != nil {
			return Record{}, err
		}
		if decoded.Unknown {
			return Record{}, fmt.Errorf("sessionstore: record: unknown turn event %s v%d", e.Type, decoded.Version)
		}
		ev, ok := decoded.Value.(Event)
		if !ok {
			return Record{}, fmt.Errorf("sessionstore: record: %s decoded to %T", e.Type, decoded.Value)
		}
		if ev.TurnID != turnID {
			continue
		}
		if len(record.Events) == 0 {
			record.Created = ledger.StreamSeq(i)
		}
		record.Events = append(record.Events, *e)
		record.Facts = append(record.Facts, ev.Fact)
		position = turn.TurnPosition(i)
	}
	if len(record.Facts) == 0 {
		return Record{}, store.ErrTurnNotFound
	}
	state, err := store.FoldTurn(record.Facts)
	if err != nil {
		return Record{}, fmt.Errorf("sessionstore: record: %w", err)
	}
	if expect != nil && page.Head == expectHead && !wire.StatesEquivalent(&state, expect) {
		return Record{}, errors.New("sessionstore: record: projection diverges from the event fold")
	}
	record.Snapshot = store.Snapshot{State: state, Position: position}
	return record, nil
}
