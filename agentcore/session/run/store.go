package runmod

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/wire"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// SnapshotPolicy decides whether the machine projection is written to the
// projection cache after a commit. It sees the state before and after Evolve.
type SnapshotPolicy func(before, after *run.MachineState) bool

// DefaultSnapshotPolicy writes when the Run returns to Open or terminates.
func DefaultSnapshotPolicy(_, after *run.MachineState) bool {
	if after.Status.Terminal() {
		return true
	}
	_, open := after.Current.(run.Open)
	return open
}

// Config assembles a SessionRunStore (agent-runtime.md 10).
type Config struct {
	Registry *extension.Registry
	// Store is the read side: Record folds from it (through Cache) without
	// taking ownership; commands write through the Writer a port is bound
	// to (OWN-HDL-2).
	Store session.Store
	// Frozen holds the bodies facts name by digest (RUN-WIR-4). It is
	// required, and it must register each body's Binding for the Writers'
	// admission: FrozenValues over a ContentStore and a BindingStore does.
	Frozen frozen.Store
	// Snapshot decides when the machine projection is cached; nil selects
	// DefaultSnapshotPolicy.
	Snapshot SnapshotPolicy
	// Cache receives the machine projection per SnapshotPolicy; nil disables.
	Cache extension.ProjectionCache
	Now   func() time.Time
}

// SessionRunStore is the Session adapter of the Run core: it realizes
// runtime.RunStore over a Session Writer (Bind), reads Runs by SessionID without
// ownership (Record), and contributes the Run module's Parts to a cross-module
// unit of work (Command, CreateRun). It is the only code that encodes Run
// facts as twilight/run/ events.
type SessionRunStore struct {
	cfg    Config
	reader extension.ProjectionReader
}

func NewSessionRunStore(cfg Config) (*SessionRunStore, error) {
	if cfg.Registry == nil || cfg.Store == nil {
		return nil, errors.New("runmod: store requires registry and store")
	}
	if cfg.Frozen == nil {
		return nil, errors.New("runmod: store requires a frozen value store")
	}
	if cfg.Snapshot == nil {
		cfg.Snapshot = DefaultSnapshotPolicy
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &SessionRunStore{cfg: cfg, reader: extension.NewProjectionReader(cfg.Store, cfg.Registry, cfg.Cache)}, nil
}

func (s *SessionRunStore) nowMilli() int64 { return s.cfg.Now().UnixMilli() }

// ownershipError maps the Writer's ownership loss onto the Run sentinel.
func ownershipError(err error) error {
	if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost) {
		return fmt.Errorf("%w: %w", runtime.ErrOwnershipLost, err)
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
		return Machine{}, fmt.Errorf("runmod: machine projection is %T", state)
	}
	return m, nil
}

// --- bound port ---------------------------------------------------------------------

// Bind returns the runtime.RunStore over one Session Writer: the caller's
// ownership capability, so every command of a drive lands on the same Writer,
// epoch and projection view (OWN-HDL-2).
func (s *SessionRunStore) Bind(w writer.Writer) runtime.RunStore { return &bound{s: s, w: w} }

type bound struct {
	s *SessionRunStore
	w writer.Writer
}

func (b *bound) Scope() run.Scope { return run.Scope(b.w.SessionID()) }

// Writer is the ownership capability the store is bound to (OWN-HDL-2);
// a Loop hook that must commit through the same Writer takes it from here.
func (b *bound) Writer() writer.Writer { return b.w }

// Load reads the Writer's transactional projection (RUN-CMT-2); a Run the
// projection no longer holds is folded from its own stream (RUN-CMT-1).
func (b *bound) Load(ctx context.Context, runID run.RunID) (runtime.Snapshot, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return runtime.Snapshot{}, err
	}
	sid := b.w.SessionID()
	state, _, err := b.w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return runtime.Snapshot{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return runtime.Snapshot{}, fmt.Errorf("runmod: machine projection is %T", state)
	}
	if snap, ok := m.snapshot(runID); ok {
		return snap, nil
	}
	record, err := b.s.record(ctx, sid, runID, nil, session.Head{})
	if err != nil {
		return runtime.Snapshot{}, err
	}
	return record.Snapshot, nil
}

// Commit is one Run command as a unit of work of its own: the Run module's
// Part alone (RUN-CMT-3).
func (b *bound) Commit(ctx context.Context, req runtime.CommitRequest) (runtime.CommitResult, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return runtime.CommitResult{}, err
	}
	cmd, err := b.s.Command(ctx, req)
	if err != nil {
		return runtime.CommitResult{}, err
	}
	res, err := unit.Commit(ctx, b.w, b.s.nowMilli(), unit.Work{CommitID: session.CommitID(req.Command.ID), Parts: []unit.Part{cmd}})
	if err != nil {
		return runtime.CommitResult{}, ownershipError(err)
	}
	return cmd.Result(ctx, b.w, &res)
}

func (b *bound) FrozenRequest(ctx context.Context, digest run.Digest) (model.ModelRequest, error) {
	return b.s.FrozenRequest(ctx, digest)
}

// FrozenRequest returns the request body a Prepared or Executing ModelStep
// names by RequestDigest (RUN-WIR-4).
func (s *SessionRunStore) FrozenRequest(ctx context.Context, digest run.Digest) (model.ModelRequest, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return model.ModelRequest{}, err
	}
	if digest == "" {
		return model.ModelRequest{}, errors.New("runmod: empty request digest")
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

// Command is the Run module's Part for one command (RUN-CMT-3): constructing
// it freezes the bodies the command's facts will name, Prepare evaluates the
// command inside the Writer against the machine projection and contributes
// the facts to the Run's stream, Result reads the accepted decision back
// once the unit has committed. Its rejections are the Run sentinels:
// ErrRunTerminal, ErrStaleRuntime, ErrCommandConflict, ErrRunNotFound.
type Command struct {
	s   *SessionRunStore
	req runtime.CommitRequest

	prepared bool
	before   run.MachineState
	after    run.MachineState
	position run.RunPosition
	facts    []run.Fact
}

// Command builds the Part of one command. The bodies it names by digest are
// stored before the unit commits: Put is idempotent and content-addressed,
// so a rejected or replayed command leaves nothing inconsistent behind.
func (s *SessionRunStore) Command(ctx context.Context, req runtime.CommitRequest) (*Command, error) {
	if req.Command.RunID == "" || req.Command.ID == "" {
		return nil, errors.New("runmod: command requires RunID and CommandID")
	}
	if err := s.freezeBodies(ctx, req.Command.Command); err != nil {
		return nil, err
	}
	return &Command{s: s, req: req}, nil
}

func (c *Command) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	env := &c.req.Command
	runID := env.RunID
	proj, err := loadMachine(view)
	if err != nil {
		return nil, err
	}
	state, active := proj.Active[runID]
	if !active {
		// Terminal Runs leave the projection (RUN-CMT-2): tell terminal from
		// unknown by the Run's own stream.
		if _, exists := view.StreamHead(Stream(runID)); exists {
			return nil, run.ErrRunTerminal
		}
		return nil, runtime.ErrRunNotFound
	}
	decision, err := runtime.EvaluateCommit(state, proj.Positions[runID], c.req)
	if err != nil {
		return nil, err
	}
	switch decision.Kind {
	case runtime.DecisionConflict:
		return nil, run.ErrCommandConflict
	case runtime.DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, run.ErrStaleRuntime) {
			return nil, fmt.Errorf("%w: %w", run.ErrStaleRuntime, decision.Reject)
		}
		return nil, run.ErrStaleRuntime
	case runtime.DecisionTerminal:
		return nil, run.ErrRunTerminal
	}
	events := make([]writer.TypedEvent, 0, len(decision.Facts))
	for _, f := range decision.Facts {
		events = append(events, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{RunID: runID, Fact: f}})
	}
	c.prepared = true
	c.before, c.after = state, decision.NewState
	c.position = proj.Positions[runID] + run.RunPosition(len(decision.Facts))
	c.facts = decision.Facts
	return []writer.TypedBatch{{Stream: Stream(runID), Events: events}}, nil
}

// Result maps the unit's outcome onto the Run's CommitResult. w is the Writer
// the unit committed through; a replay reads the current snapshot from it.
func (c *Command) Result(ctx context.Context, w writer.Writer, res *writer.CommitResult) (runtime.CommitResult, error) {
	switch res.Outcome {
	case writer.CommitApplied:
		if !c.prepared {
			return runtime.CommitResult{}, errors.New("runmod: commit applied without the run part")
		}
		c.s.afterCommit(ctx, w, &c.before, &c.after)
		return runtime.CommitResult{Status: runtime.CommitAccepted, Facts: c.facts,
			Snapshot: runtime.Snapshot{State: c.after, Position: c.position}}, nil
	case writer.CommitAlreadyApplied:
		// Idempotency is the Session's (SessionID, CommitID) index alone
		// (RUN-CMT-5): every Run CommandID is content-derived, so a hit is the
		// same command and nothing is re-decided.
		snapshot, err := c.s.Bind(w).Load(ctx, c.req.Command.RunID)
		if err != nil {
			return runtime.CommitResult{}, err
		}
		facts, err := c.s.factsOf(res.Commit, c.req.Command.RunID)
		if err != nil {
			return runtime.CommitResult{}, err
		}
		return runtime.CommitResult{Status: runtime.CommitAlreadyApplied, Snapshot: snapshot, Facts: facts}, nil
	case writer.CommitConflict:
		return runtime.CommitResult{}, run.ErrCommandConflict
	default:
		return runtime.CommitResult{}, fmt.Errorf("runmod: commit: %s: %s", res.Outcome, res.Detail)
	}
}

// factsOf decodes the Run facts of runID a stored commit holds.
func (s *SessionRunStore) factsOf(c session.Commit, runID run.RunID) ([]run.Fact, error) {
	var out []run.Fact
	for _, b := range c.Batches {
		if b.Stream != Stream(runID) {
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
// (RUN-WIR-4): the request of a Prepare, the result of a model settlement,
// the output of a tool settlement and the payload of an external response.
// Each body is encoded under the Run's schema as the envelope its digest was
// computed from (RUN-CMT-8: digest rules and body encoding are versioned
// together with Decide and Evolve).
func (s *SessionRunStore) freezeBodies(ctx context.Context, cmd run.AgentCommand) error {
	var digest run.Digest
	var body []byte
	var err error
	switch c := cmd.(type) {
	case run.PrepareModelRequest:
		digest = c.RequestDigest
		body, err = schema.Bodies().EncodeRequest(&c.Request, digest)
	case run.SubmitModelResult:
		if digest, err = schema.Canonical().DigestModelResult(c.Result); err == nil {
			body, err = schema.Bodies().EncodeModelResult(&c.Result, digest)
		}
	case run.SubmitToolResult:
		if digest, err = schema.Canonical().DigestToolOutput(c.Result.Output); err == nil {
			body, err = schema.Bodies().EncodeToolOutput(c.Result.Output, digest)
		}
	case run.SubmitToolResponse:
		digest = c.ResponseDigest
		body, err = schema.Bodies().EncodeToolResponse(c.Payload, digest)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %w", run.ErrStaleRuntime, err)
	}
	return s.cfg.Frozen.Put(ctx, digest, body)
}

// afterCommit writes the machine projection to the cache when the policy asks
// for it (RUN-CMT-2). Cache failures never affect the commit.
func (s *SessionRunStore) afterCommit(ctx context.Context, w writer.Writer, before, after *run.MachineState) {
	if s.cfg.Cache == nil || !s.cfg.Snapshot(before, after) {
		return
	}
	sid := w.SessionID()
	state, head, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return
	}
	_ = extension.SaveProjection(ctx, s.cfg.Cache, s.cfg.Registry, sid, MachineProjectionID, MachineProjection.Version, state, head)
}

// --- CreateRun part ------------------------------------------------------------------

// ErrRunExists reports a creation of a RunID this Session already holds.
var ErrRunExists = errors.New("runmod: run already exists")

// CreateRun is the Part that establishes a Run (RUN-NEW-1): RunCreated and
// one InputAccepted per initial input, in the Run's own stream. The owning
// module places it in its creation unit beside its own facts. A RunID whose
// stream this Session already wrote -- active or ended -- is ErrRunExists.
func CreateRun(newRun run.NewRun, inputs []run.AgentInput) unit.Part {
	return createRun{newRun: newRun, inputs: inputs}
}

type createRun struct {
	newRun run.NewRun
	inputs []run.AgentInput
}

func (c createRun) Prepare(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	facts, err := schema.Machine().CreateGroup(c.newRun, c.inputs)
	if err != nil {
		return nil, err
	}
	if _, exists := view.StreamHead(Stream(c.newRun.RunID)); exists {
		return nil, fmt.Errorf("%w: %s", ErrRunExists, c.newRun.RunID)
	}
	events := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		events = append(events, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{RunID: c.newRun.RunID, Fact: f}})
	}
	return []writer.TypedBatch{{Stream: Stream(c.newRun.RunID), Events: events}}, nil
}

// --- Record --------------------------------------------------------------------------

// Record is one verified read of a Run: every twilight/run/ event of the
// RunID in stream order, folded and compared with the projection.
type Record struct {
	Created  session.StreamSeq
	Snapshot runtime.Snapshot
	Events   []session.Event
	Facts    []run.Fact
}

// Record folds a Run from the Store by SessionID; it needs no ownership
// (OWN-HDL-2).
func (s *SessionRunStore) Record(ctx context.Context, sid session.SessionID, runID run.RunID) (Record, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return Record{}, err
	}
	state, head, err := s.reader.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return Record{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return Record{}, fmt.Errorf("runmod: machine projection is %T", state)
	}
	var expect *run.MachineState
	if ms, ok := m.Active[runID]; ok {
		expect = &ms
	}
	return s.record(ctx, sid, runID, expect, head)
}

// record reads the Run's events from the Store and folds them: the ledger
// fold is the authoritative read (RUN-CMT-1). When expect is given, the fold
// is compared with the projection state, but only if both were read at the
// same head: the two reads are separate round trips, and a commit landing
// between them makes both correct at different points, not divergent.
func (s *SessionRunStore) record(ctx context.Context, sid session.SessionID, runID run.RunID, expect *run.MachineState, expectHead session.Head) (Record, error) {
	page, err := s.cfg.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: sid, Stream: Stream(runID), Lineage: streamDefinition.Lineage})
	if err != nil {
		return Record{}, err
	}
	var record Record
	var position run.RunPosition
	for i := range page.Events {
		e := &page.Events[i]
		decoded, err := s.cfg.Registry.Decode(*e)
		if err != nil {
			return Record{}, err
		}
		if decoded.Unknown {
			return Record{}, fmt.Errorf("runmod: record: unknown run event %s v%d", e.Type, decoded.Version)
		}
		ev, ok := decoded.Value.(Event)
		if !ok {
			return Record{}, fmt.Errorf("runmod: record: %s decoded to %T", e.Type, decoded.Value)
		}
		if ev.RunID != runID {
			continue
		}
		if len(record.Events) == 0 {
			record.Created = session.StreamSeq(i)
		}
		record.Events = append(record.Events, *e)
		record.Facts = append(record.Facts, ev.Fact)
		position = run.RunPosition(i)
	}
	if len(record.Facts) == 0 {
		return Record{}, runtime.ErrRunNotFound
	}
	state, err := runtime.FoldRun(record.Facts)
	if err != nil {
		return Record{}, fmt.Errorf("runmod: record: %w", err)
	}
	if expect != nil && page.Head == expectHead && !wire.StatesEquivalent(&state, expect) {
		return Record{}, errors.New("runmod: record: projection diverges from the event fold")
	}
	record.Snapshot = runtime.Snapshot{State: state, Position: position}
	return record, nil
}
