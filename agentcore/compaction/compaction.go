// Package compaction records Session context-maintenance operations. An
// operation owns a frozen summary request and its effect identity, but is not
// a Turn or a conversation step. Conversation changes remain chatlog facts.
package compaction

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

type ID string

type Status string

const (
	Pending Status = "pending"
	Applied Status = "applied"
	Stale   Status = "stale"
	Failed  Status = "failed"
)

var (
	ErrPending  = errors.New("compaction: a context-maintenance operation is pending")
	ErrConflict = errors.New("compaction: operation identity conflict")
)

// Request is the immutable intent recorded before dispatch. Preparation names
// the candidate-request boundary (not its token count), when automatic. The
// request digest pins both the summary model's input and its generation options.
// Effect identities are derived from ID under this payload's version, so a new
// owner uses precisely the same assignment as its predecessor.
type Request struct {
	ID            ID                     `json:"id"`
	Plan          chatlog.CompactionPlan `json:"plan"`
	Model         turn.ModelRef          `json:"model"`
	RequestDigest turn.Digest            `json:"requestDigest"`
	Preparation   turn.Digest            `json:"preparation,omitempty"`
	Target        *turn.TargetRef        `json:"target,omitempty"`
}

func (r *Request) Key(sid session.SessionID) effect.AssignmentKey {
	return effect.AssignmentKey{Session: turn.Scope(sid), OwnerKind: effect.OwnerCompaction, OwnerID: string(r.ID), Effect: turn.EffectID("summary-" + r.ID)}
}

// Operation retains the intent after settlement too: an owner can finish the
// executor acknowledgement after crashing between the two durable commits.
type Operation struct {
	Request      Request              `json:"request"`
	Status       Status               `json:"status"`
	CompactionID chatlog.CompactionID `json:"compactionId,omitempty"`
	Code         string               `json:"code,omitempty"`
	Message      string               `json:"message,omitempty"`
	Acknowledged bool                 `json:"acknowledged,omitempty"`
}

type State struct {
	Operations map[ID]Operation `json:"operations"`
	Pending    ID               `json:"pending,omitempty"`
}

// NeedsRecovery includes completed operations whose Outcome has not yet been
// acknowledged. Hosts must also count a pending operation as Session work.
func (s *State) NeedsRecovery() bool {
	for id := range s.Operations {
		if !s.Operations[id].Acknowledged {
			return true
		}
	}
	return false
}

const (
	ModuleID         module.ModuleID     = "compaction"
	ProjectionID     module.ProjectionID = "twilight/compaction/operations"
	TypeRequested    ledger.EventType    = "twilight/compaction/requested"
	TypeFinished     ledger.EventType    = "twilight/compaction/finished"
	TypeAcknowledged ledger.EventType    = "twilight/compaction/acknowledged"
)

type RequestedPayload struct {
	Request Request `json:"request"`
}
type FinishedPayload struct {
	ID           ID                   `json:"id"`
	Status       Status               `json:"status"`
	CompactionID chatlog.CompactionID `json:"compactionId,omitempty"`
	Code         string               `json:"code,omitempty"`
	Message      string               `json:"message,omitempty"`
}
type AcknowledgedPayload struct {
	ID ID `json:"id"`
}

var streamDefinition = module.StreamDefinition{Domain: "compaction", Inheritance: module.Own}
var Stream = streamDefinition.Ref("")

func checkRequest(p *RequestedPayload) error {
	r := &p.Request
	if r.ID == "" || r.Model == "" || r.RequestDigest == "" || r.Plan.BaseContextDigest == "" || r.Plan.CoveredThrough.Commit == 0 {
		return errors.New("compaction: incomplete request")
	}
	_, err := sessionstore.FrozenRef(r.RequestDigest)
	return err
}

func checkFinished(p *FinishedPayload) error {
	if p.ID == "" {
		return errors.New("compaction: finished requires id")
	}
	switch p.Status {
	case Applied:
		if p.CompactionID == "" || p.Code != "" || p.Message != "" {
			return errors.New("compaction: applied requires compactionId and no failure")
		}
	case Stale, Failed:
		if p.CompactionID != "" || p.Code == "" {
			return errors.New("compaction: unsuccessful operation requires code and no compactionId")
		}
	default:
		return errors.New("compaction: invalid terminal status")
	}
	return nil
}

var requestBindings = module.BindingReferenceDefinition{
	Extractor: module.BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
		p, ok := value.(RequestedPayload)
		if !ok {
			return nil, fmt.Errorf("compaction: unexpected request %T", value)
		}
		return []artifact.BindingID{sessionstore.FrozenBindingID(p.Request.RequestDigest)}, nil
	}),
	Cardinality:        module.Cardinality{Min: 1},
	AllowedSchemes:     []artifact.Scheme{artifact.SchemeCAS},
	RequiredDurability: artifact.EventBound,
}

// Operations do not cross a fork: the child inherits the conversation, not
// its parent's in-flight summary execution or acknowledgement responsibility.
var Projection = module.ProjectionDefinition{
	ID: ProjectionID, Version: 1,
	Consumes:            []ledger.EventType{TypeRequested, TypeFinished, TypeAcknowledged},
	RequiredForCommit:   true,
	InheritanceOverride: module.InheritNone,
	Initial:             func() (any, error) { return State{Operations: map[ID]Operation{}}, nil },
	StateCodec:          module.JSONStateCodec[State]{},
	Apply:               apply,
}

var Module = module.ModuleDescriptor{
	Source: module.SourceTwilight, ID: ModuleID,
	Streams: []module.StreamDefinition{streamDefinition},
	Events: []module.EventDefinition{
		{Type: TypeRequested, Domain: streamDefinition.Domain, Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[RequestedPayload]{Check: checkRequest}}, Bindings: []module.BindingReferenceDefinition{requestBindings}},
		{Type: TypeFinished, Domain: streamDefinition.Domain, Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[FinishedPayload]{Check: checkFinished}}},
		{Type: TypeAcknowledged, Domain: streamDefinition.Domain, Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[AcknowledgedPayload]{Check: func(p *AcknowledgedPayload) error {
			if p.ID == "" {
				return errors.New("compaction: acknowledgement requires id")
			}
			return nil
		}}}},
	},
	Projections: []module.ProjectionDefinition{Projection},
}

func apply(state any, event module.DecodedEvent) (any, error) {
	s, ok := state.(State)
	if !ok {
		return nil, fmt.Errorf("compaction: projection is %T", state)
	}
	ops := make(map[ID]Operation, len(s.Operations)+1)
	for id := range s.Operations {
		ops[id] = s.Operations[id]
	}
	s.Operations = ops
	switch p := event.Value.(type) {
	case RequestedPayload:
		if _, exists := ops[p.Request.ID]; exists || s.Pending != "" {
			return nil, ErrConflict
		}
		ops[p.Request.ID] = Operation{Request: p.Request, Status: Pending}
		s.Pending = p.Request.ID
	case FinishedPayload:
		op, exists := ops[p.ID]
		if !exists || op.Status != Pending || s.Pending != p.ID {
			return nil, ErrConflict
		}
		op.Status, op.CompactionID, op.Code, op.Message = p.Status, p.CompactionID, p.Code, p.Message
		ops[p.ID], s.Pending = op, ""
	case AcknowledgedPayload:
		op, exists := ops[p.ID]
		if !exists || op.Status == Pending || op.Acknowledged {
			return nil, ErrConflict
		}
		op.Acknowledged = true
		ops[p.ID] = op
	default:
		return nil, fmt.Errorf("compaction: unexpected event %T", event.Value)
	}
	return s, nil
}

func Read(ctx context.Context, r session.ProjectionReader, sid session.SessionID) (State, error) {
	state, _, err := r.Load(ctx, sid, ProjectionID, Projection.Version)
	if err != nil {
		return State{}, err
	}
	s, ok := state.(State)
	if !ok {
		return State{}, fmt.Errorf("compaction: projection is %T", state)
	}
	return s, nil
}

func fromView(v writer.View) (State, error) {
	state, err := v.Projection(ProjectionID, Projection.Version)
	if err != nil {
		return State{}, err
	}
	s, ok := state.(State)
	if !ok {
		return State{}, fmt.Errorf("compaction: projection is %T", state)
	}
	return s, nil
}

// RequireIdle is a PrepareModelRequest consistency guard, evaluated inside
// the same Writer critical section as the Turn command. No candidate can be
// frozen into a ModelStep while an operation may still replace its context.
func RequireIdle(v writer.View) error {
	s, err := fromView(v)
	if err != nil {
		return err
	}
	if s.Pending != "" {
		return ErrPending
	}
	return nil
}

func fingerprint(r *Request) (jsonstable.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(1, "twilight/compaction/request", r)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

// Start contributes the durable intent. The caller must additionally enforce
// its Turn quiescence guard in the same unit before this Part is evaluated.
func Start(request Request) unit.Part {
	return unit.PartFunc(func(_ context.Context, v writer.View, now int64) ([]writer.TypedBatch, error) {
		if err := checkRequest(&RequestedPayload{Request: request}); err != nil {
			return nil, err
		}
		s, err := fromView(v)
		if err != nil {
			return nil, err
		}
		if previous, exists := s.Operations[request.ID]; exists {
			oldDigest, err := fingerprint(&previous.Request)
			if err != nil {
				return nil, err
			}
			newDigest, err := fingerprint(&request)
			if err != nil {
				return nil, err
			}
			if oldDigest != newDigest {
				return nil, ErrConflict
			}
			return nil, nil
		}
		if s.Pending != "" {
			return nil, ErrPending
		}
		state, err := v.Projection(chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		contextState, ok := state.(chatlog.Context)
		if !ok {
			return nil, fmt.Errorf("compaction: context is %T", state)
		}
		if err := request.Plan.Validate(contextState.Entries); err != nil {
			return nil, err
		}
		return batch(now, TypeRequested, RequestedPayload{Request: request}), nil
	})
}

// Finish contributes only the operation's disposition; an Applied finish
// must be combined with chatlog.CompactContext in the same unit of work.
func Finish(result FinishedPayload) unit.Part {
	return unit.PartFunc(func(_ context.Context, v writer.View, now int64) ([]writer.TypedBatch, error) {
		if err := checkFinished(&result); err != nil {
			return nil, err
		}
		s, err := fromView(v)
		if err != nil {
			return nil, err
		}
		if op, ok := s.Operations[result.ID]; !ok || op.Status != Pending {
			return nil, ErrConflict
		}
		return batch(now, TypeFinished, result), nil
	})
}

func Acknowledge(id ID) unit.Part {
	return unit.PartFunc(func(_ context.Context, v writer.View, now int64) ([]writer.TypedBatch, error) {
		s, err := fromView(v)
		if err != nil {
			return nil, err
		}
		if op, ok := s.Operations[id]; !ok || op.Status == Pending || op.Acknowledged {
			return nil, ErrConflict
		}
		return batch(now, TypeAcknowledged, AcknowledgedPayload{ID: id}), nil
	})
}

func batch(now int64, typ ledger.EventType, value any) []writer.TypedBatch {
	return []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{Type: typ, RecordedAtUnixMilli: now, Value: value}}}}
}
