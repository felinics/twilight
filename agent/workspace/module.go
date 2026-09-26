package workspace

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// The Session binding is a Session fact (APP-WSP-1): which Workspace the
// Session's workspace-placed tools run in. It lives in the Session ledger on
// the module's own logical stream, so the resolver reads it in the same view
// the Loop drives with, the PromptBuilder can tell the model where it works,
// and a fork inherits it: a child that folds its parent's prefix reads the
// parent's binding with the parent's Scope and treats it as inherited, which
// is the Share policy; the other fork policies write the child's own fact.
const (
	// Source is the SourceID of this agent's application modules (EXT-REG-1).
	Source extension.SourceID = "agent"
	// ModuleID names the module.
	ModuleID extension.ModuleID = "workspace"
	// StreamDomain is the singleton stream the binding facts live on.
	StreamDomain = "workspace"
	// Version is the payload version the module writes.
	Version extension.PayloadVersion = 1
	// TypeBound binds the Session to a Workspace.
	TypeBound session.EventType = "agent/workspace/bound"
	// TypeUnbound records that the Session works in no Workspace, ending a
	// binding of its own or an inherited one.
	TypeUnbound session.EventType = "agent/workspace/unbound"
	// TypeSnapshotted records a Snapshot of the Session's bound Workspace
	// at this point of the conversation (APP-WSP-7): what a fork at this
	// point restores.
	TypeSnapshotted session.EventType = "agent/workspace/snapshotted"
	// BindingProjectionID is the module's projection.
	BindingProjectionID extension.ProjectionID = "agent/workspace/binding"
	// TargetKind is the run.TargetRef Kind of a Workspace target.
	TargetKind = "workspace"
)

var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Lineage: session.LineageSession}

// Stream is the module's logical stream.
var Stream = streamDefinition.Ref("")

// BoundPayload is agent/workspace/bound. Scope is the Session the fact was
// written for: a fork's child reads its parent's facts with the parent's
// Scope.
type BoundPayload struct {
	Workspace ID                `json:"workspace"`
	Scope     session.SessionID `json:"scope"`
}

// UnboundPayload is agent/workspace/unbound.
type UnboundPayload struct {
	Scope  session.SessionID `json:"scope"`
	Reason string            `json:"reason,omitempty"`
}

// SnapshottedPayload is agent/workspace/snapshotted.
type SnapshottedPayload struct {
	Workspace ID                `json:"workspace"`
	Snapshot  SnapshotRef       `json:"snapshot"`
	Scope     session.SessionID `json:"scope"`
}

// Binding is the projection state: the Session's current Workspace, if any,
// the Session whose fact established it, and the latest Snapshot of that
// Workspace recorded on this Session's history.
type Binding struct {
	Bound     bool              `json:"bound"`
	Workspace ID                `json:"workspace,omitempty"`
	Scope     session.SessionID `json:"scope,omitempty"`
	Snapshot  SnapshotRef       `json:"snapshot,omitempty"`
}

// InheritedBy reports whether the binding sid reads was written for another
// Session: a fork's child sharing its parent's Workspace.
func (b Binding) InheritedBy(sid session.SessionID) bool { return b.Bound && b.Scope != sid }

// BindingProjection folds the module's two facts into the current Binding.
var BindingProjection = extension.ProjectionDefinition{
	ID: BindingProjectionID, Version: 1,
	Consumes:   []session.EventType{TypeBound, TypeUnbound, TypeSnapshotted},
	Initial:    func() (any, error) { return Binding{}, nil },
	Apply:      applyBinding,
	StateCodec: extension.JSONStateCodec[Binding]{},
}

//nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
func applyBinding(state any, e extension.DecodedEvent) (any, error) {
	b, ok := state.(Binding)
	if !ok {
		return nil, fmt.Errorf("workspace binding: state is %T", state)
	}
	switch p := e.Value.(type) {
	case BoundPayload:
		if b.Bound && b.Workspace == p.Workspace {
			// A restatement of the binding keeps the snapshot history.
			return Binding{Bound: true, Workspace: p.Workspace, Scope: p.Scope, Snapshot: b.Snapshot}, nil
		}
		return Binding{Bound: true, Workspace: p.Workspace, Scope: p.Scope}, nil
	case UnboundPayload:
		return Binding{Scope: p.Scope}, nil
	case SnapshottedPayload:
		if !b.Bound || b.Workspace != p.Workspace {
			return nil, fmt.Errorf("workspace binding: snapshot of %s while bound to %q", p.Workspace, b.Workspace)
		}
		b.Snapshot = p.Snapshot
		return b, nil
	default:
		return nil, fmt.Errorf("workspace binding: unexpected %T", e.Value)
	}
}

// Module is the workspace ModuleDescriptor: one singleton stream, two
// facts, one projection.
var Module = extension.ModuleDescriptor{
	Source:  Source,
	ID:      ModuleID,
	Streams: []extension.StreamDefinition{streamDefinition},
	Events: []extension.EventDefinition{
		{Type: TypeBound, Stream: StreamDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[BoundPayload]{Check: func(p *BoundPayload) error {
			if p.Workspace == "" || p.Scope == "" {
				return errors.New("workspace bound requires workspace and scope")
			}
			return nil
		}}}},
		{Type: TypeUnbound, Stream: StreamDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[UnboundPayload]{Check: func(p *UnboundPayload) error {
			if p.Scope == "" {
				return errors.New("workspace unbound requires scope")
			}
			return nil
		}}}},
		{Type: TypeSnapshotted, Stream: StreamDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[SnapshottedPayload]{Check: func(p *SnapshottedPayload) error {
			if p.Workspace == "" || p.Snapshot == "" || p.Scope == "" {
				return errors.New("workspace snapshotted requires workspace, snapshot and scope")
			}
			return nil
		}}}},
	},
	Projections: []extension.ProjectionDefinition{BindingProjection},
}

// Read folds the Session's Binding through a lease-free reader.
func Read(ctx context.Context, reader extension.ProjectionReader, sid session.SessionID) (Binding, error) {
	state, _, err := reader.Load(ctx, sid, BindingProjectionID, BindingProjection.Version)
	if err != nil {
		return Binding{}, err
	}
	b, ok := state.(Binding)
	if !ok {
		return Binding{}, fmt.Errorf("workspace binding: projection state is %T", state)
	}
	return b, nil
}

// Commands write the binding facts through a Session's Writer (APP-WSP-2).
type Commands struct {
	// Now stamps the facts; nil selects time.Now.
	Now func() time.Time
}

func (c *Commands) now() int64 {
	if c.Now == nil {
		return time.Now().UnixMilli()
	}
	return c.Now().UnixMilli()
}

func current(v writer.View) (Binding, error) {
	state, err := v.Projection(BindingProjectionID, BindingProjection.Version)
	if err != nil {
		return Binding{}, err
	}
	b, ok := state.(Binding)
	if !ok {
		return Binding{}, fmt.Errorf("workspace binding: projection state is %T", state)
	}
	return b, nil
}

// Bind binds the Session to id. A Session already bound to id by its own
// fact writes nothing; an inherited binding to the same id is restated as
// the Session's own. The CommitID carries the head, so a retry at the same
// head replays and a later Bind is a new fact.
func (c *Commands) Bind(ctx context.Context, w writer.Writer, id ID) error {
	if id == "" {
		return errors.New("workspace: bind requires a workspace id")
	}
	sid := w.SessionID()
	return c.commit(ctx, w, func(v writer.View, b Binding) (session.CommitID, writer.TypedEvent, bool) {
		if b.Bound && b.Workspace == id && b.Scope == sid {
			return "", writer.TypedEvent{}, false
		}
		return session.CommitID("workspace-bound/" + string(id) + "/" + strconv.FormatUint(uint64(v.Head().Next), 10)),
			writer.TypedEvent{Type: TypeBound, RecordedAtUnixMilli: c.now(), Value: BoundPayload{Workspace: id, Scope: sid}}, true
	})
}

// Unbind records that the Session works in no Workspace; a Session with no
// binding, own or inherited, writes nothing.
func (c *Commands) Unbind(ctx context.Context, w writer.Writer, reason string) error {
	sid := w.SessionID()
	return c.commit(ctx, w, func(v writer.View, b Binding) (session.CommitID, writer.TypedEvent, bool) {
		if !b.Bound {
			return "", writer.TypedEvent{}, false
		}
		return session.CommitID("workspace-unbound/" + strconv.FormatUint(uint64(v.Head().Next), 10)),
			writer.TypedEvent{Type: TypeUnbound, RecordedAtUnixMilli: c.now(), Value: UnboundPayload{Scope: sid, Reason: reason}}, true
	})
}

// RecordSnapshot records that snap is the latest Snapshot of the Session's
// bound Workspace (APP-WSP-7); a Session bound to another Workspace, or to
// none, is an error, and a snapshot already recorded writes nothing.
func (c *Commands) RecordSnapshot(ctx context.Context, w writer.Writer, snap *Snapshot) error {
	if snap == nil || snap.Ref == "" || snap.Workspace == "" {
		return errors.New("workspace: record snapshot requires a snapshot with a ref and a workspace")
	}
	sid := w.SessionID()
	return c.commit(ctx, w, func(_ writer.View, b Binding) (session.CommitID, writer.TypedEvent, bool) {
		if b.Snapshot == snap.Ref {
			return "", writer.TypedEvent{}, false
		}
		return session.CommitID("workspace-snapshotted/" + string(snap.Ref)),
			writer.TypedEvent{Type: TypeSnapshotted, RecordedAtUnixMilli: c.now(), Value: SnapshottedPayload{Workspace: snap.Workspace, Snapshot: snap.Ref, Scope: sid}}, true
	})
}

func (c *Commands) commit(ctx context.Context, w writer.Writer, decide func(writer.View, Binding) (session.CommitID, writer.TypedEvent, bool)) error {
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		b, err := current(v)
		if err != nil {
			return nil, err
		}
		id, ev, write := decide(v, b)
		if !write {
			return nil, nil
		}
		return &writer.SemanticGroup{CommitID: id, Batches: []writer.TypedBatch{{Stream: Stream, Events: []writer.TypedEvent{ev}}}}, nil
	})
	if err != nil {
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	default:
		return fmt.Errorf("workspace: binding commit: %s: %s", res.Outcome, res.Detail)
	}
}
