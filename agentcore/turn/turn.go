// Package turn is the Turn module: the logical Turn's own facts
// (started, failed, superseded), the identity derivations that name them,
// the protocol vocabulary operated on them and the surface that projects
// Turn state from the Session's facts. Which Run executes a Turn and where
// an input was delivered are other modules' facts; the surface joins them
// and writes no derived copy.
package turn

import (
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
)

const (
	ModuleID module.ModuleID = "turn"
	// StreamDomain is the stream domain of Turn events: one keyed stream
	// per Turn, bound by the payload's turnId.
	StreamDomain = "turn"
)

// streamDefinition declares the turn domain: keyed by TurnID and of session
// lineage, so a fork continues its parent's Turns.
var streamDefinition = module.StreamDefinition{Domain: StreamDomain, Key: streamKey, Inheritance: module.Inherited}

// streamKey binds a turn event to its Turn's stream.
func streamKey(value any) (string, error) {
	switch p := value.(type) {
	case StartedPayload:
		return string(p.TurnID), nil
	case FailedPayload:
		return string(p.TurnID), nil
	case SupersededPayload:
		return string(p.TurnID), nil
	}
	return "", fmt.Errorf("value is %T, not a turn event", value)
}

// Stream is the logical stream of one Turn's events.
func Stream(turnID TurnID) ledger.Domain { return streamDefinition.Ref(string(turnID)) }

type TurnID string

type TurnRef struct {
	SessionID session.SessionID
	TurnID    TurnID
}

type Settlement string

const (
	SettlementCompleted Settlement = "completed"
	SettlementFailed    Settlement = "failed"
	SettlementStopped   Settlement = "stopped"
)

// EventTypes are the Turn domain's own decisions. A Run's end
// (twilight/run/run_ended) is not among them: the surface folds it from
// the run module that owns it.
const (
	TypeStarted    ledger.EventType = "twilight/turn/started"
	TypeFailed     ledger.EventType = "twilight/turn/failed"
	TypeSuperseded ledger.EventType = "twilight/turn/superseded"
)

type StartedPayload struct {
	TurnID   TurnID            `json:"turnId"`
	RunID    run.RunID         `json:"runId"`
	InputIDs []chatlog.InputID `json:"inputIds,omitempty"`
	Preset   preset.PresetRef  `json:"preset"`
}

// FailedPayload settles a Turn as failed or stopped. Reason is the
// caller's stated reason for a stop, kept as given; FailureClass is the
// settlement's class.
type FailedPayload struct {
	TurnID       TurnID     `json:"turnId"`
	RunID        run.RunID  `json:"runId"`
	Settlement   Settlement `json:"settlement"`
	FailureClass string     `json:"failureClass,omitempty"`
	Reason       string     `json:"reason,omitempty"`
}

type SupersededPayload struct {
	TurnID            TurnID `json:"turnId"`
	ReplacementTurnID TurnID `json:"replacementTurnId"`
}

// --- identity derivations -------------------------------------------------------

func digestOf(domain string, parts ...string) jsonstable.Digest {
	raw, _ := jsonstable.EncodeTypedPayload(1, domain, parts)
	return jsonstable.DigestBytes(raw)
}

// PlanDigest names the Turn plan a Start commits: the Turn, the preset
// digest and the ordered input IDs.
func PlanDigest(turnID TurnID, presetDigest jsonstable.Digest, inputs []chatlog.InputID) jsonstable.Digest {
	parts := make([]string, 0, 2+len(inputs))
	parts = append(parts, string(turnID), string(presetDigest))
	for _, id := range inputs {
		parts = append(parts, string(id))
	}
	return digestOf("twilight/turn/plan", parts...)
}

// StartOperationDigest is the Start commit's CommitID.
func StartOperationDigest(sid session.SessionID, turnID TurnID, plan jsonstable.Digest) jsonstable.Digest {
	return digestOf("twilight/turn/start-operation", string(sid), string(turnID), string(plan))
}

// DeriveRunID names the one Run that executes a Turn.
func DeriveRunID(sid session.SessionID, turnID TurnID) run.RunID {
	return run.RunID(digestOf("twilight/turn/run", string(sid), string(turnID)))
}

// CancelCommandID names the Run cancellation a Stop commits beside the
// Turn's failed settlement.
func CancelCommandID(sid session.SessionID, turnID TurnID, runID run.RunID) run.CommandID {
	return run.CommandID(digestOf("twilight/turn/cancel-run", string(sid), string(turnID), string(runID), string(run.ReasonCancelled)))
}

// --- module -----------------------------------------------------------------------

// Version is the payload version the turn module writes every event with.
const Version module.PayloadVersion = 1

func def[T any](typ ledger.EventType, check func(*T) error) module.EventDefinition {
	return module.EventDefinition{Type: typ, Domain: StreamDomain,
		Codecs: map[module.PayloadVersion]module.PayloadCodec{Version: module.JSONCodec[T]{Check: check}}}
}

// Module declares the turn events, the surface projection and the Requires
// of the Turn's scope: run (run_ended, which settles the Turn's Run) and
// chatlog (input_delivered, which extends the Turn's inputs).
var Module = module.ModuleDescriptor{
	Source:  module.SourceTwilight,
	ID:      ModuleID,
	Streams: []module.StreamDefinition{streamDefinition},
	Requires: []module.ModuleRequirement{
		{Source: module.SourceTwilight, Module: sessionstore.ModuleID, Events: []ledger.EventType{sessionstore.Prefix + "run_ended"}},
		{Source: module.SourceTwilight, Module: chatlog.ModuleID, Events: []ledger.EventType{chatlog.TypeInputDelivered}},
	},
	Events: []module.EventDefinition{
		def[StartedPayload](TypeStarted, func(p *StartedPayload) error {
			if p.TurnID == "" || p.RunID == "" || p.Preset.ID == "" || p.Preset.Digest == "" {
				return errors.New("started requires turnId, runId and preset")
			}
			return nil
		}),
		def[FailedPayload](TypeFailed, func(p *FailedPayload) error {
			if p.TurnID == "" || p.RunID == "" || (p.Settlement != SettlementFailed && p.Settlement != SettlementStopped) {
				return errors.New("failed requires turnId, runId and settlement failed|stopped")
			}
			return nil
		}),
		def[SupersededPayload](TypeSuperseded, func(p *SupersededPayload) error {
			if p.TurnID == "" || p.ReplacementTurnID == "" {
				return errors.New("superseded requires turnId and replacementTurnId")
			}
			return nil
		}),
	},
	Projections: []module.ProjectionDefinition{SurfaceProjection},
}
