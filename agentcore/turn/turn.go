// Package turn is the first-party Turn module (docs/design/agent-turn.md):
// the logical turn, mid-turn input delivery and settlement. Which Run
// executes which attempt of a Turn is the attempt module's fact
// (agent/session/attempt); attempt outcomes are the Run's own run_ended
// fact. The surface joins the three and writes no derived copy of them.
package turn

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

const (
	ModuleID extension.ModuleID = "turn"
	// StreamDomain is the stream domain of Turn events: one keyed stream
	// per Turn, bound by the payload's turnId.
	StreamDomain = "turn"
)

// streamDefinition declares the turn domain: keyed by TurnID and of session
// lineage, so a fork continues its parent's Turns.
var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Key: streamKey, Lineage: session.LineageSession}

// streamKey binds a turn event to its Turn's stream (EXT-STR-1).
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
func Stream(turnID TurnID) session.StreamRef { return streamDefinition.Ref(string(turnID)) }

type (
	TurnID   string
	PresetID string
)

type TurnRef struct {
	SessionID session.SessionID
	TurnID    TurnID
}

type PresetRef struct {
	ID     PresetID  `json:"id"`
	Digest es.Digest `json:"digest"`
}

type Settlement string

const (
	SettlementCompleted Settlement = "completed"
	SettlementFailed    Settlement = "failed"
	SettlementStopped   Settlement = "stopped"
)

// EventTypes (TRN-EVT-1): the Turn domain's own decisions. Neither the
// binding of an attempt to its Run (twilight/attempt/started) nor an
// attempt's end (twilight/run/run_ended) is among them: the surface folds
// both from the modules that own them.
const (
	TypeStarted    session.EventType = "twilight/turn/started"
	TypeFailed     session.EventType = "twilight/turn/failed"
	TypeSuperseded session.EventType = "twilight/turn/superseded"
)

type StartedPayload struct {
	TurnID   TurnID            `json:"turnId"`
	InputIDs []chatlog.InputID `json:"inputIds,omitempty"`
	Preset   PresetRef         `json:"preset"`
}

type FailedPayload struct {
	TurnID       TurnID     `json:"turnId"`
	RunID        run.RunID  `json:"runId"`
	Settlement   Settlement `json:"settlement"`
	FailureClass string     `json:"failureClass,omitempty"`
}

type SupersededPayload struct {
	TurnID            TurnID `json:"turnId"`
	ReplacementTurnID TurnID `json:"replacementTurnId"`
}

// --- identity derivations (TRN-ID) ----------------------------------------------

func digestOf(domain string, parts ...string) es.Digest {
	raw, _ := es.EncodeTypedPayload(1, domain, parts)
	return es.DigestBytes(raw)
}

// PlanDigest is TRN-ID-2.
func PlanDigest(turnID TurnID, preset es.Digest, inputs []chatlog.InputID) es.Digest {
	parts := make([]string, 0, 2+len(inputs))
	parts = append(parts, string(turnID), string(preset))
	for _, id := range inputs {
		parts = append(parts, string(id))
	}
	return digestOf("twilight/turn/plan", parts...)
}

// StartOperationDigest is TRN-ID-3; it is the Start commit's CommitID.
func StartOperationDigest(sid session.SessionID, turnID TurnID, plan es.Digest) es.Digest {
	return digestOf("twilight/turn/start-operation", string(sid), string(turnID), string(plan))
}

// DeriveRunID is TRN-ID-4.
func DeriveRunID(sid session.SessionID, turnID TurnID, ordinal uint32) run.RunID {
	return run.RunID(digestOf("twilight/turn/run", string(sid), string(turnID), fmt.Sprintf("%d", ordinal)))
}

func RetryCommitID(sid session.SessionID, turnID TurnID, ordinal uint32) session.CommitID {
	return session.CommitID(digestOf("twilight/turn/retry", string(sid), string(turnID), fmt.Sprintf("%d", ordinal)))
}

func CancelCommandID(sid session.SessionID, turnID TurnID, runID run.RunID) run.CommandID {
	return run.CommandID(digestOf("twilight/turn/cancel-run", string(sid), string(turnID), string(runID), string(run.ReasonCancelled)))
}

func SettleCommitID(sid session.SessionID, turnID TurnID, runID run.RunID) session.CommitID {
	return session.CommitID(digestOf("twilight/turn/settle", string(sid), string(turnID), string(runID)))
}

// --- module -----------------------------------------------------------------------

// Version is the payload version the turn module writes every event with
// (EXT-REG-2).
const Version extension.PayloadVersion = 1

func def[T any](typ session.EventType, check func(*T) error) extension.EventDefinition {
	return extension.EventDefinition{Type: typ, Stream: StreamDomain,
		Codecs: map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[T]{Check: check}}}
}

// Module declares the turn events, the surface projection and the Requires of
// TRN-SCP-1: attempt (started, which binds a Run to a Turn), run (run_ended,
// which settles the attempt) and chatlog (input_delivered).
var Module = extension.ModuleDescriptor{
	Source:  extension.SourceTwilight,
	ID:      ModuleID,
	Streams: []extension.StreamDefinition{streamDefinition},
	Requires: []extension.ModuleRequirement{
		{Source: extension.SourceTwilight, Module: attempt.ModuleID, Events: []session.EventType{attempt.TypeStarted}},
		{Source: extension.SourceTwilight, Module: runmod.ModuleID, Events: []session.EventType{runmod.Prefix + "run_ended"}},
		{Source: extension.SourceTwilight, Module: chatlog.ModuleID, Events: []session.EventType{chatlog.TypeInputDelivered}},
	},
	Events: []extension.EventDefinition{
		def[StartedPayload](TypeStarted, func(p *StartedPayload) error {
			if p.TurnID == "" || p.Preset.ID == "" || p.Preset.Digest == "" {
				return errors.New("started requires turnId and preset")
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
	Projections: []extension.ProjectionDefinition{SurfaceProjection},
}
