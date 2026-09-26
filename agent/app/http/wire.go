// Package http is the HTTP binding of the owner's command face (CLD-GWY-1,
// CLD-WIR-0): what a gateway, a controller or a test drives an
// app.Application through. Writes reach a Session only as inbox commands
// (APP-INB-1), so every command is durable before it is answered and is
// applied by whichever owner holds the Session; reads are the lease-free
// read side (OWN-HDL-2). Server binds an Application, Client is the same
// face for the caller.
package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/turn"
)

// Error is the body of every error response and the error a Client returns
// for one: Status is the HTTP status, Code a stable word.
type Error struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("owner http: %d %s: %s", e.Status, e.Code, e.Message)
}

// The Codes an Error carries.
const (
	CodeNotFound        = "not_found"
	CodeOwned           = "owned"
	CodeNotOpen         = "not_open"
	CodeConflict        = "conflict"
	CodeInvalid         = "invalid_request"
	CodeUnavailable     = "unavailable"
	CodeCommandConflict = "command_conflict"
	CodeInternal        = "internal"
)

// IsCode reports whether err is an Error with the code.
func IsCode(err error, code string) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Code == code
}

// OpenRequest opens a Session in this owner (APP-SES-1).
type OpenRequest struct {
	// Preset is the PresetID new Turns run under; the owner resolves it to
	// the registered PresetRef.
	Preset turn.PresetID `json:"preset"`
	// InheritedWorkspace is the policy for a binding inherited from the fork
	// parent (APP-WSP-5): share (default), none, allocate, clone, restore.
	InheritedWorkspace string `json:"inheritedWorkspace,omitempty"`
	// ResumeActive drives a still-active Turn inside the open call.
	ResumeActive bool `json:"resumeActive,omitempty"`
}

// OpenResponse reports the opened Session.
type OpenResponse struct {
	Recovered int         `json:"recovered"`
	Active    turn.TurnID `json:"active,omitempty"`
	// AlreadyOpen: this owner held the Session already; nothing changed.
	AlreadyOpen bool `json:"alreadyOpen,omitempty"`
}

// WakeResponse reports whether this owner holds the Session and woke it.
type WakeResponse struct {
	Open bool `json:"open"`
}

// TurnResponse is one Turn's view and, once completed, its reply.
type TurnResponse struct {
	Turn  turn.TurnView `json:"turn"`
	Reply string        `json:"reply,omitempty"`
}

// LeaseResponse is the Session's writer lease (SES-OWN-5).
type LeaseResponse struct {
	Held  bool   `json:"held"`
	Lease *Lease `json:"lease,omitempty"`
}

// Lease is the wire form of session.Lease.
type Lease struct {
	Session        session.SessionID `json:"session"`
	Epoch          session.Epoch     `json:"epoch"`
	Owner          string            `json:"owner"`
	UntilUnixMilli int64             `json:"untilUnixMilli"`
}

// AllocateWorkspaceRequest creates a Workspace record (APP-WSP-2).
type AllocateWorkspaceRequest struct {
	Project string                `json:"project,omitempty"`
	Base    workspace.RevisionRef `json:"base,omitempty"`
}

// ForkRequest forks a Session: before a Turn (OWN-FRK-2) when BeforeTurn is
// set, at commit At otherwise (OWN-FRK-1).
type ForkRequest struct {
	Child      session.SessionID `json:"child"`
	BeforeTurn turn.TurnID       `json:"beforeTurn,omitempty"`
	At         session.CommitSeq `json:"at,omitempty"`
}

// Event is the wire form of an observe.Event (OBS-1): a committed fact with
// its ledger position and decoded value, a transient progress observation,
// or a stream failure.
type Event struct {
	Session  session.SessionID        `json:"session"`
	Position session.Position         `json:"position"`
	Type     session.EventType        `json:"type,omitempty"`
	Module   extension.ModuleKey      `json:"module,omitempty"`
	Version  extension.PayloadVersion `json:"version,omitempty"`
	Value    json.RawMessage          `json:"value,omitempty"`
	Unknown  bool                     `json:"unknown,omitempty"`
	Error    string                   `json:"error,omitempty"`
	Progress *observe.Progress        `json:"progress,omitempty"`
}

// EventOf renders an observe.Event.
func EventOf(e *observe.Event) Event {
	out := Event{Session: e.Session, Position: e.Position, Type: e.Row.Type, Module: e.Module, Version: e.Version, Unknown: e.Unknown, Progress: e.Progress}
	if e.Err != nil {
		out.Error = e.Err.Error()
	}
	if e.Value != nil {
		if raw, err := json.Marshal(e.Value); err == nil {
			out.Value = raw
		}
	}
	return out
}

// Entry is inbox.Entry.
type Entry = inbox.Entry

// Command is inbox.Command.
type Command = inbox.Command
