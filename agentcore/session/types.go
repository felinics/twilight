// Package session is the commit-ledger kernel of a Twilight Session
// (docs/design/agent-session.md). It owns the header, the Commit as the
// atomic unit of append, logical streams within commits, Session-level
// writer ownership with epoch fencing, and ordered reads over an append-only
// store. Payloads are opaque canonical JSON that Session modules encode and
// interpret.
package session

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
)

type (
	SessionID string
	CommitID  string
	EventType string
	// Epoch is the writer ownership generation of a stream, from 1.
	Epoch uint64
)

// SegmentHeader is the immutable creation record of a commit segment
// (agent-session.md section 8): a node of the lineage tree. It names no
// Session: which roots append to or inherit from the segment is the roots'
// business (SessionRecord), and a segment outlives every Session that named
// it for as long as some root reaches it. ID is the segment's identity,
// drawn at random, so two otherwise identical records are two segments.
// Readers of a Session see the header of the segment its root names as its
// tip.
type SegmentHeader struct {
	// ID is the segment's identity: 128 random bits the kernel draws at
	// Create, hex encoded. Nothing derives it, so two segments
	// with otherwise equal records are two nodes (SES-WIR-4).
	ID          SegmentID      `json:"id"`
	Parent      *LedgerRef     `json:"parent,omitempty"` // nil for a root segment; the edge to the parent otherwise
	CausationID es.CausationID `json:"causationId,omitempty"`
	// Ext holds the module extension slots of the creation record, one raw
	// value per module (SES-WIR-5); a reader that knows none of the modules
	// keeps them.
	Ext Extensions `json:"ext,omitempty"`
}

// ErrorCode classifies kernel failures (SES 7).
type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrNotFound      ErrorCode = "not_found"
	ErrConflict      ErrorCode = "conflict"
	ErrCorrupt       ErrorCode = "corrupt"
	ErrOwned         ErrorCode = "owned"
	ErrOwnershipLost ErrorCode = "ownership_lost"
	// ErrHandleFailed: a previous Append of this Handle failed while writing
	// or persisting, so what reached storage is unknown. The Handle refuses
	// further Appends; the caller reopens and Open reads the log as it is
	// (SES-APP-1).
	ErrHandleFailed ErrorCode = "handle_failed"
	ErrUnsupported  ErrorCode = "unsupported"
)

// Error is the kernel's discriminable error value.
type Error struct {
	Code      ErrorCode
	Operation string
	SessionID SessionID
	CommitID  CommitID
	Detail    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("session: %s: %s", e.Operation, e.Code)
	if e.SessionID != "" {
		s += fmt.Sprintf(" session=%s", e.SessionID)
	}
	if e.CommitID != "" {
		s += fmt.Sprintf(" commit=%s", e.CommitID)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Is lets callers match on the code: errors.Is(err, &Error{Code: ErrNotFound}).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.Code == e.Code && (t.Operation == "" || t.Operation == e.Operation)
}

func newError(code ErrorCode, op string, sid SessionID, detail string) *Error {
	return &Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

// IsCode reports whether err is a kernel Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	for err != nil {
		var ce *Error
		if errors.As(err, &ce) {
			e = ce
			break
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return e != nil && e.Code == code
}
