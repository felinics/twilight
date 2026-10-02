package session

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
)

// ErrorCode classifies kernel failures (SES 7): what a caller can act on.
type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrNotFound      ErrorCode = "not_found"
	ErrConflict      ErrorCode = "conflict"
	ErrCorrupt       ErrorCode = "corrupt"
	ErrOwned         ErrorCode = "owned"
	ErrOwnershipLost ErrorCode = "ownership_lost"
	// ErrReferenced: a segment removal found a live tip or a child edge
	// still naming the node; nothing was removed (SES-GC-4).
	ErrReferenced ErrorCode = "referenced"
	// ErrDeleted: the SessionID names a deleted Session; it is never reused
	// (SES-GC-1).
	ErrDeleted ErrorCode = "deleted"
	// ErrHandleFailed: a previous Append of this Handle failed while writing
	// or persisting, so what reached storage is unknown. The Handle refuses
	// further Appends; the caller reopens and Open reads the log as it is
	// (SES-APP-1).
	ErrHandleFailed ErrorCode = "handle_failed"
	ErrUnsupported  ErrorCode = "unsupported"
)

// Error is the kernel's discriminable error value: the class, the operation
// that failed, the Session and commit it concerns when there are ones, and
// the detail. Callers classify it with the Is predicates.
type Error struct {
	Code      ErrorCode
	Operation string
	SessionID SessionID
	CommitID  ledger.CommitID
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

// NewError is an Error of code from operation op about sid.
func NewError(code ErrorCode, op string, sid SessionID, detail string) *Error {
	return &Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

func newError(code ErrorCode, op string, sid SessionID, detail string) *Error {
	return NewError(code, op, sid, detail)
}

// IsCode reports whether err is or wraps a kernel Error of code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// The predicates of each code.
func IsInvalid(err error) bool       { return IsCode(err, ErrInvalid) }
func IsNotFound(err error) bool      { return IsCode(err, ErrNotFound) }
func IsConflict(err error) bool      { return IsCode(err, ErrConflict) }
func IsCorrupt(err error) bool       { return IsCode(err, ErrCorrupt) }
func IsOwned(err error) bool         { return IsCode(err, ErrOwned) }
func IsOwnershipLost(err error) bool { return IsCode(err, ErrOwnershipLost) }
func IsReferenced(err error) bool    { return IsCode(err, ErrReferenced) }
func IsDeleted(err error) bool       { return IsCode(err, ErrDeleted) }
func IsHandleFailed(err error) bool  { return IsCode(err, ErrHandleFailed) }
