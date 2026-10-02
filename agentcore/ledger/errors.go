package ledger

import "errors"

// ErrorCode classifies a failure of the ledger or of a module framework
// built on it: what a caller can act on, never how it happened.
type ErrorCode string

const (
	// CodeInvalid: a declaration or a value the framework refuses.
	CodeInvalid ErrorCode = "invalid"
	// CodeUnknownEvent: an event type or payload version no module of the
	// registry declares.
	CodeUnknownEvent ErrorCode = "unknown_event"
	// CodeCodec: a payload that does not encode or decode under its codec.
	CodeCodec ErrorCode = "codec"
)

// Error is a classified failure: the class, the event type it concerns
// when there is one, and the detail. Callers classify it with the Is
// predicates; the constructors are the only way one is made.
type Error struct {
	Code   ErrorCode
	Type   EventType
	Detail string
}

// NewInvalid is an Error of CodeInvalid about typ, or about no type when
// typ is empty.
func NewInvalid(typ EventType, detail string) *Error {
	return &Error{Code: CodeInvalid, Type: typ, Detail: detail}
}

// NewUnknownEvent is an Error of CodeUnknownEvent about typ.
func NewUnknownEvent(typ EventType, detail string) *Error {
	return &Error{Code: CodeUnknownEvent, Type: typ, Detail: detail}
}

// NewCodec is an Error of CodeCodec about typ.
func NewCodec(typ EventType, detail string) *Error {
	return &Error{Code: CodeCodec, Type: typ, Detail: detail}
}

func (e *Error) Error() string {
	s := "ledger: " + string(e.Code)
	if e.Type != "" {
		s += " " + string(e.Type)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// IsCode reports whether err is or wraps an Error of code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// IsInvalid reports an Error of CodeInvalid.
func IsInvalid(err error) bool { return IsCode(err, CodeInvalid) }

// IsUnknownEvent reports an Error of CodeUnknownEvent.
func IsUnknownEvent(err error) bool { return IsCode(err, CodeUnknownEvent) }

// IsCodec reports an Error of CodeCodec.
func IsCodec(err error) bool { return IsCode(err, CodeCodec) }
