package ledger

type ErrorCode string

const (
	CodeInvalid       ErrorCode = "invalid"
	CodeUnknownEvent  ErrorCode = "unknown_event"
	CodeCodec         ErrorCode = "codec"
	CodeBinding       ErrorCode = "binding"
	CodeConflict      ErrorCode = "conflict"
	CodeOwnershipLost ErrorCode = "ownership_lost"
	// CodeProjectionUnhealthy: a derived projection failed to fold a commit
	// the Writer applied; its state is frozen at the last good commit until
	// the Writer reopens and rebuilds it (EXT-PRJ-9).
	CodeProjectionUnhealthy ErrorCode = "projection_unhealthy"
	// CodeUnknownOutcome: an Append failed in a way that leaves what reached
	// the log unknown (an IO error, or the kernel's ErrHandleFailed). The
	// Writer's head and projections may no longer match the log, so it fails
	// closed; the host reopens and replays (EXT-WRT-4).
	CodeUnknownOutcome ErrorCode = "unknown_outcome"
)

// Error is a classified failure of the ledger or of a module framework built
// on it: the class, the event type it concerns when there is one, and the
// detail.
type Error struct {
	Code   ErrorCode
	Type   EventType
	Detail string
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

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}
