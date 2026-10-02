package writer

import "errors"

// The conditions a Writer reports as its own. Each is a sentinel the
// Writer wraps with its detail; callers match with errors.Is.
var (
	// ErrOwnershipLost: the Session's ownership was superseded. A fenced
	// Append or a fenced lease renewal puts the Writer here for good; every
	// further commit through it is refused with this (EXT-WRT-4).
	ErrOwnershipLost = errors.New("writer: ownership lost")
	// ErrUnknownOutcome: an Append failed in a way that leaves what reached
	// the log unknown (an IO error, or the kernel's handle failure). The
	// Writer's head and projections may no longer match the log, so it
	// fails closed; the host reopens and replays (EXT-WRT-4).
	ErrUnknownOutcome = errors.New("writer: append outcome unknown")
	// ErrProjectionUnhealthy: a derived projection failed to fold a commit
	// the Writer applied; its state is frozen at the last good commit until
	// the Writer reopens and rebuilds it (EXT-PRJ-9).
	ErrProjectionUnhealthy = errors.New("writer: projection unhealthy")
	// ErrClosed: the Writer was closed and answers nothing more.
	ErrClosed = errors.New("writer: closed")
)
