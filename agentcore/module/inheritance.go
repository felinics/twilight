package module

import (
	"errors"
	"fmt"
)

// Inheritance is how a stream reads across a parent edge. A child segment's
// path includes a prefix of its parent's commits; the module that owns a
// stream declares whether the stream is one history across that edge or
// starts empty in every segment, and a read names the mode it wants. The
// fold applies the mode it is given.
type Inheritance string

const (
	// Inherited reads the stream as one history across parent edges: the
	// inherited prefix stitched before the tip's own commits, so a child
	// continues the stream at the next event after that prefix.
	Inherited Inheritance = "inherited"
	// Own reads the stream as the history of the segment that wrote it: the
	// tip's own commits only, so a child or a new tip starts the stream
	// empty.
	Own Inheritance = "own"
)

// ValidateInheritance checks that a read names one of the two modes.
func ValidateInheritance(m Inheritance) error {
	switch m {
	case Inherited, Own:
		return nil
	case "":
		return errors.New("stream inheritance is empty")
	default:
		return fmt.Errorf("unknown stream inheritance %q", m)
	}
}
