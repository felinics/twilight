package turn

import "errors"

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")

// ErrNoForkPoint reports a fork point the Turn history refuses: inside an
// active Turn, or before a Turn that opens the history.
var ErrNoForkPoint = errors.New("turn: no fork point")
