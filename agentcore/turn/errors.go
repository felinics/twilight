package turn

import "errors"

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")
