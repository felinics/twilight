package prompt

import (
	"errors"
)

// ErrUnknownBuilder reports a PromptBuilderRef the catalog does not hold:
// the process cannot rebuild the Turn's prompt builder.
var ErrUnknownBuilder = errors.New("prompt: unknown builder")
