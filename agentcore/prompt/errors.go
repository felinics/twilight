package prompt

import (
	"errors"
)

// ErrUnknownPromptBuilder reports a PromptBuilderRef the catalog does not
// hold: the Owner cannot rebuild the Turn's prompt builder (DEC-CAT-2).
var ErrUnknownPromptBuilder = errors.New("prompt: unknown prompt builder")
