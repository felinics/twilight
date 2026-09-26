package decision

import "errors"

// ErrUnknownPromptBuilder reports a PromptBuilderRef the registry does not
// hold: the Owner cannot rebuild the Turn's decision function
// (DEC-CAT-2).
var ErrUnknownPromptBuilder = errors.New("decision: unknown prompt builder")
