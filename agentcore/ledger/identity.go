package ledger

import (
	"fmt"
	"unicode/utf8"
)

// ValidIdentity checks that v is a non-empty valid-UTF-8 identity string.
func ValidIdentity(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	return nil
}
