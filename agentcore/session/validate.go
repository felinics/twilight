package session

import (
	"fmt"
	"unicode/utf8"
)

func validIdentity(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	return nil
}
