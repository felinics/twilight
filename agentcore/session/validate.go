package session

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// validateEventShape checks the event invariants:
// a non-empty valid-UTF-8 type and a canonical JSON object payload.
func validateEventShape(typ EventType, payload jsonstable.Value) error {
	if err := validIdentity("EventType", string(typ)); err != nil {
		return err
	}
	if payload.IsZero() {
		return errors.New("empty payload")
	}
	canon, err := jsonstable.Canonicalize(payload.Bytes())
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if !bytes.Equal(canon, payload.Bytes()) {
		return errors.New("payload is not canonical")
	}
	if !bytes.HasPrefix(bytes.TrimSpace(payload.Bytes()), []byte("{")) {
		return errors.New("payload is not an object")
	}
	return nil
}

func validIdentity(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	return nil
}
