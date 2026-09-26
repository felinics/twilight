package turn

import (
	"crypto/rand"
	"encoding/hex"
)

// NewTurnID mints a collision-free TurnID.
func NewTurnID() TurnID {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("turn: rand: " + err.Error())
	}
	return TurnID("turn-" + hex.EncodeToString(b))
}
