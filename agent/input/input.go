// Package input is the reference agent's user input content shape
// (DEC-INP-1): the canonical JSON a chatlog Input carries and the Run's
// AgentInput payload names by digest. The kernel stores and digests the
// content as an opaque value; what is inside it is this agent's decision.
package input

import (
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
)

// Text builds the v1 user input body: {"text": "<user string>"}.
func Text(text string) run.CanonicalJSON {
	raw, err := json.Marshal(text)
	if err != nil {
		panic(err) // a string always marshals
	}
	return run.MustParseCanonicalJSON(`{"text":` + string(raw) + `}`)
}

// TextOf is the inverse of Text: the user text of a v1 input body.
func TextOf(content run.CanonicalJSON) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := content.Decode(&body); err != nil {
		return "", fmt.Errorf("input: payload: %w", err)
	}
	return body.Text, nil
}
