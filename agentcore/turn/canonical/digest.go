package canonical

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
)

// Envelope types of the bodies facts name by digest only. The
// frozen store (agentcore/turn/frozen) keeps each body as the typed envelope its
// digest was computed from, so it encodes and decodes under the same names.
const (
	RequestType      = "model_request"
	ModelResultType  = "model_result"
	ToolOutputType   = "tool_output"
	ToolResponseType = "tool_response_payload"
)

// preimageVersion is the version every digest preimage of this package
// carries in its domain separator. It is part of the persisted digests and
// never changes; a new digest rule would be a new domain, not a new number.
const preimageVersion uint16 = 1

// Digests is the digest rules of every body a fact names.
type Digests struct{}

func (Digests) DigestRequest(req model.ModelRequest) (turn.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, RequestType, req)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

func (Digests) DigestToolDefinition(def model.ToolDefinition) (turn.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

type toolResponseDecisionDigestBody struct {
	Kind     turn.ResponseKind     `json:"kind"`
	Decision turn.ResponseDecision `json:"decision"`
	Reason   string                `json:"reason,omitempty"`
}

type ToolResponsePayloadBody struct {
	Payload turn.CanonicalJSON `json:"payload"`
}

type ToolOutputBody struct {
	Output turn.CanonicalJSON `json:"output"`
}

func (Digests) DigestToolResponseDecision(kind turn.ResponseKind, decision turn.ResponseDecision, reason string) (turn.Digest, error) {
	if kind != turn.ResponseApproval && kind != turn.ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != turn.ResponseDecisionApproved && decision != turn.ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

func (Digests) DigestToolResponsePayload(payload turn.CanonicalJSON) (turn.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, ToolResponseType, ToolResponsePayloadBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

// DigestModelResult names a frozen model result; ModelStepCompleted carries
// this digest and the frozen store holds the body.
func (Digests) DigestModelResult(result model.ModelResult) (turn.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, ModelResultType, result)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

// DigestToolOutput names one tool output; ToolCallCompleted carries it.
func (Digests) DigestToolOutput(output turn.CanonicalJSON) (turn.Digest, error) {
	if output.IsZero() {
		return "", errors.New("agent: tool output: empty output")
	}
	body, err := jsonstable.EncodeTypedPayload(preimageVersion, ToolOutputType, ToolOutputBody{Output: output})
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}
