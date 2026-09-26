package canonical

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
)

// Envelope types of the bodies facts name by digest only (RUN-WIR-4). The
// frozen store (agent/run/frozen) keeps each body as the typed envelope its
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

func (Digests) DigestRequest(req model.ModelRequest) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	body, err := es.EncodeTypedPayload(preimageVersion, RequestType, req)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolDefinition(def model.ToolDefinition) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

type toolResponseDecisionDigestBody struct {
	Kind     run.ResponseKind     `json:"kind"`
	Decision run.ResponseDecision `json:"decision"`
	Reason   string               `json:"reason,omitempty"`
}

type ToolResponsePayloadBody struct {
	Payload run.CanonicalJSON `json:"payload"`
}

type ToolOutputBody struct {
	Output run.CanonicalJSON `json:"output"`
}

func (Digests) DigestToolResponseDecision(kind run.ResponseKind, decision run.ResponseDecision, reason string) (run.Digest, error) {
	if kind != run.ResponseApproval && kind != run.ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != run.ResponseDecisionApproved && decision != run.ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolResponsePayload(payload run.CanonicalJSON) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(preimageVersion, ToolResponseType, ToolResponsePayloadBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestModelResult names a frozen model result; ModelStepCompleted carries
// this digest and the frozen store holds the body (RUN-WIR-4).
func (Digests) DigestModelResult(result model.ModelResult) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	body, err := es.EncodeTypedPayload(preimageVersion, ModelResultType, result)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestToolOutput names one tool output; ToolCallCompleted carries it.
func (Digests) DigestToolOutput(output run.CanonicalJSON) (run.Digest, error) {
	if output.IsZero() {
		return "", errors.New("agent: tool output: empty output")
	}
	body, err := es.EncodeTypedPayload(preimageVersion, ToolOutputType, ToolOutputBody{Output: output})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}
