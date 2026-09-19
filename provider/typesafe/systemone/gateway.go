package systemone

import (
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/sdk"
)

// Vercel AI Gateway serves Jev as "typesafe-ai/jev", in the AI SDK's
// evaluation-model dialect rather than TypeSafe's own. The questions and
// answers are the same; the wire format is not.
const (
	// GatewayBaseURL is the gateway's AI SDK protocol root. Evaluation is posted
	// to <base>/evaluation-model.
	GatewayBaseURL = "https://ai-gateway.vercel.sh/v4/ai"
	// GatewayModel is Jev's model ID on the gateway.
	GatewayModel = "typesafe-ai/jev"
	// GatewayAPIKeyEnv names the environment variable the AI SDK reads the
	// gateway credential from. This package never reads it; it is here so
	// callers and tests agree on the name.
	GatewayAPIKeyEnv = "AI_GATEWAY_API_KEY"

	// Protocol headers the gateway expects, as sent by @ai-sdk/gateway 4.0.
	gatewayProtocolHeader  = "Ai-Gateway-Protocol-Version"
	gatewayProtocolVersion = "0.0.1"
	gatewayAuthHeader      = "Ai-Gateway-Auth-Method"
	gatewayAuthMethod      = "api-key"
	gatewaySpecHeader      = "Ai-Evaluation-Model-Specification-Version"
	gatewaySpecVersion     = "4"
	gatewayModelHeader     = "Ai-Model-Id"

	// gatewayBooleanKind is what the gateway calls a noul.
	gatewayBooleanKind = "boolean"
)

type gatewayWire struct{}

func (gatewayWire) defaultBaseURL() string { return GatewayBaseURL }
func (gatewayWire) evaluatePath() string   { return "/evaluation-model" }

// listsModels is false: the gateway has no evaluation model listing.
func (gatewayWire) listsModels() bool { return false }

func (gatewayWire) headers(modelID string) map[string]string {
	return map[string]string{
		gatewayProtocolHeader: gatewayProtocolVersion,
		gatewayAuthHeader:     gatewayAuthMethod,
		gatewaySpecHeader:     gatewaySpecVersion,
		gatewayModelHeader:    modelID,
	}
}

// gatewayRequest carries no model: the gateway reads it from a header.
type gatewayRequest struct {
	State     any            `json:"state"`
	Questions sdk.JSONObject `json:"questions"`
}

func (gatewayWire) encodeRequest(params sdk.EvaluateParams) (any, error) {
	return gatewayRequest{
		State:     params.State,
		Questions: encodeQuestions(params.Questions, gatewayKind),
	}, nil
}

func gatewayKind(kind sdk.QuestionKind) string {
	if kind == sdk.QuestionKindNoul {
		return gatewayBooleanKind
	}
	return string(kind)
}

type gatewayAnswer struct {
	Type          string             `json:"type"`
	Probability   *float64           `json:"probability"`
	Choice        string             `json:"choice"`
	Score         float64            `json:"score"`
	Probabilities map[string]float64 `json:"probabilities"`
}

type gatewayResponse struct {
	Answers map[string]gatewayAnswer `json:"answers"`
	Usage   *struct {
		InputTokens  int `json:"inputTokens"`
		OutputTokens int `json:"outputTokens"`
	} `json:"usage"`
	ProviderMetadata map[string]map[string]json.RawMessage `json:"providerMetadata"`
}

func (gatewayWire) decodeResponse(body []byte, modelID string) (*sdk.EvaluateResult, error) {
	var resp gatewayResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("typesafe: decode gateway response: %w", err)
	}
	if resp.Answers == nil {
		return nil, fmt.Errorf("typesafe: gateway response has no answers")
	}

	// The gateway has no confidence field of its own; TypeSafe's rides along in
	// provider metadata, keyed by question ID.
	confidence := map[string]float64{}
	if raw, ok := resp.ProviderMetadata["typesafe"]["confidence"]; ok {
		if err := json.Unmarshal(raw, &confidence); err != nil {
			return nil, fmt.Errorf("typesafe: decode gateway providerMetadata.typesafe.confidence: %w", err)
		}
	}

	answers := make(map[string]sdk.Answer, len(resp.Answers))
	for id, a := range resp.Answers {
		answer := sdk.Answer{Probabilities: a.Probabilities}
		switch a.Type {
		case gatewayBooleanKind:
			answer.Kind = sdk.QuestionKindNoul
			if a.Probability != nil {
				answer.Noul = *a.Probability
			}
			// A noul carries no confidence, so it is not read from the metadata.
			answers[id] = answer
			continue
		case string(sdk.QuestionKindChoice):
			answer.Kind = sdk.QuestionKindChoice
			answer.Choice = a.Choice
		case string(sdk.QuestionKindScore):
			answer.Kind = sdk.QuestionKindScore
			answer.Score = a.Score
			// The gateway does not echo the rubric back, so Legend stays nil here
			// while the TypeSafe API fills it in.
		default:
			return nil, fmt.Errorf("typesafe: gateway answer for question %q has unknown type %q", id, a.Type)
		}
		if c, ok := confidence[id]; ok {
			answer.Confidence = &c
		}
		answers[id] = answer
	}

	result := &sdk.EvaluateResult{Model: modelID, Answers: answers}
	if resp.Usage != nil {
		result.Usage = sdk.EvaluationUsage{
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
		}
	}
	return result, nil
}
