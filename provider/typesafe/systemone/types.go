package systemone

import (
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/sdk"
)

// wire is one API's dialect: where to post, which protocol headers it wants,
// and how it encodes a request and decodes a response. Everything else, the
// HTTP client, retries and error mapping, is shared by the Provider.
type wire interface {
	defaultBaseURL() string
	evaluatePath() string
	// headers returns the dialect's protocol headers for a request against the
	// given model. The gateway carries the model here rather than in the body.
	headers(modelID string) map[string]string
	encodeRequest(params sdk.EvaluateParams) (any, error)
	decodeResponse(body []byte, modelID string) (*sdk.EvaluateResult, error)
	listsModels() bool
}

// wireQuestion is one question as both dialects send it. Only the value of
// type differs: the gateway calls a noul a boolean.
type wireQuestion struct {
	Type         string `json:"type"`
	Instructions any    `json:"instructions"`
	Criteria     any    `json:"criteria,omitempty"`
}

// encodeQuestions renders the questions as a JSON object keyed by question ID.
// It uses sdk.JSONObject so the object follows the order the questions were
// declared in rather than a sort of their IDs.
func encodeQuestions(questions []sdk.Question, kindOf func(sdk.QuestionKind) string) sdk.JSONObject {
	out := make(sdk.JSONObject, 0, len(questions))
	for i := range questions {
		q := &questions[i]
		out = out.Set(q.ID, wireQuestion{
			Type:         kindOf(q.Kind),
			Instructions: q.Instructions,
			Criteria:     q.Criteria,
		})
	}
	return out
}

// ---------- TypeSafe's own dialect: POST /v1/systemone ----------

type typesafeWire struct{}

func (typesafeWire) defaultBaseURL() string           { return DefaultBaseURL }
func (typesafeWire) evaluatePath() string             { return "/systemone" }
func (typesafeWire) headers(string) map[string]string { return nil }
func (typesafeWire) listsModels() bool                { return true }

type typesafeRequest struct {
	State     any            `json:"state"`
	Model     string         `json:"model"`
	Questions sdk.JSONObject `json:"questions"`
}

func (typesafeWire) encodeRequest(params sdk.EvaluateParams) (any, error) {
	return typesafeRequest{
		State:     params.State,
		Model:     params.Model.ID,
		Questions: encodeQuestions(params.Questions, func(k sdk.QuestionKind) string { return string(k) }),
	}, nil
}

type typesafeAnswer struct {
	Type          string             `json:"type"`
	Noul          float64            `json:"noul"`
	Choice        string             `json:"choice"`
	Score         float64            `json:"score"`
	Legend        map[string]string  `json:"legend"`
	Probabilities map[string]float64 `json:"probabilities"`
	Confidence    *float64           `json:"confidence"`
}

type typesafeResponse struct {
	Model   string                    `json:"model"`
	Answers map[string]typesafeAnswer `json:"answers"`
	Usage   struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

func (typesafeWire) decodeResponse(body []byte, modelID string) (*sdk.EvaluateResult, error) {
	var resp typesafeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("typesafe: decode response: %w", err)
	}
	if resp.Answers == nil {
		return nil, fmt.Errorf("typesafe: response has no answers")
	}

	answers := make(map[string]sdk.Answer, len(resp.Answers))
	for id, a := range resp.Answers {
		answer := sdk.Answer{
			Kind:          sdk.QuestionKind(a.Type),
			Probabilities: a.Probabilities,
			Confidence:    a.Confidence,
		}
		switch answer.Kind {
		case sdk.QuestionKindNoul:
			answer.Noul = a.Noul
			// A noul carries no confidence; drop anything that arrived anyway so
			// the zero value cannot be mistaken for a reported one.
			answer.Confidence = nil
		case sdk.QuestionKindChoice:
			answer.Choice = a.Choice
		case sdk.QuestionKindScore:
			answer.Score = a.Score
			answer.Legend = a.Legend
		default:
			return nil, fmt.Errorf("typesafe: answer for question %q has unknown type %q", id, a.Type)
		}
		answers[id] = answer
	}

	model := resp.Model
	if model == "" {
		model = modelID
	}
	return &sdk.EvaluateResult{
		Model:   model,
		Answers: answers,
		Usage: sdk.EvaluationUsage{
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
		},
	}, nil
}

type typesafeModelList struct {
	Models []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		ReleaseDate string `json:"release_date"`
	} `json:"models"`
}
