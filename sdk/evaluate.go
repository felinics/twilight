package sdk

import "context"

type evaluateConfig struct {
	Params EvaluateParams
}

// EvaluateOption configures an evaluation request.
type EvaluateOption func(*evaluateConfig)

// WithEvaluationModel selects the System One model that answers the questions.
func WithEvaluationModel(model *EvaluationModel) EvaluateOption {
	return func(c *evaluateConfig) { c.Params.Model = model }
}

// WithState sets the content to evaluate. It may be a string, or any value
// that marshals to a JSON object or array. Use a struct or a [JSONObject]
// rather than a map when the order of the fields matters.
func WithState(state any) EvaluateOption {
	return func(c *evaluateConfig) { c.Params.State = state }
}

// WithQuestions appends questions to the request. Questions are evaluated
// independently against the same state, so asking them together costs one
// round trip and one reading of the state.
func WithQuestions(questions ...Question) EvaluateOption {
	return func(c *evaluateConfig) {
		c.Params.Questions = append(c.Params.Questions, questions...)
	}
}

func buildEvaluateConfig(options []EvaluateOption) (*evaluateConfig, EvaluationProvider, error) {
	cfg := &evaluateConfig{}
	for _, opt := range options {
		opt(cfg)
	}
	if err := cfg.Params.Validate(); err != nil {
		return nil, nil, err
	}
	return cfg, cfg.Params.Model.Provider, nil
}

// Evaluate answers typed questions about a state with a System One model.
//
//	result, err := client.Evaluate(ctx,
//		sdk.WithEvaluationModel(model),
//		sdk.WithState(ticket),
//		sdk.WithQuestions(
//			sdk.Noul("is_urgent", "Does this convey urgency?"),
//			sdk.Choice("department", "Which team should handle this?",
//				sdk.Opt("billing", "Payments, invoicing, refunds"),
//				sdk.Opt("technical", "Bugs, outages, integrations"),
//			),
//			sdk.Score("frustration", "How frustrated is the customer?",
//				"Calm", "Frustrated", "Very angry"),
//		),
//	)
func (c *Client) Evaluate(ctx context.Context, options ...EvaluateOption) (*EvaluateResult, error) {
	cfg, prov, err := buildEvaluateConfig(options)
	if err != nil {
		return nil, err
	}
	return prov.DoEvaluate(ctx, cfg.Params)
}
