package sdk

import (
	"context"
	"fmt"
)

// Documented limits of the question types. They are enforced client-side so a
// malformed questionnaire fails before the round trip rather than as a 422.
const (
	MaxChoiceOptions = 255
	MinScoreLevels   = 2
	MaxScoreLevels   = 10
)

// EvaluationProvider is the interface that System One backends must implement.
//
// A System One model evaluates a state against typed questions and returns one
// typed answer per question. It does not generate text, so it is a capability
// of its own rather than a [Provider]: there are no messages, no tools and no
// streaming.
type EvaluationProvider interface {
	ListModels(ctx context.Context) ([]*EvaluationModel, error)
	DoEvaluate(ctx context.Context, params EvaluateParams) (*EvaluateResult, error)
}

// EvaluationModel represents a System One model bound to an EvaluationProvider.
type EvaluationModel struct {
	ID       string
	Provider EvaluationProvider
}

// EvaluateParams holds the parameters for an evaluation request.
type EvaluateParams struct {
	Model *EvaluationModel
	// State is the content to evaluate. It may be a string, or any value that
	// marshals to a JSON object or array. Use a struct or a [JSONObject] rather
	// than a map when the order of the fields matters.
	State any
	// Questions are evaluated independently against the same State. One answer
	// comes back per question, keyed by the question's ID.
	Questions []Question
}

// QuestionKind identifies one of the three System One question types.
type QuestionKind string

const (
	QuestionKindNoul   QuestionKind = "noul"
	QuestionKindChoice QuestionKind = "choice"
	QuestionKindScore  QuestionKind = "score"
)

// Question is one typed question about a state. Build one with [Noul],
// [Choice] or [Score].
type Question struct {
	// ID names the question. The answer comes back under the same ID. It is not
	// sent to the model and takes no part in the evaluation.
	ID string
	// Kind selects which of the three question types this is, and therefore what
	// Criteria holds.
	Kind QuestionKind
	// Instructions is what the model should decide, rate or answer. A string, or
	// any value that marshals to a JSON object or array.
	Instructions any
	// Criteria is the answer space, whose shape follows Kind: a [NoulCriteria]
	// pointer for a Noul, [ChoiceOptions] for a Choice, [ScoreLevels] for a
	// Score. It may be nil for a Noul, which is the only kind whose criteria are
	// optional.
	Criteria any
}

// NoulCriteria describes what a yes and a no mean for a Noul question. Both
// fields are optional and may be any value that marshals to JSON.
type NoulCriteria struct {
	True  any `json:"true,omitempty"`
	False any `json:"false,omitempty"`
}

// ChoiceOption is one option of a [Choice] question. Description is optional
// and may be any value that marshals to JSON; an empty or nil description is
// sent as null.
type ChoiceOption struct {
	Name        string
	Description any
}

// ChoiceOptions is the ordered set of options of a Choice question. It marshals
// to a JSON object whose keys stay in slice order, because the order the
// options are written in is the order the model is shown them.
type ChoiceOptions []ChoiceOption

// MarshalJSON implements json.Marshaler.
func (o ChoiceOptions) MarshalJSON() ([]byte, error) {
	obj := make(JSONObject, len(o))
	for i, opt := range o {
		description := opt.Description
		if s, ok := description.(string); ok && s == "" {
			description = nil
		}
		obj[i] = JSONMember{Key: opt.Name, Value: description}
	}
	return obj.MarshalJSON()
}

// ScoreLevels is the ordered rubric of a Score question, from the low end of
// the scale to the high end. A level's index is its number.
type ScoreLevels []any

// Opt builds a [ChoiceOption]. Pass an empty description for an option that
// needs no extra detail.
func Opt(name string, description any) ChoiceOption {
	return ChoiceOption{Name: name, Description: description}
}

// Noul builds a yes/no question. The answer is the probability that the answer
// is yes, from 0 to 1.
func Noul(id string, instructions any) Question {
	return Question{ID: id, Kind: QuestionKindNoul, Instructions: instructions}
}

// WithMeanings describes what a yes and a no mean. It applies to Noul questions
// and is ignored by the other kinds.
func (q Question) WithMeanings(trueMeaning, falseMeaning any) Question {
	if q.Kind == QuestionKindNoul {
		q.Criteria = &NoulCriteria{True: blankToNil(trueMeaning), False: blankToNil(falseMeaning)}
	}
	return q
}

// Choice builds a question that picks one of the given options. The answer
// carries the selected option, a probability for every option, and a confidence.
func Choice(id string, instructions any, options ...ChoiceOption) Question {
	return Question{ID: id, Kind: QuestionKindChoice, Instructions: instructions, Criteria: ChoiceOptions(options)}
}

// Score builds a question that rates the state against ordered levels, from the
// low end of the scale to the high end. The answer is a probability weighted
// value that can land between levels.
func Score(id string, instructions any, levels ...any) Question {
	return Question{ID: id, Kind: QuestionKindScore, Instructions: instructions, Criteria: ScoreLevels(levels)}
}

func blankToNil(value any) any {
	if s, ok := value.(string); ok && s == "" {
		return nil
	}
	return value
}

// Validate reports whether the question is well formed.
func (q *Question) Validate() error {
	if q.ID == "" {
		return fmt.Errorf("twilightai: question has no ID")
	}
	if q.Instructions == nil {
		return fmt.Errorf("twilightai: question %q has no instructions", q.ID)
	}

	switch q.Kind {
	case QuestionKindNoul:
		if q.Criteria == nil {
			return nil
		}
		if _, ok := q.Criteria.(*NoulCriteria); !ok {
			return fmt.Errorf("twilightai: noul question %q has %T criteria, want *NoulCriteria", q.ID, q.Criteria)
		}
		return nil
	case QuestionKindChoice:
		options, ok := q.Criteria.(ChoiceOptions)
		if !ok {
			return fmt.Errorf("twilightai: choice question %q has %T criteria, want ChoiceOptions", q.ID, q.Criteria)
		}
		return validateChoiceOptions(q.ID, options)
	case QuestionKindScore:
		levels, ok := q.Criteria.(ScoreLevels)
		if !ok {
			return fmt.Errorf("twilightai: score question %q has %T criteria, want ScoreLevels", q.ID, q.Criteria)
		}
		if len(levels) < MinScoreLevels || len(levels) > MaxScoreLevels {
			return fmt.Errorf("twilightai: score question %q has %d levels, want %d to %d",
				q.ID, len(levels), MinScoreLevels, MaxScoreLevels)
		}
		return nil
	default:
		return fmt.Errorf("twilightai: question %q has unknown kind %q", q.ID, q.Kind)
	}
}

func validateChoiceOptions(id string, options ChoiceOptions) error {
	if len(options) == 0 {
		return fmt.Errorf("twilightai: choice question %q has no options", id)
	}
	if len(options) > MaxChoiceOptions {
		return fmt.Errorf("twilightai: choice question %q has %d options, want at most %d",
			id, len(options), MaxChoiceOptions)
	}
	seen := make(map[string]struct{}, len(options))
	for _, opt := range options {
		if opt.Name == "" {
			return fmt.Errorf("twilightai: choice question %q has an option with no name", id)
		}
		if _, dup := seen[opt.Name]; dup {
			return fmt.Errorf("twilightai: choice question %q has duplicate option %q", id, opt.Name)
		}
		seen[opt.Name] = struct{}{}
	}
	return nil
}

// Validate reports whether the parameters are well formed. Providers may call
// it to reject a request before sending it.
func (p EvaluateParams) Validate() error {
	if p.Model == nil {
		return fmt.Errorf("twilightai: evaluation model is required (use WithEvaluationModel)")
	}
	if p.Model.Provider == nil {
		return fmt.Errorf("twilightai: evaluation model %q has no provider", p.Model.ID)
	}
	if p.State == nil {
		return fmt.Errorf("twilightai: state is required (use WithState)")
	}
	if len(p.Questions) == 0 {
		return fmt.Errorf("twilightai: at least one question is required (use WithQuestions)")
	}

	seen := make(map[string]struct{}, len(p.Questions))
	for i := range p.Questions {
		q := &p.Questions[i]
		if err := q.Validate(); err != nil {
			return err
		}
		if _, dup := seen[q.ID]; dup {
			return fmt.Errorf("twilightai: duplicate question ID %q", q.ID)
		}
		seen[q.ID] = struct{}{}
	}
	return nil
}

// EvaluateResult holds the result of an evaluation request.
type EvaluateResult struct {
	// Model is the versioned ID of the model that answered, which may differ
	// from the alias that was requested.
	Model   string
	Answers map[string]Answer
	Usage   EvaluationUsage
}

// EvaluationUsage tracks token usage for evaluation requests.
type EvaluationUsage struct {
	InputTokens  int
	OutputTokens int
}

// Answer is the answer to one [Question]. Kind says which fields are set.
type Answer struct {
	Kind QuestionKind

	// Noul is the yes/no answer, from 0 (no) to 1 (yes). Noul only.
	Noul float64

	// Choice is the highest-probability option. Choice only.
	Choice string

	// Score is the probability weighted value across the levels, and Legend maps
	// each level number back to its description. Score only.
	Score  float64
	Legend map[string]string

	// Probabilities maps each option, or each level number, to its probability.
	// Choice and Score only.
	Probabilities map[string]float64

	// Confidence collapses Probabilities into a single number from 0 to 1. It is
	// nil for a Noul answer, which carries no confidence.
	Confidence *float64
}

// Answer returns the answer to the question with the given ID.
func (r *EvaluateResult) Answer(id string) (Answer, error) {
	answer, ok := r.Answers[id]
	if !ok {
		return Answer{}, fmt.Errorf("twilightai: no answer for question %q", id)
	}
	return answer, nil
}

// Noul returns the probability that the answer to the given Noul question is
// yes, from 0 to 1.
func (r *EvaluateResult) Noul(id string) (float64, error) {
	answer, err := r.answerOfKind(id, QuestionKindNoul)
	if err != nil {
		return 0, err
	}
	return answer.Noul, nil
}

// Choice returns the selected option and the confidence of the given Choice
// question. Read [EvaluateResult.Answer] for the full distribution.
func (r *EvaluateResult) Choice(id string) (option string, confidence float64, err error) {
	answer, err := r.answerOfKind(id, QuestionKindChoice)
	if err != nil {
		return "", 0, err
	}
	return answer.Choice, answer.confidence(), nil
}

// Score returns the value and the confidence of the given Score question. Read
// [EvaluateResult.Answer] for the legend and the full distribution.
func (r *EvaluateResult) Score(id string) (value, confidence float64, err error) {
	answer, err := r.answerOfKind(id, QuestionKindScore)
	if err != nil {
		return 0, 0, err
	}
	return answer.Score, answer.confidence(), nil
}

func (r *EvaluateResult) answerOfKind(id string, kind QuestionKind) (Answer, error) {
	answer, err := r.Answer(id)
	if err != nil {
		return Answer{}, err
	}
	if answer.Kind != kind {
		return Answer{}, fmt.Errorf("twilightai: answer for question %q is a %s, not a %s", id, answer.Kind, kind)
	}
	return answer, nil
}

func (a Answer) confidence() float64 {
	if a.Confidence == nil {
		return 0
	}
	return *a.Confidence
}
