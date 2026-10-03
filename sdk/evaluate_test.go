package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/sdk"
)

type fakeEvaluationProvider struct {
	got    sdk.EvaluateParams
	result *sdk.EvaluateResult
	err    error
}

func (p *fakeEvaluationProvider) ListModels(context.Context) ([]*sdk.EvaluationModel, error) {
	return []*sdk.EvaluationModel{{ID: "fake-latest", Provider: p}}, nil
}

func (p *fakeEvaluationProvider) DoEvaluate(_ context.Context, params sdk.EvaluateParams) (*sdk.EvaluateResult, error) {
	p.got = params
	if p.err != nil {
		return nil, p.err
	}
	return p.result, nil
}

func confidenceOf(v float64) *float64 { return &v }

func fakeEvaluationModel(p *fakeEvaluationProvider) *sdk.EvaluationModel {
	return &sdk.EvaluationModel{ID: "fake-latest", Provider: p}
}

// ---------- ordering ----------

// The options are declared urgent, billing, account; sorting them would emit
// account, billing, urgent instead.
func TestChoiceOptions_MarshalPreservesDeclaredOrder(t *testing.T) {
	q := sdk.Choice("department", "Which team should handle this?",
		sdk.Opt("urgent", "Anything on fire"),
		sdk.Opt("billing", "Payments, invoicing, refunds"),
		sdk.Opt("account", ""),
	)

	got, err := json.Marshal(q.Criteria)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"urgent":"Anything on fire","billing":"Payments, invoicing, refunds","account":null}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestChoiceOptions_MarshalStructuredDescription(t *testing.T) {
	q := sdk.Choice("department", "Which team?",
		sdk.Opt("orders", sdk.JSONObject{}.
			Set("what", "Order status, delivery, returns").
			Set("not_for", "Account access")),
	)
	got, err := json.Marshal(q.Criteria)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"orders":{"what":"Order status, delivery, returns","not_for":"Account access"}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestChoiceOptions_MarshalNilDescription(t *testing.T) {
	got, err := json.Marshal(sdk.ChoiceOptions{sdk.Opt("a", nil)})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(got) != `{"a":null}` {
		t.Errorf("got %s, want {\"a\":null}", got)
	}
}

// A struct marshals in field-declaration order, so the criteria object keeps
// true before false; a map would sort them the other way round.
func TestNoulCriteria_MarshalKeepsTrueBeforeFalse(t *testing.T) {
	q := sdk.Noul("urgent", "Urgent?").WithMeanings("Explicitly time-sensitive", "No urgency expressed")

	got, err := json.Marshal(q.Criteria)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"true":"Explicitly time-sensitive","false":"No urgency expressed"}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestNoulCriteria_MarshalOmitsBlankMeanings(t *testing.T) {
	q := sdk.Noul("urgent", "Urgent?").WithMeanings("", "No urgency expressed")

	got, err := json.Marshal(q.Criteria)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"false":"No urgency expressed"}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// WithMeanings is a no-op on the kinds that have no yes/no criteria, so it
// cannot overwrite a Choice's options with a NoulCriteria.
func TestWithMeanings_IgnoredByOtherKinds(t *testing.T) {
	q := sdk.Choice("team", "Which team?", sdk.Opt("billing", "")).WithMeanings("a", "b")
	if _, ok := q.Criteria.(sdk.ChoiceOptions); !ok {
		t.Fatalf("criteria = %T, want sdk.ChoiceOptions", q.Criteria)
	}
	if err := q.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// ---------- validation ----------

func TestQuestion_Validate(t *testing.T) {
	tenLevels := make([]any, 0, sdk.MaxScoreLevels)
	for range sdk.MaxScoreLevels {
		tenLevels = append(tenLevels, "level")
	}
	elevenLevels := make([]any, 0, sdk.MaxScoreLevels+1)
	elevenLevels = append(elevenLevels, tenLevels...)
	elevenLevels = append(elevenLevels, "extra")
	tooManyOptions := make([]sdk.ChoiceOption, sdk.MaxChoiceOptions+1)
	for i := range tooManyOptions {
		tooManyOptions[i] = sdk.Opt(string(rune('a'+i%26))+string(rune('a'+i/26)), "")
	}

	tests := []struct {
		name    string
		q       sdk.Question
		wantErr string
	}{
		{name: "noul", q: sdk.Noul("urgent", "Does this convey urgency?")},
		{name: "noul with meanings", q: sdk.Noul("urgent", "Urgent?").WithMeanings("Time-sensitive", "Not urgent")},
		{name: "choice", q: sdk.Choice("team", "Which team?", sdk.Opt("billing", ""))},
		{name: "score two levels", q: sdk.Score("mood", "How calm?", "Calm", "Angry")},
		{name: "score ten levels", q: sdk.Score("mood", "How calm?", tenLevels...)},
		{
			name:    "no id",
			q:       sdk.Noul("", "Urgent?"),
			wantErr: "has no ID",
		},
		{
			name:    "no instructions",
			q:       sdk.Noul("urgent", nil),
			wantErr: "has no instructions",
		},
		{
			name:    "unknown kind",
			q:       sdk.Question{ID: "x", Kind: "rank", Instructions: "?"},
			wantErr: "unknown kind",
		},
		{
			name:    "choice with the wrong criteria type",
			q:       sdk.Question{ID: "team", Kind: sdk.QuestionKindChoice, Instructions: "?", Criteria: sdk.ScoreLevels{"a", "b"}},
			wantErr: "want ChoiceOptions",
		},
		{
			name:    "score with the wrong criteria type",
			q:       sdk.Question{ID: "mood", Kind: sdk.QuestionKindScore, Instructions: "?", Criteria: sdk.ChoiceOptions{sdk.Opt("a", "")}},
			wantErr: "want ScoreLevels",
		},
		{
			name:    "choice with no options",
			q:       sdk.Choice("team", "Which team?"),
			wantErr: "has no options",
		},
		{
			name:    "choice with unnamed option",
			q:       sdk.Choice("team", "Which team?", sdk.Opt("", "")),
			wantErr: "option with no name",
		},
		{
			name:    "choice with duplicate options",
			q:       sdk.Choice("team", "Which team?", sdk.Opt("billing", "a"), sdk.Opt("billing", "b")),
			wantErr: "duplicate option",
		},
		{
			name:    "choice with too many options",
			q:       sdk.Choice("team", "Which team?", tooManyOptions...),
			wantErr: "at most 255",
		},
		{
			name:    "score with one level",
			q:       sdk.Score("mood", "How calm?", "Calm"),
			wantErr: "want 2 to 10",
		},
		{
			name:    "score with eleven levels",
			q:       sdk.Score("mood", "How calm?", elevenLevels...),
			wantErr: "want 2 to 10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.q.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected an error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestEvaluate_ValidationErrors(t *testing.T) {
	prov := &fakeEvaluationProvider{result: &sdk.EvaluateResult{}}

	tests := []struct {
		name    string
		options []sdk.EvaluateOption
		wantErr string
	}{
		{
			name:    "no model",
			options: []sdk.EvaluateOption{sdk.WithState("s"), sdk.WithQuestions(sdk.Noul("a", "?"))},
			wantErr: "evaluation model is required",
		},
		{
			name:    "no provider",
			options: []sdk.EvaluateOption{sdk.WithEvaluationModel(&sdk.EvaluationModel{ID: "x"}), sdk.WithState("s"), sdk.WithQuestions(sdk.Noul("a", "?"))},
			wantErr: "has no provider",
		},
		{
			name:    "no state",
			options: []sdk.EvaluateOption{sdk.WithEvaluationModel(fakeEvaluationModel(prov)), sdk.WithQuestions(sdk.Noul("a", "?"))},
			wantErr: "state is required",
		},
		{
			name:    "no questions",
			options: []sdk.EvaluateOption{sdk.WithEvaluationModel(fakeEvaluationModel(prov)), sdk.WithState("s")},
			wantErr: "at least one question is required",
		},
		{
			name: "duplicate question IDs",
			options: []sdk.EvaluateOption{
				sdk.WithEvaluationModel(fakeEvaluationModel(prov)), sdk.WithState("s"),
				sdk.WithQuestions(sdk.Noul("a", "?"), sdk.Noul("a", "?")),
			},
			wantErr: "duplicate question ID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sdk.Evaluate(context.Background(), tc.options...)
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// ---------- request and answers ----------

func TestEvaluate_PassesParamsToProvider(t *testing.T) {
	prov := &fakeEvaluationProvider{result: &sdk.EvaluateResult{Model: "fake-1.0.0"}}
	state := sdk.JSONObject{}.Set("ticket", "payouts failing")

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(fakeEvaluationModel(prov)),
		sdk.WithState(state),
		sdk.WithQuestions(sdk.Noul("urgent", "Urgent?")),
		sdk.WithQuestions(sdk.Score("mood", "How calm?", "Calm", "Angry")),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if prov.got.Model == nil || prov.got.Model.ID != "fake-latest" {
		t.Errorf("model = %v, want fake-latest", prov.got.Model)
	}
	// WithQuestions appends, so two calls make two questions in order.
	if len(prov.got.Questions) != 2 {
		t.Fatalf("got %d questions, want 2", len(prov.got.Questions))
	}
	if prov.got.Questions[0].ID != "urgent" || prov.got.Questions[1].ID != "mood" {
		t.Errorf("question order = %q, %q; want urgent, mood",
			prov.got.Questions[0].ID, prov.got.Questions[1].ID)
	}
	if prov.got.State == nil {
		t.Error("state was not passed through")
	}
}

func TestEvaluate_ProviderError(t *testing.T) {
	sentinel := errors.New("boom")
	prov := &fakeEvaluationProvider{err: sentinel}

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(fakeEvaluationModel(prov)),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

func evaluatedResult(t *testing.T) *sdk.EvaluateResult {
	t.Helper()
	prov := &fakeEvaluationProvider{result: &sdk.EvaluateResult{
		Model: "fake-1.0.0",
		Answers: map[string]sdk.Answer{
			"is_urgent": {Kind: sdk.QuestionKindNoul, Noul: 0.999},
			"department": {
				Kind:          sdk.QuestionKindChoice,
				Choice:        "billing",
				Probabilities: map[string]float64{"billing": 0.84, "technical": 0.159, "sales": 0.001},
				Confidence:    confidenceOf(0.596),
			},
			"frustration": {
				Kind:          sdk.QuestionKindScore,
				Score:         1.035,
				Legend:        map[string]string{"0": "Calm", "1": "Frustrated", "2": "Very angry"},
				Probabilities: map[string]float64{"0": 0.05, "1": 0.3, "2": 0.65},
				Confidence:    confidenceOf(0.842),
			},
		},
		Usage: sdk.EvaluationUsage{InputTokens: 312, OutputTokens: 48},
	}}

	result, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(fakeEvaluationModel(prov)),
		sdk.WithState("Help! My payouts have been failing for 3 days."),
		sdk.WithQuestions(
			sdk.Noul("is_urgent", "Does this convey urgency?"),
			sdk.Choice("department", "Which team?", sdk.Opt("billing", ""), sdk.Opt("technical", ""), sdk.Opt("sales", "")),
			sdk.Score("frustration", "How frustrated?", "Calm", "Frustrated", "Very angry"),
		),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	return result
}

func TestEvaluateResult_TypedAccessors(t *testing.T) {
	result := evaluatedResult(t)

	noul, err := result.Noul("is_urgent")
	if err != nil {
		t.Fatalf("Noul: %v", err)
	}
	if noul != 0.999 {
		t.Errorf("noul = %v, want 0.999", noul)
	}

	option, confidence, err := result.Choice("department")
	if err != nil {
		t.Fatalf("Choice: %v", err)
	}
	if option != "billing" || confidence != 0.596 {
		t.Errorf("choice = %q, %v; want billing, 0.596", option, confidence)
	}

	score, confidence, err := result.Score("frustration")
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	if score != 1.035 || confidence != 0.842 {
		t.Errorf("score = %v, %v; want 1.035, 0.842", score, confidence)
	}

	if result.Model != "fake-1.0.0" {
		t.Errorf("model = %q, want fake-1.0.0", result.Model)
	}
	if result.Usage.InputTokens != 312 || result.Usage.OutputTokens != 48 {
		t.Errorf("usage = %+v, want 312/48", result.Usage)
	}
}

// A Noul answer carries no confidence, so the field stays nil rather than
// reading as a hard zero that a confidence gate would act on.
func TestEvaluateResult_NoulHasNoConfidence(t *testing.T) {
	result := evaluatedResult(t)

	answer, err := result.Answer("is_urgent")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if answer.Confidence != nil {
		t.Errorf("confidence = %v, want nil", *answer.Confidence)
	}

	choice, err := result.Answer("department")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if choice.Confidence == nil {
		t.Fatal("choice answer has no confidence")
	}
	if len(choice.Probabilities) != 3 {
		t.Errorf("probabilities = %v, want 3 entries", choice.Probabilities)
	}
}

func TestEvaluateResult_AccessorErrors(t *testing.T) {
	result := evaluatedResult(t)

	if _, err := result.Answer("nope"); err == nil {
		t.Error("expected an error for an unknown question ID")
	}
	if _, err := result.Noul("department"); err == nil {
		t.Error("expected an error reading a choice answer as a noul")
	} else if !strings.Contains(err.Error(), "is a choice, not a noul") {
		t.Errorf("unhelpful error: %v", err)
	}
	if _, _, err := result.Choice("frustration"); err == nil {
		t.Error("expected an error reading a score answer as a choice")
	}
	if _, _, err := result.Score("is_urgent"); err == nil {
		t.Error("expected an error reading a noul answer as a score")
	}
}

func TestEvaluationProvider_ListModels(t *testing.T) {
	prov := &fakeEvaluationProvider{}
	models, err := prov.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 1 || models[0].ID != "fake-latest" {
		t.Errorf("models = %v, want one fake-latest", models)
	}
	if models[0].Provider == nil {
		t.Error("listed model is not bound to its provider")
	}
}
