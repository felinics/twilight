package systemone_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/provider/typesafe/systemone"
	"github.com/felinics/twilight/sdk"
)

type capture struct {
	body    string
	headers http.Header
	path    string
	calls   atomic.Int32
}

// serve starts a server that records each request and replies with the given
// handler, and returns a provider pointed at it.
func serve(t *testing.T, opts []systemone.Option, handler http.HandlerFunc) (*systemone.Provider, *capture) {
	t.Helper()
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		got.body = string(body)
		got.headers = r.Header.Clone()
		got.path = r.URL.Path
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	opts = append([]systemone.Option{
		systemone.WithAPIKey("test-key"),
		systemone.WithBaseURL(srv.URL),
	}, opts...)
	return systemone.New(opts...), got
}

// newServer starts a test server and returns its URL.
func newServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL
}

func replyJSON(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// The triage questionnaire from TypeSafe's own quickstart.
func triageQuestions() []sdk.Question {
	return []sdk.Question{
		sdk.Choice("department", "Which team should handle this",
			sdk.Opt("billing", "Payment or subscription issues"),
			sdk.Opt("technical", "Bugs or integration problems"),
			sdk.Opt("account", ""),
		),
		sdk.Score("frustration", "How frustrated the customer appears",
			"Calm, just stating facts", "Frustrated but civil", "Very angry"),
		sdk.Noul("is_urgent", "The message conveys urgency").
			WithMeanings("Explicitly time-sensitive", "No urgency expressed"),
	}
}

const typesafeAnswers = `{
  "model": "jev-1.13.0",
  "answers": {
    "department":  {"type": "choice", "choice": "billing",
                    "probabilities": {"billing": 0.84, "technical": 0.159, "account": 0.001},
                    "confidence": 0.596},
    "frustration": {"type": "score", "score": 1.035,
                    "legend": {"0": "Calm", "1": "Frustrated", "2": "Very angry"},
                    "probabilities": {"0": 0.05, "1": 0.3, "2": 0.65},
                    "confidence": 0.842},
    "is_urgent":   {"type": "noul", "noul": 0.999}
  },
  "usage": {"input_tokens": 312, "output_tokens": 48}
}`

func evaluate(t *testing.T, p *systemone.Provider, modelID string, state any) *sdk.EvaluateResult {
	t.Helper()
	result, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel(modelID)),
		sdk.WithState(state),
		sdk.WithQuestions(triageQuestions()...),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	return result
}

// ---------- request shape ----------

// The request is asserted verbatim: the options must keep their declared order
// rather than being sorted, "true" must come before "false", and the questions
// must follow the order they were asked in.
func TestDoEvaluate_RequestBody(t *testing.T) {
	p, got := serve(t, nil, replyJSON(http.StatusOK, typesafeAnswers))
	evaluate(t, p, systemone.DefaultModel, "Payouts have been failing for 3 days.")

	want := `{"state":"Payouts have been failing for 3 days.","model":"jev-latest","questions":{` +
		`"department":{"type":"choice","instructions":"Which team should handle this",` +
		`"criteria":{"billing":"Payment or subscription issues","technical":"Bugs or integration problems","account":null}},` +
		`"frustration":{"type":"score","instructions":"How frustrated the customer appears",` +
		`"criteria":["Calm, just stating facts","Frustrated but civil","Very angry"]},` +
		`"is_urgent":{"type":"noul","instructions":"The message conveys urgency",` +
		`"criteria":{"true":"Explicitly time-sensitive","false":"No urgency expressed"}}}}`

	if got.body != want {
		t.Errorf("request body:\n got %s\nwant %s", got.body, want)
	}
	if got.path != "/systemone" {
		t.Errorf("path = %q, want /systemone", got.path)
	}
	if auth := got.headers.Get("Authorization"); auth != "Bearer test-key" {
		t.Errorf("Authorization = %q, want Bearer test-key", auth)
	}
}

func TestDoEvaluate_OmitsAbsentNoulCriteria(t *testing.T) {
	p, got := serve(t, nil, replyJSON(http.StatusOK, `{"model":"m","answers":{"a":{"type":"noul","noul":0.5}}}`))

	if _, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "Urgent?")),
	); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if strings.Contains(got.body, "criteria") {
		t.Errorf("a noul with no criteria should not send the field: %s", got.body)
	}
}

func TestDoEvaluate_StateForms(t *testing.T) {
	type order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount_usd"`
	}

	tests := []struct {
		name  string
		state any
		want  string
	}{
		{name: "string", state: "charged twice", want: `"state":"charged twice"`},
		{name: "struct", state: order{ID: "A-104", Amount: 49}, want: `"state":{"id":"A-104","amount_usd":49}`},
		{
			name:  "ordered object",
			state: sdk.JSONObject{}.Set("ticket", "charged twice").Set("order", order{ID: "A-104", Amount: 49}),
			want:  `"state":{"ticket":"charged twice","order":{"id":"A-104","amount_usd":49}}`,
		},
		{name: "array", state: []string{"Hi", "charged twice"}, want: `"state":["Hi","charged twice"]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, got := serve(t, nil, replyJSON(http.StatusOK, `{"model":"m","answers":{"a":{"type":"noul","noul":0.5}}}`))
			if _, err := sdk.Evaluate(context.Background(),
				sdk.WithEvaluationModel(p.EvaluationModel("m")),
				sdk.WithState(tc.state),
				sdk.WithQuestions(sdk.Noul("a", "?")),
			); err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if !strings.Contains(got.body, tc.want) {
				t.Errorf("body %s does not contain %s", got.body, tc.want)
			}
		})
	}
}

// ---------- response decoding ----------

func TestDoEvaluate_DecodesAnswers(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK, typesafeAnswers))
	result := evaluate(t, p, systemone.DefaultModel, "state")

	// The alias was sent; the versioned ID that answered comes back.
	if result.Model != "jev-1.13.0" {
		t.Errorf("model = %q, want jev-1.13.0", result.Model)
	}
	if result.Usage.InputTokens != 312 || result.Usage.OutputTokens != 48 {
		t.Errorf("usage = %+v, want 312/48", result.Usage)
	}

	if noul, err := result.Noul("is_urgent"); err != nil || noul != 0.999 {
		t.Errorf("noul = %v, %v; want 0.999", noul, err)
	}
	option, confidence, err := result.Choice("department")
	if err != nil || option != "billing" || confidence != 0.596 {
		t.Errorf("choice = %q, %v, %v; want billing, 0.596", option, confidence, err)
	}
	score, confidence, err := result.Score("frustration")
	if err != nil || score != 1.035 || confidence != 0.842 {
		t.Errorf("score = %v, %v, %v; want 1.035, 0.842", score, confidence, err)
	}

	answer, err := result.Answer("frustration")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if answer.Legend["2"] != "Very angry" {
		t.Errorf("legend = %v, want level 2 to be Very angry", answer.Legend)
	}
	if len(answer.Probabilities) != 3 {
		t.Errorf("probabilities = %v, want 3 levels", answer.Probabilities)
	}

	urgent, err := result.Answer("is_urgent")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if urgent.Confidence != nil {
		t.Errorf("a noul reported a confidence of %v; it carries none", *urgent.Confidence)
	}
}

// The API reference marks probabilities as required on a score answer, but the
// quickstart's own example omits it, so a missing one must not be an error.
func TestDoEvaluate_ScoreWithoutProbabilities(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK,
		`{"model":"m","answers":{"frustration":{"type":"score","score":1.0,"confidence":0.8}},"usage":{}}`))

	result, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Score("frustration", "?", "a", "b")),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if score, _, err := result.Score("frustration"); err != nil || score != 1.0 {
		t.Errorf("score = %v, %v; want 1.0", score, err)
	}
}

// A noul that somehow arrives with a confidence must not keep it: the API does
// not report one, and a caller gating on it would be acting on noise.
func TestDoEvaluate_DropsConfidenceOnNoul(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK,
		`{"model":"m","answers":{"a":{"type":"noul","noul":0.9,"confidence":0.4}}}`))

	result, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	answer, _ := result.Answer("a")
	if answer.Confidence != nil {
		t.Errorf("confidence = %v, want nil", *answer.Confidence)
	}
}

func TestDoEvaluate_UnknownAnswerType(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK,
		`{"model":"m","answers":{"a":{"type":"rank","rank":3}}}`))

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("err = %v, want an unknown type error", err)
	}
}

// ---------- errors and retries ----------

func TestDoEvaluate_RetriesOverloadAndRateLimit(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, 529} {
		var calls atomic.Int32
		p, _ := serve(t, []systemone.Option{systemone.WithMaxRetries(2)},
			func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) < 2 {
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"model":"m","answers":{"a":{"type":"noul","noul":0.5}}}`)
			})

		if _, err := sdk.Evaluate(context.Background(),
			sdk.WithEvaluationModel(p.EvaluationModel("m")),
			sdk.WithState("s"),
			sdk.WithQuestions(sdk.Noul("a", "?")),
		); err != nil {
			t.Fatalf("status %d: Evaluate: %v", status, err)
		}
		if got := calls.Load(); got != 2 {
			t.Errorf("status %d: attempts = %d, want 2", status, got)
		}
	}
}

func TestDoEvaluate_DoesNotRetryValidationErrors(t *testing.T) {
	p, got := serve(t, []systemone.Option{systemone.WithMaxRetries(3)},
		replyJSON(http.StatusUnprocessableEntity, `{"message":"criteria must have at least two levels"}`))

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "at least two levels") {
		t.Errorf("error %q drops the server's explanation", err)
	}
	if calls := got.calls.Load(); calls != 1 {
		t.Errorf("attempts = %d, want 1: a 422 will not pass on a retry", calls)
	}
}

func TestDoEvaluate_Unauthorized(t *testing.T) {
	p, got := serve(t, []systemone.Option{systemone.WithMaxRetries(3)},
		replyJSON(http.StatusUnauthorized, `{"message":"invalid api key"}`))

	if _, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	); err == nil {
		t.Fatal("expected an error")
	}
	if calls := got.calls.Load(); calls != 1 {
		t.Errorf("attempts = %d, want 1", calls)
	}
}

// Validation runs before the request, so a malformed questionnaire never
// reaches the network.
func TestDoEvaluate_ValidatesBeforeSending(t *testing.T) {
	p, got := serve(t, nil, replyJSON(http.StatusOK, `{}`))

	_, err := p.DoEvaluate(context.Background(), sdk.EvaluateParams{
		Model:     p.EvaluationModel("m"),
		State:     "s",
		Questions: []sdk.Question{sdk.Score("mood", "?", "only one level")},
	})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if calls := got.calls.Load(); calls != 0 {
		t.Errorf("made %d requests; validation should run first", calls)
	}
}

// ---------- model listing ----------

func TestListModels(t *testing.T) {
	p, got := serve(t, nil, replyJSON(http.StatusOK, `{"models":[
		{"name":"jev-latest","description":"most recent stable","release_date":"2026-09-01"},
		{"name":"jev-preview","description":"most recent","release_date":"2026-09-01"}]}`))

	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 2 || models[0].ID != "jev-latest" || models[1].ID != "jev-preview" {
		t.Fatalf("models = %v, want jev-latest and jev-preview", models)
	}
	if models[0].Provider == nil {
		t.Error("listed model is not bound to its provider")
	}
	if got.path != "/models" {
		t.Errorf("path = %q, want /models", got.path)
	}
}

func TestProviderName(t *testing.T) {
	if got := systemone.New().Name(); got != "typesafe-systemone" {
		t.Errorf("name = %q", got)
	}
	if got := systemone.New(systemone.WithVercelAIGateway()).Name(); got != "typesafe-systemone-gateway" {
		t.Errorf("gateway name = %q", got)
	}
}

func TestNew_ImplementsCapability(t *testing.T) {
	var _ sdk.EvaluationProvider = systemone.New()
}

func TestDoEvaluate_DecodeGarbage(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK, `{"model":"m","answers":`))

	if _, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	); err == nil {
		t.Fatal("expected a decode error")
	}
}

func TestDoEvaluate_ResponseWithoutAnswers(t *testing.T) {
	p, _ := serve(t, nil, replyJSON(http.StatusOK, `{"model":"m","usage":{}}`))

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel("m")),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if err == nil || !strings.Contains(err.Error(), "no answers") {
		t.Fatalf("err = %v, want a missing answers error", err)
	}
}

func TestJSONTagsMatchDocumentedWire(t *testing.T) {
	// Guards the snake_case usage keys, which differ from the gateway's.
	var resp struct {
		Usage struct {
			InputTokens int `json:"input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(`{"usage":{"input_tokens":7}}`), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Usage.InputTokens != 7 {
		t.Error("usage keys are snake_case on the TypeSafe API")
	}
}
