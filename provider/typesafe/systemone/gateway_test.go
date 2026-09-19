package systemone_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/felinics/twilight/provider/typesafe/systemone"
	"github.com/felinics/twilight/sdk"
)

func gatewayProvider(t *testing.T, handler http.HandlerFunc) (*systemone.Provider, *capture) {
	t.Helper()
	return serve(t, []systemone.Option{systemone.WithVercelAIGateway()}, handler)
}

const gatewayAnswers = `{
  "answers": {
    "department":  {"type": "choice", "choice": "billing",
                    "probabilities": {"billing": 0.84, "technical": 0.159, "account": 0.001}},
    "frustration": {"type": "score", "score": 1.035,
                    "probabilities": {"0": 0.05, "1": 0.3, "2": 0.65}},
    "is_urgent":   {"type": "boolean", "probability": 0.999}
  },
  "usage": {"inputTokens": 312, "outputTokens": 48},
  "providerMetadata": {"typesafe": {"confidence": {"department": 0.596, "frustration": 0.842}}}
}`

// The gateway takes the model in a header, calls a noul a boolean, and sends
// no model field in the body. Option order is still the caller's.
func TestGateway_RequestBodyAndHeaders(t *testing.T) {
	p, got := gatewayProvider(t, replyJSON(http.StatusOK, gatewayAnswers))
	evaluate(t, p, systemone.GatewayModel, "Payouts have been failing for 3 days.")

	want := `{"state":"Payouts have been failing for 3 days.","questions":{` +
		`"department":{"type":"choice","instructions":"Which team should handle this",` +
		`"criteria":{"billing":"Payment or subscription issues","technical":"Bugs or integration problems","account":null}},` +
		`"frustration":{"type":"score","instructions":"How frustrated the customer appears",` +
		`"criteria":["Calm, just stating facts","Frustrated but civil","Very angry"]},` +
		`"is_urgent":{"type":"boolean","instructions":"The message conveys urgency",` +
		`"criteria":{"true":"Explicitly time-sensitive","false":"No urgency expressed"}}}}`

	if got.body != want {
		t.Errorf("request body:\n got %s\nwant %s", got.body, want)
	}
	if strings.Contains(got.body, `"model"`) {
		t.Error("the gateway takes the model in a header, not the body")
	}
	if got.path != "/evaluation-model" {
		t.Errorf("path = %q, want /evaluation-model", got.path)
	}

	for header, want := range map[string]string{
		"Ai-Model-Id":                               systemone.GatewayModel,
		"Ai-Gateway-Protocol-Version":               "0.0.1",
		"Ai-Gateway-Auth-Method":                    "api-key",
		"Ai-Evaluation-Model-Specification-Version": "4",
		"Authorization":                             "Bearer test-key",
	} {
		if value := got.headers.Get(header); value != want {
			t.Errorf("%s = %q, want %q", header, value, want)
		}
	}
}

func TestGateway_DecodesAnswers(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK, gatewayAnswers))
	result := evaluate(t, p, systemone.GatewayModel, "state")

	// The gateway does not echo the model, so the requested ID is reported.
	if result.Model != systemone.GatewayModel {
		t.Errorf("model = %q, want %q", result.Model, systemone.GatewayModel)
	}
	if result.Usage.InputTokens != 312 || result.Usage.OutputTokens != 48 {
		t.Errorf("usage = %+v, want 312/48 from the camelCase keys", result.Usage)
	}

	// A boolean answer carries its value in probability, not noul.
	if noul, err := result.Noul("is_urgent"); err != nil || noul != 0.999 {
		t.Errorf("noul = %v, %v; want 0.999", noul, err)
	}

	// Confidence rides in providerMetadata rather than on the answer.
	option, confidence, err := result.Choice("department")
	if err != nil || option != "billing" || confidence != 0.596 {
		t.Errorf("choice = %q, %v, %v; want billing, 0.596", option, confidence, err)
	}
	score, confidence, err := result.Score("frustration")
	if err != nil || score != 1.035 || confidence != 0.842 {
		t.Errorf("score = %v, %v, %v; want 1.035, 0.842", score, confidence, err)
	}
}

func TestGateway_NoulHasNoConfidence(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK, gatewayAnswers))
	result := evaluate(t, p, systemone.GatewayModel, "state")

	answer, err := result.Answer("is_urgent")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if answer.Confidence != nil {
		t.Errorf("confidence = %v, want nil", *answer.Confidence)
	}
}

// The gateway does not echo the rubric, so a score answer has no legend. The
// TypeSafe API does return one; this pins the difference.
func TestGateway_ScoreHasNoLegend(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK, gatewayAnswers))
	result := evaluate(t, p, systemone.GatewayModel, "state")

	answer, err := result.Answer("frustration")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if answer.Legend != nil {
		t.Errorf("legend = %v, want nil through the gateway", answer.Legend)
	}
	if len(answer.Probabilities) != 3 {
		t.Errorf("probabilities = %v, want the 3 levels", answer.Probabilities)
	}
}

func TestGateway_MissingConfidenceMetadata(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK,
		`{"answers":{"a":{"type":"choice","choice":"x","probabilities":{"x":1}}}}`))

	result, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel(systemone.GatewayModel)),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Choice("a", "?", sdk.Opt("x", ""))),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	answer, _ := result.Answer("a")
	if answer.Confidence != nil {
		t.Errorf("confidence = %v, want nil when the metadata is absent", *answer.Confidence)
	}
}

func TestGateway_UnknownAnswerType(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK, `{"answers":{"a":{"type":"rank"}}}`))

	_, err := sdk.Evaluate(context.Background(),
		sdk.WithEvaluationModel(p.EvaluationModel(systemone.GatewayModel)),
		sdk.WithState("s"),
		sdk.WithQuestions(sdk.Noul("a", "?")),
	)
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("err = %v, want an unknown type error", err)
	}
}

func TestGateway_DoesNotListModels(t *testing.T) {
	p, _ := gatewayProvider(t, replyJSON(http.StatusOK, `{}`))

	_, err := p.ListModels(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not list models") {
		t.Fatalf("err = %v, want a not-supported error", err)
	}
}

// WithBaseURL must win over the gateway's default whichever order they are
// given in, or a test server would be bypassed depending on option order.
func TestGateway_ExplicitBaseURLWinsInEitherOrder(t *testing.T) {
	for _, name := range []string{"gateway first", "base URL first"} {
		t.Run(name, func(t *testing.T) {
			var reached bool
			srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
				reached = true
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"answers":{"a":{"type":"boolean","probability":0.5}}}`)
			})

			opts := make([]systemone.Option, 0, 3)
			if name == "base URL first" {
				opts = append(opts, systemone.WithBaseURL(srv), systemone.WithVercelAIGateway())
			} else {
				opts = append(opts, systemone.WithVercelAIGateway(), systemone.WithBaseURL(srv))
			}
			opts = append(opts, systemone.WithAPIKey("k"))

			p := systemone.New(opts...)
			if _, err := sdk.Evaluate(context.Background(),
				sdk.WithEvaluationModel(p.EvaluationModel(systemone.GatewayModel)),
				sdk.WithState("s"),
				sdk.WithQuestions(sdk.Noul("a", "?")),
			); err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if !reached {
				t.Error("the request did not reach the configured base URL")
			}
		})
	}
}
