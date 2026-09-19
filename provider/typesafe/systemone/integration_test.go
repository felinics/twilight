package systemone_test

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/internal/testutil"
	"github.com/felinics/twilight/provider/typesafe/systemone"
	"github.com/felinics/twilight/sdk"
)

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	os.Exit(m.Run())
}

// gatewayKey returns the Vercel AI Gateway credential.
//
// AI_GATEWAY_API_KEY is the name the AI SDK uses and the one to prefer. A
// gateway key also authenticates the OpenAI-compatible endpoint, so a project
// that already routes OPENAI_API_KEY through the gateway has one under that
// name; it is only used here when it carries the gateway's own "vck_" prefix,
// so a real OpenAI key is never sent to Vercel.
func gatewayKey() string {
	if key := strings.TrimSpace(os.Getenv("AI_GATEWAY_API_KEY")); key != "" {
		return key
	}
	if key := strings.TrimSpace(os.Getenv("OPENAI_API_KEY")); strings.HasPrefix(key, "vck_") {
		return key
	}
	return ""
}

func skipWithoutKey(t *testing.T, key, name string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if key == "" {
		t.Skipf("skipping: %s not set", name)
	}
}

// checkTriage asserts the properties every System One answer must have,
// whatever the model decides: probabilities that form a distribution, a choice
// drawn from the declared options, a score inside the rubric, and confidence in
// range. The values themselves are the model's to pick.
func checkTriage(t *testing.T, result *sdk.EvaluateResult) {
	t.Helper()
	t.Logf("model=%s usage=%+v", result.Model, result.Usage)

	urgency, err := result.Noul("is_urgent")
	if err != nil {
		t.Fatalf("Noul: %v", err)
	}
	if urgency < 0 || urgency > 1 {
		t.Errorf("noul = %v, want it within 0..1", urgency)
	}

	option, confidence, err := result.Choice("department")
	if err != nil {
		t.Fatalf("Choice: %v", err)
	}
	t.Logf("department=%s confidence=%.3f urgency=%.3f", option, confidence, urgency)
	switch option {
	case "billing", "technical", "account":
	default:
		t.Errorf("choice = %q, want one of the declared options", option)
	}
	if confidence < 0 || confidence > 1 {
		t.Errorf("confidence = %v, want it within 0..1", confidence)
	}

	choice, err := result.Answer("department")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if len(choice.Probabilities) != 3 {
		t.Errorf("probabilities = %v, want one per option", choice.Probabilities)
	}
	var total float64
	for _, p := range choice.Probabilities {
		total += p
	}
	if math.Abs(total-1) > 0.01 {
		t.Errorf("probabilities sum to %v, want 1", total)
	}

	score, scoreConfidence, err := result.Score("frustration")
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	t.Logf("frustration=%.3f confidence=%.3f", score, scoreConfidence)
	if score < 0 || score > 2 {
		t.Errorf("score = %v, want it within the rubric's 0..2", score)
	}

	if result.Usage.InputTokens <= 0 {
		t.Errorf("input tokens = %d, want the request to be accounted for", result.Usage.InputTokens)
	}
}

const triageState = "Hi, I've been trying to connect my Stripe account for 3 days and it keeps " +
	"failing. I'm losing sales. Please help ASAP."

// TestIntegration_GatewayEvaluate runs the questionnaire against the real Jev
// through Vercel AI Gateway.
func TestIntegration_GatewayEvaluate(t *testing.T) {
	key := gatewayKey()
	skipWithoutKey(t, key, "AI_GATEWAY_API_KEY")

	provider := systemone.New(
		systemone.WithVercelAIGateway(),
		systemone.WithAPIKey(key),
	)
	model := provider.EvaluationModel(systemone.GatewayModel)
	if id := os.Getenv("AI_GATEWAY_EVALUATION_MODEL"); id != "" {
		model = provider.EvaluationModel(id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := sdk.Evaluate(ctx,
		sdk.WithEvaluationModel(model),
		sdk.WithState(triageState),
		sdk.WithQuestions(triageQuestions()...),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	checkTriage(t, result)
}

// TestIntegration_GatewayStructuredState sends an object state and reads a
// single yes/no answer back.
func TestIntegration_GatewayStructuredState(t *testing.T) {
	key := gatewayKey()
	skipWithoutKey(t, key, "AI_GATEWAY_API_KEY")

	provider := systemone.New(systemone.WithVercelAIGateway(), systemone.WithAPIKey(key))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := sdk.Evaluate(ctx,
		sdk.WithEvaluationModel(provider.EvaluationModel(systemone.GatewayModel)),
		sdk.WithState(sdk.JSONObject{}.
			Set("order", sdk.JSONObject{}.Set("id", "A-104").Set("status", "refunded")).
			Set("message", "Where is my refund?")),
		sdk.WithQuestions(sdk.Noul("refunded", "Has the order already been refunded?")),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	refunded, err := result.Noul("refunded")
	if err != nil {
		t.Fatalf("Noul: %v", err)
	}
	t.Logf("refunded=%.3f", refunded)
	if refunded < 0.5 {
		t.Errorf("refunded = %v; the state says the order is refunded", refunded)
	}
}

// TestIntegration_TypeSafeEvaluate runs the same questionnaire against
// TypeSafe's own API, which additionally returns the score legend.
func TestIntegration_TypeSafeEvaluate(t *testing.T) {
	key := strings.TrimSpace(os.Getenv("TYPESAFE_API_KEY"))
	skipWithoutKey(t, key, "TYPESAFE_API_KEY")

	options := []systemone.Option{systemone.WithAPIKey(key)}
	if base := os.Getenv("TYPESAFE_BASE_URL"); base != "" {
		options = append(options, systemone.WithBaseURL(base))
	}
	provider := systemone.New(options...)

	modelID := systemone.DefaultModel
	if id := os.Getenv("TYPESAFE_MODEL"); id != "" {
		modelID = id
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := sdk.Evaluate(ctx,
		sdk.WithEvaluationModel(provider.EvaluationModel(modelID)),
		sdk.WithState(triageState),
		sdk.WithQuestions(triageQuestions()...),
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	checkTriage(t, result)

	answer, err := result.Answer("frustration")
	if err != nil {
		t.Fatalf("Answer: %v", err)
	}
	if len(answer.Legend) != 3 {
		t.Errorf("legend = %v, want the 3 rubric levels echoed back", answer.Legend)
	}
}

func TestIntegration_TypeSafeListModels(t *testing.T) {
	key := strings.TrimSpace(os.Getenv("TYPESAFE_API_KEY"))
	skipWithoutKey(t, key, "TYPESAFE_API_KEY")

	provider := systemone.New(systemone.WithAPIKey(key))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	models, err := provider.ListModels(ctx)
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) == 0 {
		t.Fatal("no models returned")
	}
	for _, m := range models {
		t.Logf("model: %s", m.ID)
	}
}
