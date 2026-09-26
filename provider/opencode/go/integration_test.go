package opencodego_test

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/internal/testutil"
	opencodego "github.com/felinics/twilight/provider/opencode/go"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	os.Exit(m.Run())
}

// ---------- integration tests (real API, skipped without env) ----------
//
// OPENCODE_GO_API_KEY enables them. OPENCODE_GO_MODELS narrows the run to a
// comma-separated list of model IDs; by default every catalog model is
// exercised. These requests incur upstream usage charges.

func newIntegrationProvider(t *testing.T) *opencodego.Provider {
	t.Helper()
	apiKey := os.Getenv("OPENCODE_GO_API_KEY")
	if apiKey == "" {
		t.Skip("skipping: OPENCODE_GO_API_KEY not set")
	}
	options := []opencodego.Option{
		opencodego.WithAPIKey(apiKey),
		opencodego.WithHeaders(map[string]string{"User-Agent": "twilight-integration-test/1.0"}),
	}
	if base := os.Getenv("OPENCODE_GO_BASE_URL"); base != "" {
		options = append(options, opencodego.WithBaseURL(base))
	}
	return opencodego.New(options...)
}

func integrationModels() []opencodego.ModelDescriptor {
	var only []string
	for id := range strings.SplitSeq(os.Getenv("OPENCODE_GO_MODELS"), ",") {
		if id = strings.TrimSpace(id); id != "" {
			only = append(only, id)
		}
	}
	models := opencodego.Catalog()
	if len(only) == 0 {
		return models
	}
	return slices.DeleteFunc(models, func(m opencodego.ModelDescriptor) bool { return !slices.Contains(only, m.ID) })
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	t.Cleanup(cancel)
	return sdk.WithRequestHeaders(ctx, map[string]string{opencodego.SessionHeader: "twilight-integration-" + t.Name()})
}

func integrationLookupTool() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name: "lookup", Description: "Look up a value by key",
		Parameters: &jsonschema.Schema{
			Type: "object", Properties: map[string]*jsonschema.Schema{"key": {Type: "string"}}, Required: []string{"key"},
		},
	}
}

// The live catalog must still list every model that has a local route.
func TestIntegration_CatalogIsLive(t *testing.T) {
	p := newIntegrationProvider(t)
	models, err := p.ListModels(integrationContext(t))
	if err != nil {
		t.Fatal(err)
	}
	live := make(map[string]bool, len(models))
	for _, model := range models {
		live[model.ID] = true
		if _, err := p.ProtocolForModel(model.ID); err != nil {
			t.Logf("live model without a local route: %s", model.ID)
		}
	}
	for _, entry := range opencodego.Catalog() {
		if !live[entry.ID] {
			t.Errorf("catalog model %q is no longer listed upstream", entry.ID)
		}
	}
}

// A streamed tool loop of up to four steps replays each model's own reasoning. Whether
// the model chooses to call the tool is its decision; the request must succeed.
func TestIntegration_StreamToolLoop(t *testing.T) {
	p := newIntegrationProvider(t)
	for _, entry := range integrationModels() {
		t.Run(entry.ID, func(t *testing.T) {
			t.Parallel()
			ctx := integrationContext(t)
			model := p.ChatModel(entry.ID)
			messages := []sdk.Message{sdk.UserMessage("Call the lookup tool with key 'color', then reply with only the value it returned.")}
			var result *sdk.ModelResult
			steps := 0
			for steps < 4 {
				steps++
				stream, err := model.Stream(ctx, sdk.Request{Messages: messages, Tools: []sdk.ToolDefinition{integrationLookupTool()}})
				if err != nil {
					t.Fatal(err)
				}
				for range stream.Parts {
				}
				if result, err = stream.Result(); err != nil {
					t.Fatal(err)
				}
				if len(result.ToolCalls) == 0 {
					break
				}
				messages = append(messages, stepMessages(result, "blue")...)
			}
			if strings.TrimSpace(result.Text) == "" {
				t.Errorf("empty text: %+v", result)
			}
			t.Logf("[%s] steps=%d text=%q", entry.Protocol, steps, result.Text)
		})
	}
}

// Persisted history whose assistant tool call carries no reasoning, sent with
// thinking enabled. Some Completions routes reject it unless it is padded.
func TestIntegration_ReplayWithoutReasoning(t *testing.T) {
	p := newIntegrationProvider(t)
	maxTokens := 2048
	effort := "high"
	input, err := sdk.ToolArgumentsJSON(map[string]any{"key": "color"})
	if err != nil {
		t.Fatal(err)
	}
	history := []sdk.Message{
		sdk.UserMessage("Use lookup for key 'color', then tell me the result."),
		{Role: sdk.MessageRoleAssistant, Content: []sdk.MessagePart{sdk.ToolCallPart{ToolCallID: "call_1", ToolName: "lookup", Input: input}}},
		sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "call_1", ToolName: "lookup", Result: sdk.TextOutput("blue")}),
	}
	for _, entry := range integrationModels() {
		t.Run(entry.ID, func(t *testing.T) {
			t.Parallel()
			result, err := p.ChatModel(entry.ID).Generate(integrationContext(t), sdk.Request{
				MaxTokens: &maxTokens, ReasoningEffort: &effort,
				Tools: []sdk.ToolDefinition{integrationLookupTool()}, Messages: history,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.ToLower(result.Text), "blue") {
				t.Errorf("text = %q", result.Text)
			}
		})
	}
}

// Several Completions models ignore the developer role on the wire, so a
// developer instruction must still take effect on every model.
func TestIntegration_DeveloperInstruction(t *testing.T) {
	p := newIntegrationProvider(t)
	maxTokens := 2048
	for _, entry := range integrationModels() {
		t.Run(entry.ID, func(t *testing.T) {
			t.Parallel()
			result, err := p.ChatModel(entry.ID).Generate(integrationContext(t), sdk.Request{
				MaxTokens: &maxTokens,
				Messages:  []sdk.Message{sdk.DeveloperMessage("Always answer in uppercase."), sdk.UserMessage("Say: pong")},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(result.Text, "PONG") {
				t.Errorf("developer instruction ignored: %q", result.Text)
			}
		})
	}
}
