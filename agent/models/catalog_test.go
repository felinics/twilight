package models_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/models"
	"github.com/felinics/twilight/agent/secrets"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/sdk"
)

// fakeProvider records the model id each request reaches it with.
type fakeProvider struct {
	name string
	seen []string
}

func (p *fakeProvider) Name() string                                    { return p.name }
func (p *fakeProvider) ListModels(context.Context) ([]sdk.Model, error) { return nil, nil }
func (p *fakeProvider) Test(context.Context) *sdk.ProviderTestResult    { return nil }
func (p *fakeProvider) TestModel(context.Context, string) (*sdk.ModelTestResult, error) {
	return nil, nil
}
func (p *fakeProvider) DoGenerate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	p.seen = append(p.seen, req.Model)
	return sdk.ModelResult{Text: "from " + req.Model, FinishReason: sdk.FinishReasonStop}, nil
}
func (p *fakeProvider) DoStream(_ context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) {
	p.seen = append(p.seen, req.Model)
	ch := make(chan sdk.StreamPart, 3)
	ch <- &sdk.TextDeltaPart{Text: "from "}
	ch <- &sdk.TextDeltaPart{Text: req.Model}
	ch <- &sdk.FinishPart{FinishReason: sdk.FinishReasonStop}
	close(ch)
	return ch, nil
}

// The frozen request names the logical ref; the provider receives the
// physical id, on Generate and on Stream alike, and an unknown ref is
// refused at resolution.
func TestCatalogBindsLogicalRefToPhysicalModel(t *testing.T) {
	ctx := context.Background()
	p := &fakeProvider{name: "fake"}
	cat, err := models.FromModels(map[run.ModelRef]*sdk.Model{"fast": {ID: "vendor-small-2", Provider: p}})
	if err != nil {
		t.Fatal(err)
	}
	inv, err := cat.ResolveModel("fast")
	if err != nil {
		t.Fatal(err)
	}
	frozen := sdk.Request{Model: "fast", Messages: []sdk.Message{sdk.UserMessage("hi")}}
	result, err := inv.Generate(ctx, frozen)
	if err != nil || result.Text != "from vendor-small-2" {
		t.Fatalf("generate = %+v, %v", result, err)
	}
	stream, err := inv.(loop.StreamingModelInvoker).Stream(ctx, frozen)
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil || streamed.Text != "from vendor-small-2" {
		t.Fatalf("stream = %+v, %v", streamed, err)
	}
	if len(p.seen) != 2 || p.seen[0] != "vendor-small-2" || p.seen[1] != "vendor-small-2" {
		t.Fatalf("provider saw models %v, want the physical id twice", p.seen)
	}
	if _, err := cat.ResolveModel("slow"); err == nil {
		t.Fatal("unknown ref resolved")
	}
	if refs := cat.Refs(); len(refs) != 1 || refs[0] != "fast" {
		t.Fatalf("refs = %v", refs)
	}
}

// Build resolves the credential each Entry names through the Resolver it is
// given, never through the process environment, and refuses an entry it
// cannot serve: no secret named, both named, a name the resolver lacks, an
// empty value, an unknown kind, a duplicate ref, a bearer token for a
// provider that takes a key.
func TestBuildResolvesSecrets(t *testing.T) {
	ctx := context.Background()
	t.Setenv("OPENAI_API_KEY", "must-not-be-read")
	resolver := secrets.Static{"openai": "k1", "anthropic-bearer": "t1", "empty": ""}
	cat, err := models.Build(ctx, []models.Entry{
		{Ref: "fast", Kind: models.KindOpenAI, Model: "vendor-small", APIKeySecret: "openai", BaseURL: "https://gateway.example/v1"},
		{Ref: "smart", Kind: models.KindOpenAI, Model: "vendor-large", APIKeySecret: "openai", BaseURL: "https://gateway.example/v1"},
		{Ref: "deep", Kind: models.KindAnthropic, Model: "vendor-x", AuthTokenSecret: "anthropic-bearer"},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if refs := cat.Refs(); len(refs) != 3 || refs[0] != "deep" || refs[1] != "fast" || refs[2] != "smart" {
		t.Fatalf("refs = %v", refs)
	}
	if inv := cat.Invokers(); len(inv) != 3 || inv["fast"] == nil {
		t.Fatalf("invokers = %v", inv)
	}
	for _, tc := range []struct {
		name    string
		entries []models.Entry
		wantErr error
	}{
		{"no secret named", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m"}}, nil},
		{"both named", []models.Entry{{Ref: "a", Kind: models.KindAnthropic, Model: "m", APIKeySecret: "openai", AuthTokenSecret: "anthropic-bearer"}}, nil},
		{"unknown secret", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m", APIKeySecret: "missing"}}, secrets.ErrNotFound},
		{"empty secret", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m", APIKeySecret: "empty"}}, nil},
		{"bearer for a key provider", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m", AuthTokenSecret: "anthropic-bearer"}}, nil},
		{"unknown kind", []models.Entry{{Ref: "a", Kind: "mystery", Model: "m", APIKeySecret: "openai"}}, nil},
		{"duplicate ref", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m", APIKeySecret: "openai"}, {Ref: "a", Kind: models.KindOpenAI, Model: "n", APIKeySecret: "openai"}}, nil},
		{"missing model", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, APIKeySecret: "openai"}}, nil},
	} {
		_, err := models.Build(ctx, tc.entries, resolver)
		if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
			t.Fatalf("%s: build = %v, want failure %v", tc.name, err, tc.wantErr)
		}
	}
	if _, err := models.Build(ctx, nil, nil); err == nil {
		t.Fatal("build without a resolver succeeded")
	}
}

// Load reads exactly one catalog document that names secrets and carries
// none; a document with a literal credential, an empty list or trailing
// content is refused. A catalog built from a mounted volume resolves.
func TestLoadAndBuildFromVolume(t *testing.T) {
	ctx := context.Background()
	entries, err := models.Load(strings.NewReader(`{"models":[{"ref":"fast","kind":"openai","model":"vendor-small","apiKeySecret":"openai"}]}`))
	if err != nil || len(entries) != 1 || entries[0].APIKeySecret != "openai" {
		t.Fatalf("Load = %+v, %v", entries, err)
	}
	for name, doc := range map[string]string{
		"literal credential": `{"models":[{"ref":"fast","kind":"openai","model":"m","apiKey":"literal"}]}`,
		"empty list":         `{"models":[]}`,
		"trailing document":  `{"models":[{"ref":"fast","kind":"openai","model":"m","apiKeySecret":"openai"}]} {"models":[]}`,
		"trailing garbage":   `{"models":[{"ref":"fast","kind":"openai","model":"m","apiKeySecret":"openai"}]} x`,
	} {
		if _, err := models.Load(strings.NewReader(doc)); err == nil {
			t.Fatalf("%s: loaded", name)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "openai"), []byte("k-from-volume\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cat, err := models.Build(ctx, entries, secrets.Dir(dir))
	if err != nil || len(cat.Refs()) != 1 {
		t.Fatalf("build from volume = %v %v", cat, err)
	}
}
