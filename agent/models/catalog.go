// Package models is the application's model catalog: the table that maps a
// logical run.ModelRef, the name a preset freezes, to a physical model at a
// provider. It is deliberately a lookup and nothing more. Routing between
// providers, fallback, quotas and key rotation are a gateway's job; a
// deployment that wants them points an Entry's BaseURL at one.
//
// The catalog reads nothing from its process environment. An Entry names
// the secret its credential lives under; Build resolves the name through
// the secrets.Resolver the deployment provides (a mounted Kubernetes
// Secret, a local configuration, a vault). The catalog document therefore
// never carries a credential and serves every deployment unchanged. The package lives in
// agent/, not agentcore/: Agent Core knows the loop.ModelCatalog seam and
// the frozen ModelRef, and nothing about providers or credentials.
package models

import (
	"context"
	"fmt"
	"sort"

	"github.com/felinics/twilight/agent/secrets"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	anthropic "github.com/felinics/twilight/provider/anthropic/messages"
	copilot "github.com/felinics/twilight/provider/github/copilot"
	google "github.com/felinics/twilight/provider/google/generativeai"
	completions "github.com/felinics/twilight/provider/openai/completions"
	responses "github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

// Kind names a provider implementation in the tree.
type Kind string

const (
	KindAnthropic       Kind = "anthropic"
	KindOpenAI          Kind = "openai" // chat completions
	KindOpenAIResponses Kind = "openai-responses"
	KindGoogle          Kind = "google"
	KindCopilot         Kind = "copilot"
)

// Entry maps one logical ModelRef to a physical model. The credential is
// named, not carried: exactly one of APIKeySecret and AuthTokenSecret names
// the secret Build looks up. APIKey is the provider's key header, AuthToken
// a bearer token for the providers that take one (anthropic behind a
// gateway). BaseURL empty means the provider's default endpoint.
type Entry struct {
	Ref             run.ModelRef `json:"ref"`
	Kind            Kind         `json:"kind"`
	Model           string       `json:"model"`
	BaseURL         string       `json:"baseURL,omitempty"`
	APIKeySecret    string       `json:"apiKeySecret,omitempty"`
	AuthTokenSecret string       `json:"authTokenSecret,omitempty"`
}

// credential is what an Entry resolved to.
type credential struct {
	apiKey    string
	authToken string
}

// Catalog is loop.ModelCatalog over the built entries.
type Catalog struct {
	invokers map[run.ModelRef]loop.ModelInvoker
}

// Build resolves every Entry's credential through resolver and constructs
// one provider per distinct (Kind, BaseURL, credential) and a model per
// Entry. An Entry the provider would refuse is refused here: no credential
// named, both named, a name the resolver does not hold, an unknown Kind, a
// duplicate Ref.
func Build(ctx context.Context, entries []Entry, resolver secrets.Resolver) (*Catalog, error) {
	if resolver == nil {
		return nil, fmt.Errorf("models: Build requires a secrets.Resolver")
	}
	type endpoint struct {
		kind    Kind
		baseURL string
		cred    credential
	}
	providers := map[endpoint]sdk.Provider{}
	c := &Catalog{invokers: make(map[run.ModelRef]loop.ModelInvoker, len(entries))}
	for _, e := range entries {
		if e.Ref == "" || e.Model == "" {
			return nil, fmt.Errorf("models: entry %q needs a ref and a model", e.Ref)
		}
		if _, dup := c.invokers[e.Ref]; dup {
			return nil, fmt.Errorf("models: duplicate ref %q", e.Ref)
		}
		cred, err := resolve(ctx, e, resolver)
		if err != nil {
			return nil, fmt.Errorf("models: %s: %w", e.Ref, err)
		}
		k := endpoint{e.Kind, e.BaseURL, cred}
		p, ok := providers[k]
		if !ok {
			if p, err = newProvider(e.Kind, e.BaseURL, cred); err != nil {
				return nil, fmt.Errorf("models: %s: %w", e.Ref, err)
			}
			providers[k] = p
		}
		c.invokers[e.Ref] = &invoker{model: &sdk.Model{ID: e.Model, Provider: p, Type: sdk.ModelTypeChat}}
	}
	return c, nil
}

// FromModels builds a catalog over ready sdk.Models, for hosts that
// construct providers themselves and for tests.
func FromModels(models map[run.ModelRef]*sdk.Model) (*Catalog, error) {
	c := &Catalog{invokers: make(map[run.ModelRef]loop.ModelInvoker, len(models))}
	for ref, m := range models {
		if ref == "" || m == nil || m.Provider == nil {
			return nil, fmt.Errorf("models: ref %q needs a model with a provider", ref)
		}
		c.invokers[ref] = &invoker{model: m}
	}
	return c, nil
}

// ResolveModel is loop.ModelCatalog.
func (c *Catalog) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	inv, ok := c.invokers[ref]
	if !ok {
		return nil, fmt.Errorf("models: unknown model ref %q", ref)
	}
	return inv, nil
}

// Invokers returns the catalog as the map app.ExecutorConfig.Models takes.
func (c *Catalog) Invokers() map[run.ModelRef]loop.ModelInvoker {
	out := make(map[run.ModelRef]loop.ModelInvoker, len(c.invokers))
	for ref, inv := range c.invokers {
		out[ref] = inv
	}
	return out
}

// Refs lists the logical names the catalog serves, sorted.
func (c *Catalog) Refs() []run.ModelRef {
	refs := make([]run.ModelRef, 0, len(c.invokers))
	for ref := range c.invokers {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}

func resolve(ctx context.Context, e Entry, resolver secrets.Resolver) (credential, error) {
	switch {
	case e.APIKeySecret == "" && e.AuthTokenSecret == "":
		return credential{}, fmt.Errorf("no credential named for %s: set apiKeySecret or authTokenSecret", e.Kind)
	case e.APIKeySecret != "" && e.AuthTokenSecret != "":
		return credential{}, fmt.Errorf("both apiKeySecret and authTokenSecret named for %s", e.Kind)
	case e.AuthTokenSecret != "" && e.Kind != KindAnthropic:
		return credential{}, fmt.Errorf("authTokenSecret is only for %s; %s takes apiKeySecret", KindAnthropic, e.Kind)
	}
	name := e.APIKeySecret
	if name == "" {
		name = e.AuthTokenSecret
	}
	value, err := resolver.Lookup(ctx, name)
	if err != nil {
		return credential{}, err
	}
	if value == "" {
		return credential{}, fmt.Errorf("secret %q is empty", name)
	}
	if e.APIKeySecret != "" {
		return credential{apiKey: value}, nil
	}
	return credential{authToken: value}, nil
}

func newProvider(kind Kind, baseURL string, cred credential) (sdk.Provider, error) {
	switch kind {
	case KindAnthropic:
		opts := []anthropic.Option{}
		if cred.apiKey != "" {
			opts = append(opts, anthropic.WithAPIKey(cred.apiKey))
		} else {
			opts = append(opts, anthropic.WithAuthToken(cred.authToken))
		}
		if baseURL != "" {
			opts = append(opts, anthropic.WithBaseURL(baseURL))
		}
		return anthropic.New(opts...), nil
	case KindOpenAI:
		opts := []completions.Option{completions.WithAPIKey(cred.apiKey)}
		if baseURL != "" {
			opts = append(opts, completions.WithBaseURL(baseURL))
		}
		return completions.New(opts...), nil
	case KindOpenAIResponses:
		opts := []responses.Option{responses.WithAPIKey(cred.apiKey)}
		if baseURL != "" {
			opts = append(opts, responses.WithBaseURL(baseURL))
		}
		return responses.New(opts...), nil
	case KindGoogle:
		opts := []google.Option{google.WithAPIKey(cred.apiKey)}
		if baseURL != "" {
			opts = append(opts, google.WithBaseURL(baseURL))
		}
		return google.New(opts...), nil
	case KindCopilot:
		opts := []copilot.Option{copilot.WithGitHubToken(cred.apiKey)}
		if baseURL != "" {
			opts = append(opts, copilot.WithBaseURL(baseURL))
		}
		return copilot.New(opts...), nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", kind)
	}
}

// invoker is the one adaptation between a frozen request and an sdk.Model:
// the request names the logical ModelRef in its Model field, and sdk.Model
// binds requests to its own physical ID, so the field is cleared and the
// model fills it in. Everything else, Generate and Stream alike, is the
// sdk.Model's.
type invoker struct {
	model *sdk.Model
}

func (i *invoker) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // hugeParam: interface method
	req.Model = ""
	return i.model.Generate(ctx, req)
}

func (i *invoker) Stream(ctx context.Context, req sdk.Request) (sdk.ModelStream, error) { //nolint:gocritic // hugeParam: interface method
	req.Model = ""
	return i.model.Stream(ctx, req)
}

var (
	_ loop.ModelCatalog          = (*Catalog)(nil)
	_ loop.ModelInvoker          = (*invoker)(nil)
	_ loop.StreamingModelInvoker = (*invoker)(nil)
)
