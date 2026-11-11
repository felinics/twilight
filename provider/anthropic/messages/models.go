package messages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const (
	// modelsPageLimit is the largest page GET /v1/models accepts.
	modelsPageLimit = 1000
	// maxModelPages stops a host that keeps reporting more pages.
	maxModelPages = 100
)

// ListModels fetches every page of GET /v1/models, following last_id as the
// after_id cursor while has_more is true. Paging stops early, with the models
// already read, when a page has no cursor, repeats the previous cursor or
// adds no model not already seen, so a host that ignores after_id ends after
// one repeated page instead of looping.
//
// Every request asks for the largest page Anthropic allows. A compatible host
// whose cap is lower may reject that with 400 or 422; when the first page is
// rejected so, the listing is retried once without limit, at the host's
// default page size.
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	models, err := p.listModels(ctx, true)
	var apiErr *sdk.APIError
	if errors.As(err, &apiErr) && errFirstPage(err) &&
		(apiErr.StatusCode == http.StatusBadRequest || apiErr.StatusCode == http.StatusUnprocessableEntity) {
		models, err = p.listModels(ctx, false)
	}
	if err != nil {
		return nil, fmt.Errorf("anthropic: list models request failed: %w", err)
	}
	return models, nil
}

// firstPageError marks a failure of the first page request, the only one a
// retry without limit can help.
type firstPageError struct{ err error }

func (e *firstPageError) Error() string { return e.err.Error() }
func (e *firstPageError) Unwrap() error { return e.err }

func errFirstPage(err error) bool {
	var fp *firstPageError
	return errors.As(err, &fp)
}

func (p *Provider) listModels(ctx context.Context, withLimit bool) ([]sdk.Model, error) {
	var models []sdk.Model
	seen := make(map[string]struct{})
	cursor := ""
	for page := range maxModelPages {
		query := map[string]string{}
		if withLimit {
			query["limit"] = strconv.Itoa(modelsPageLimit)
		}
		if cursor != "" {
			query["after_id"] = cursor
		}
		resp, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
			Method:      http.MethodGet,
			BaseURL:     p.baseURL,
			Path:        "/models",
			Query:       query,
			Headers:     p.requestHeaders(ctx),
			Provider:    p.Name(),
			DecodeError: decodeError,
		})
		if err != nil {
			if page == 0 {
				return nil, &firstPageError{err: err}
			}
			return nil, err
		}

		added := 0
		for i := range resp.Data {
			item := &resp.Data[i]
			if _, dup := seen[item.ID]; dup {
				continue
			}
			seen[item.ID] = struct{}{}
			models = append(models, item.model(p))
			added++
		}

		next := resp.LastID
		if next == "" && len(resp.Data) > 0 {
			next = resp.Data[len(resp.Data)-1].ID
		}
		if !resp.HasMore || added == 0 || next == "" || next == cursor {
			break
		}
		cursor = next
	}
	if models == nil {
		models = []sdk.Model{}
	}
	return models, nil
}

// --- Models API response types ---

type modelsListResponse struct {
	Data    []anthropicModelObject `json:"data"`
	HasMore bool                   `json:"has_more"`
	FirstID string                 `json:"first_id"`
	LastID  string                 `json:"last_id"`
}

type anthropicModelObject struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	DisplayName string `json:"display_name"`
	CreatedAt   string `json:"created_at"`

	// limits holds the optional limits and capabilities. It is zero when the
	// host sends none, or sends them in a shape that does not decode.
	limits modelLimits
}

type modelLimits struct {
	MaxInputTokens int                `json:"max_input_tokens"`
	MaxTokens      int                `json:"max_tokens"`
	Capabilities   *modelCapabilities `json:"capabilities"`
}

type capabilitySupport struct {
	Supported bool `json:"supported"`
}

func (c *capabilitySupport) ok() bool { return c != nil && c.Supported }

type modelCapabilities struct {
	ImageInput        *capabilitySupport  `json:"image_input"`
	PDFInput          *capabilitySupport  `json:"pdf_input"`
	StructuredOutputs *capabilitySupport  `json:"structured_outputs"`
	Thinking          *thinkingCapability `json:"thinking"`
	Effort            *effortCapability   `json:"effort"`
}

type thinkingCapability struct {
	Supported bool `json:"supported"`
	Types     struct {
		Adaptive *capabilitySupport `json:"adaptive"`
		Disabled *capabilitySupport `json:"disabled"`
		Enabled  *capabilitySupport `json:"enabled"`
	} `json:"types"`
}

type effortCapability struct {
	Supported bool               `json:"supported"`
	Low       *capabilitySupport `json:"low"`
	Medium    *capabilitySupport `json:"medium"`
	High      *capabilitySupport `json:"high"`
	XHigh     *capabilitySupport `json:"xhigh"`
	Max       *capabilitySupport `json:"max"`
}

// UnmarshalJSON decodes the identifying fields strictly and the limits and
// capabilities on a best-effort basis, so a compatible host whose optional
// fields differ in shape still lists its models.
func (o *anthropicModelObject) UnmarshalJSON(data []byte) error {
	type core anthropicModelObject
	var c core
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	*o = anthropicModelObject(c)
	var limits modelLimits
	if err := json.Unmarshal(data, &limits); err == nil {
		o.limits = limits
	}
	return nil
}

// model maps the item onto an sdk.Model bound to provider.
//
// ContextWindow is max_input_tokens. On Claude the response shares the
// context window with the input, so the input limit is the whole window. A
// compatible host may report the input limit after reserving max_tokens for
// the response (OpenRouter reports its context length minus max_tokens), in
// which case ContextWindow is that smaller value. MaxOutputTokens is
// max_tokens. The rest comes from capabilities and stays zero when it is
// null: tool use is not among the capabilities, so ToolCall stays false.
func (o *anthropicModelObject) model(provider sdk.Provider) sdk.Model {
	model := sdk.Model{
		ID:              o.ID,
		DisplayName:     o.DisplayName,
		Provider:        provider,
		ContextWindow:   max(o.limits.MaxInputTokens, 0),
		MaxOutputTokens: max(o.limits.MaxTokens, 0),
	}
	caps := o.limits.Capabilities
	if caps == nil {
		return model
	}
	input := []sdk.Modality{sdk.ModalityText}
	if caps.ImageInput.ok() {
		input = append(input, sdk.ModalityImage)
	}
	if caps.PDFInput.ok() {
		input = append(input, sdk.ModalityPDF)
	}
	model.Modalities = sdk.Modalities{Input: input, Output: []sdk.Modality{sdk.ModalityText}}
	model.StructuredOutput = caps.StructuredOutputs.ok()
	model.Reasoning = caps.reasoning()
	return model
}

// reasoning maps the thinking and effort capabilities. Efforts are the
// supported levels weakest first. thinking.type enabled takes a budget_tokens
// budget, whose bounds the list does not state. Dialect prefers adaptive over
// budget when a model accepts both. Off follows whether thinking.type
// disabled is accepted, and that switch is the Toggle. What the model does by
// default is not stated.
func (c *modelCapabilities) reasoning() *sdk.ModelReasoning {
	thinking := c.Thinking
	if thinking == nil || !thinking.Supported {
		return nil
	}
	reasoning := &sdk.ModelReasoning{}
	if effort := c.Effort; effort != nil && effort.Supported {
		levels := []struct {
			name    string
			support *capabilitySupport
		}{
			{sdk.ReasoningEffortLow, effort.Low},
			{sdk.ReasoningEffortMedium, effort.Medium},
			{sdk.ReasoningEffortHigh, effort.High},
			{sdk.ReasoningEffortXHigh, effort.XHigh},
			{sdk.ReasoningEffortMax, effort.Max},
		}
		for _, level := range levels {
			if level.support.ok() {
				reasoning.Efforts = append(reasoning.Efforts, level.name)
			}
		}
	}

	types := &thinking.Types
	if types.Enabled.ok() {
		reasoning.Budget = &sdk.ReasoningBudget{}
	}
	switch {
	case types.Adaptive.ok():
		reasoning.Dialect = sdk.ReasoningDialectAdaptive
	case types.Enabled.ok():
		reasoning.Dialect = sdk.ReasoningDialectBudget
	}
	if types.Disabled != nil {
		if types.Disabled.Supported {
			reasoning.Off = sdk.ReasoningOffAccepted
			reasoning.Toggle = types.Adaptive.ok() || types.Enabled.ok()
		} else {
			reasoning.Off = sdk.ReasoningOffRejected
		}
	}
	return reasoning
}
