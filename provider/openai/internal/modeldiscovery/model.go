// Package modeldiscovery contains the conservative model-list filtering shared
// by the OpenAI-compatible text-generation providers.
package modeldiscovery

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Endpoint identifies the generation endpoint for which models are being
// discovered. Capability names are endpoint-specific: Chat Completions support
// does not imply Responses support, or vice versa.
type Endpoint string

const (
	ChatCompletions Endpoint = "chat_completions"
	Responses       Endpoint = "responses"
)

// Model is the documented OpenAI model object plus capability extensions used
// by some compatible APIs. The OpenAI API itself normally supplies only ID,
// Object, Created, and OwnedBy.
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`

	// These compatible-API extensions stay raw so an endpoint using another
	// shape cannot make the otherwise standard model list fail to decode.
	Capabilities       json.RawMessage `json:"capabilities"`
	SupportedEndpoints json.RawMessage `json:"supported_endpoints"`
	Architecture       json.RawMessage `json:"architecture"`
}

// IsCandidate reports whether a listed model is a conservative candidate for
// endpoint. It consumes explicit endpoint capabilities when present, then
// rejects models that explicitly lack text output, and finally excludes only
// well-known dedicated non-chat model families. An unknown ID is deliberately
// retained because the standard OpenAI model object has no capability fields.
func IsCandidate(model *Model, endpoint Endpoint) bool {
	if supported, ok := endpointCapability(model.Capabilities, endpoint); ok {
		return supported
	}
	if supported, ok := endpointListed(model.SupportedEndpoints, endpoint); ok {
		return supported
	}
	if text, ok := textOutput(model.Architecture); ok && !text {
		return false
	}
	return !knownNonChatModel(model.ID, endpoint)
}

func endpointCapability(raw json.RawMessage, endpoint Endpoint) (supported, known bool) {
	var capabilities map[string]json.RawMessage
	if err := json.Unmarshal(raw, &capabilities); err != nil {
		return false, false
	}

	var keys []string
	switch endpoint {
	case ChatCompletions:
		// Azure-style APIs use chat_completion, while Mistral's model
		// catalog calls the same endpoint capability completion_chat.
		keys = []string{"chat_completion", "chat_completions", "completion_chat"}
	case Responses:
		keys = []string{"responses", "response"}
	default:
		return false, false
	}
	for _, key := range keys {
		for actual, raw := range capabilities {
			if !strings.EqualFold(actual, key) {
				continue
			}
			var value bool
			if err := json.Unmarshal(raw, &value); err == nil {
				return value, true
			}
			var text string
			if err := json.Unmarshal(raw, &text); err == nil {
				value, err = strconv.ParseBool(text)
				if err == nil {
					return value, true
				}
			}
		}
	}
	return false, false
}

func endpointListed(raw json.RawMessage, endpoint Endpoint) (supported, known bool) {
	var endpoints []string
	if err := json.Unmarshal(raw, &endpoints); err != nil || endpoints == nil {
		// A missing, null, or malformed extension conveys no usable
		// capability information. A valid empty array, however, explicitly
		// lists no supported endpoints and is handled by the false result below.
		return false, false
	}
	for _, listed := range endpoints {
		listed = strings.ToLower(strings.TrimSpace(listed))
		listed = strings.TrimSuffix(listed, "/")
		switch endpoint {
		case ChatCompletions:
			if listed == "chat_completion" || listed == "chat_completions" || strings.HasSuffix(listed, "/chat/completions") {
				return true, true
			}
		case Responses:
			if listed == "response" || listed == "responses" || strings.HasSuffix(listed, "/responses") {
				return true, true
			}
		}
	}
	return false, true
}

func textOutput(raw json.RawMessage) (supported, known bool) {
	var architecture struct {
		OutputModalities []string `json:"output_modalities"`
	}
	if err := json.Unmarshal(raw, &architecture); err != nil || len(architecture.OutputModalities) == 0 {
		return false, false
	}
	return containsFold(architecture.OutputModalities, "text"), true
}

func containsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}
	return false
}

func knownNonChatModel(id string, endpoint Endpoint) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	if slash := strings.LastIndexByte(id, '/'); slash >= 0 {
		id = id[slash+1:]
	}

	for _, prefix := range []string{
		"text-embedding-",
		"mistral-embed",
		"codestral-embed",
		"whisper-",
		"gpt-transcribe",
		"tts-",
		"gpt-4o-mini-tts",
		"gpt-4o-mini-transcribe",
		"gpt-4o-transcribe",
		"dall-e-",
		"gpt-image-",
		"chatgpt-image-",
		"sora-",
		"text-moderation-",
		"omni-moderation-",
		"mistral-moderation",
		"gpt-4o-realtime-",
		"gpt-realtime",
		"gpt-live-",
	} {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}

	// These legacy base/instruct models target the legacy Completions API, not
	// either chat endpoint represented by sdk.Model.
	for _, legacy := range []string{"babbage-002", "davinci-002", "gpt-3.5-turbo-instruct"} {
		if id == legacy || strings.HasPrefix(id, "ft:"+legacy+":") {
			return true
		}
	}

	return endpoint == ChatCompletions && knownResponsesOnlyModel(id)
}

func knownResponsesOnlyModel(id string) bool {
	for _, family := range []string{
		"computer-use-preview",
		"o1-pro",
		"o3-pro",
		"o3-deep-research",
		"o4-mini-deep-research",
	} {
		if id == family || strings.HasPrefix(id, family+"-") {
			return true
		}
	}

	// GPT-5 Codex and Pro variants use versioned names such as
	// gpt-5.1-codex-max and gpt-5.2-pro. They are Responses-only, whereas the
	// corresponding general GPT-5 models may also support Chat Completions.
	if strings.HasPrefix(id, "gpt-5") {
		parts := strings.Split(id, "-")
		for _, part := range parts {
			if part == "codex" || part == "pro" {
				return true
			}
		}
	}
	return false
}
