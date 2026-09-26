package speech

import "strings"

// formatPCM16 is the raw PCM output format name on the MiMo wire.
const formatPCM16 = "pcm16"

type audioConfig struct {
	Voice       string
	Format      string
	StylePrompt string
}

func parseConfig(cfg map[string]any) audioConfig {
	out := audioConfig{
		Voice:  defaultVoice,
		Format: defaultFormat,
	}
	if cfg == nil {
		return out
	}
	if v, ok := cfg["voice"].(string); ok && strings.TrimSpace(v) != "" {
		out.Voice = strings.TrimSpace(v)
	}
	if v, ok := cfg["format"].(string); ok && strings.TrimSpace(v) != "" {
		out.Format = strings.TrimSpace(v)
	}
	if v, ok := cfg["style_prompt"].(string); ok {
		out.StylePrompt = strings.TrimSpace(v)
	}
	return out
}

func contentTypeForFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case formatPCM16:
		return "audio/pcm"
	case "wav":
		return "audio/wav"
	default:
		return speechContentType
	}
}
