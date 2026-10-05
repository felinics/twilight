package sdk

// InputTokenDetail partitions Usage.InputTokens into uncached, cache-read, and
// cache-write tokens: NoCacheTokens + CacheReadTokens + CacheWriteTokens = InputTokens.
type InputTokenDetail struct {
	// NoCacheTokens counts input tokens neither read from nor written to cache.
	// When the provider reports no cache details, it equals Usage.InputTokens.
	NoCacheTokens int `json:"noCacheTokens"`
	// CacheReadTokens counts input tokens read from cache.
	CacheReadTokens int `json:"cacheReadTokens"`
	// CacheWriteTokens counts input tokens written to cache, including all TTLs.
	CacheWriteTokens int `json:"cacheWriteTokens"`
	// CacheWrite5mTokens is the number of tokens written to the 5-minute cache
	// (Anthropic-specific, populated when using cache_control with default TTL).
	// It is a subset of CacheWriteTokens, not an additional input token count.
	CacheWrite5mTokens int `json:"cacheWrite5mTokens,omitempty"`
	// CacheWrite1hTokens is the number of tokens written to the 1-hour cache
	// (Anthropic-specific, populated when using cache_control with ttl="1h").
	// It is a subset of CacheWriteTokens, not an additional input token count.
	CacheWrite1hTokens int `json:"cacheWrite1hTokens,omitempty"`
}

type OutputTokenDetail struct {
	TextTokens      int `json:"textTokens"`
	ReasoningTokens int `json:"reasoningTokens"`
}

type Usage struct {
	// InputTokens is the total input count, including cache reads and writes.
	// Providers normalize their wire usage to this shared meaning.
	InputTokens     int `json:"inputTokens"`
	OutputTokens    int `json:"outputTokens"`
	TotalTokens     int `json:"totalTokens"`
	ReasoningTokens int `json:"reasoningTokens,omitempty"`
	// CachedInputTokens equals InputTokenDetails.CacheReadTokens.
	CachedInputTokens int `json:"cachedInputTokens,omitempty"`
	// CacheReadTokensReported is true when the provider explicitly reports cache reads, including zero.
	CacheReadTokensReported bool `json:"cacheReadTokensReported"`
	// InputTokenDetails partitions InputTokens; cache TTL details are subsets.
	InputTokenDetails  InputTokenDetail  `json:"inputTokenDetails,omitempty"`
	OutputTokenDetails OutputTokenDetail `json:"outputTokenDetails,omitempty"`
}

// Add sums counters and reports cache reads only when both usages report them.
//
//nolint:gocritic // hugeParam: Add is a pure value operation and must not mutate caller-owned Usage.
func (u Usage) Add(other Usage) Usage {
	u.CacheReadTokensReported = u.CacheReadTokensReported && other.CacheReadTokensReported
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.TotalTokens += other.TotalTokens
	u.ReasoningTokens += other.ReasoningTokens
	u.CachedInputTokens += other.CachedInputTokens
	u.InputTokenDetails.NoCacheTokens += other.InputTokenDetails.NoCacheTokens
	u.InputTokenDetails.CacheReadTokens += other.InputTokenDetails.CacheReadTokens
	u.InputTokenDetails.CacheWriteTokens += other.InputTokenDetails.CacheWriteTokens
	u.InputTokenDetails.CacheWrite5mTokens += other.InputTokenDetails.CacheWrite5mTokens
	u.InputTokenDetails.CacheWrite1hTokens += other.InputTokenDetails.CacheWrite1hTokens
	u.OutputTokenDetails.TextTokens += other.OutputTokenDetails.TextTokens
	u.OutputTokenDetails.ReasoningTokens += other.OutputTokenDetails.ReasoningTokens
	return u
}
