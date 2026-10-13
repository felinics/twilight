# Embeddings

Embeddings convert text into dense numeric vectors that capture semantic meaning. They power search, retrieval, clustering, classification, and similarity-comparison use cases.

Twilight AI provides a unified embedding API with provider implementations for **OpenAI** and **Google Gemini**.

## Core Concepts

| Concept | Description |
|---------|-------------|
| `EmbeddingProvider` | Interface that embedding backends implement |
| `EmbeddingModel` | An embedding model bound to a provider, created via `provider.EmbeddingModel(id)`. It is separate from the chat `Model` and carries optional metadata: display name, description, default `Dimensions`, `MaxInputTokens` and `MaxEmbeddingsPerCall` |
| `Embed` | Generate a single embedding vector |
| `EmbedMany` | Generate embeddings for multiple inputs in one call |

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/felinics/twilight/provider/openai/embedding"
    "github.com/felinics/twilight/sdk"
)

func main() {
    provider := embedding.New(
        embedding.WithAPIKey("sk-..."),
    )
    model := provider.EmbeddingModel("text-embedding-3-small")

    vec, err := sdk.Embed(context.Background(), "Hello world",
        sdk.WithEmbeddingModel(model),
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Vector length: %d\n", len(vec))
}
```

## Single Embedding

`sdk.Embed` generates a vector for a single text input:

```go
vec, err := sdk.Embed(ctx, "What is the capital of France?",
    sdk.WithEmbeddingModel(model),
)
// vec is []float64
```

## Batch Embeddings

`sdk.EmbedMany` generates embeddings for multiple inputs in a single API call:

```go
result, err := sdk.EmbedMany(ctx,
    []string{"Paris", "London", "Berlin", "Tokyo"},
    sdk.WithEmbeddingModel(model),
)
// result.Embeddings[0] → vector for "Paris"
// result.Embeddings[1] → vector for "London"
// result.Usage.Tokens  → total tokens consumed
```

## Options

| Option | Description |
|--------|-------------|
| `sdk.WithEmbeddingModel(model)` | **Required.** The embedding model to use |
| `sdk.WithDimensions(d)` | Output vector dimensionality (model-dependent) |

### Custom Dimensions

Some models (e.g. `text-embedding-3-small`, `text-embedding-3-large`) allow you to reduce the output vector size:

```go
vec, err := sdk.Embed(ctx, "Hello world",
    sdk.WithEmbeddingModel(model),
    sdk.WithDimensions(256), // default is 1536 for text-embedding-3-small
)
```

Smaller dimensions reduce storage and computation cost while maintaining most of the semantic quality.

## Using a Client Instance

The package-level `sdk.Embed` and `sdk.EmbedMany` use a default client. You can also create your own:

```go
client := sdk.NewClient()
vec, err := client.Embed(ctx, "Hello world", sdk.WithEmbeddingModel(model))
result, err := client.EmbedMany(ctx, texts, sdk.WithEmbeddingModel(model))
```

## OpenAI Provider

```go
import "github.com/felinics/twilight/provider/openai/embedding"

provider := embedding.New(
    embedding.WithAPIKey("sk-..."),
)
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key |
| `WithBaseURL(url)` | `https://api.openai.com/v1` | Base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |

### Models

| Model | Dimensions | Description |
|-------|-----------|-------------|
| `text-embedding-3-small` | 1536 | Cost-effective, supports custom dimensions |
| `text-embedding-3-large` | 3072 | Higher quality, supports custom dimensions |
| `text-embedding-ada-002` | 1536 | Legacy model |

### OpenAI-Compatible Endpoints

Any service that supports the OpenAI `/embeddings` API works:

```go
// Ollama
provider := embedding.New(
    embedding.WithBaseURL("http://localhost:11434/v1"),
)
model := provider.EmbeddingModel("nomic-embed-text")

// Azure OpenAI
provider := embedding.New(
    embedding.WithAPIKey("your-azure-key"),
    embedding.WithBaseURL("https://your-resource.openai.azure.com/openai/deployments/text-embedding-3-small"),
)
```

## Google Provider

```go
import "github.com/felinics/twilight/provider/google/embedding"

provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
)
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithAPIKey(key)` | `""` | API key |
| `WithBaseURL(url)` | `https://generativelanguage.googleapis.com/v1beta` | Base URL |
| `WithHTTPClient(client)` | `&http.Client{}` | Custom HTTP client |
| `WithTaskType(taskType)` | `""` | Default task type for optimization |

### Task Types

Google embedding models optimize the output based on intended usage. Set the task type at the provider level:

```go
// For indexing documents
provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
    embedding.WithTaskType("RETRIEVAL_DOCUMENT"),
)

// For search queries
provider := embedding.New(
    embedding.WithAPIKey("AIza..."),
    embedding.WithTaskType("RETRIEVAL_QUERY"),
)
```

| Task Type | Use Case |
|-----------|----------|
| `RETRIEVAL_QUERY` | Query text for search/retrieval |
| `RETRIEVAL_DOCUMENT` | Document text being indexed |
| `SEMANTIC_SIMILARITY` | Comparing text similarity |
| `CLASSIFICATION` | Text classification |
| `CLUSTERING` | Text clustering |
| `QUESTION_ANSWERING` | Question answering |
| `FACT_VERIFICATION` | Fact verification |
| `CODE_RETRIEVAL_QUERY` | Code search queries |

### Models

| Model | Description |
|-------|-------------|
| `gemini-embedding-001` | Latest Gemini embedding model |
| `text-embedding-004` | Text embedding model |

`ListModels` lists the models that support `embedContent`, with display name,
description and `MaxInputTokens` filled in. The chat provider's `ListModels`
leaves these models out.

```go
models, err := provider.ListModels(ctx) // []sdk.EmbeddingModel
```

### API Routing

The provider automatically selects the optimal endpoint:

| Scenario | Endpoint |
|----------|----------|
| Single value (`sdk.Embed` or 1-element `EmbedMany`) | `embedContent` |
| Multiple values (`sdk.EmbedMany`) | `batchEmbedContents` |

## Common Patterns

### Semantic Search

```go
import "github.com/felinics/twilight/provider/openai/embedding"

provider := embedding.New(embedding.WithAPIKey("sk-..."))
model := provider.EmbeddingModel("text-embedding-3-small")

// Index documents
docs := []string{
    "Go is a statically typed, compiled language.",
    "Python is dynamically typed and interpreted.",
    "Rust focuses on memory safety without garbage collection.",
}
indexed, _ := sdk.EmbedMany(ctx, docs, sdk.WithEmbeddingModel(model))

// Embed a query
query, _ := sdk.Embed(ctx, "Which language is memory safe?",
    sdk.WithEmbeddingModel(model),
)

// Compare using cosine similarity (bring your own similarity function)
for i, docVec := range indexed.Embeddings {
    score := cosineSimilarity(query, docVec)
    fmt.Printf("Doc %d score: %.4f\n", i, score)
}
```

### RAG (Retrieval-Augmented Generation)

Combine embeddings with text generation for grounded answers:

```go
import (
    "github.com/felinics/twilight/provider/openai/completions"
    "github.com/felinics/twilight/provider/openai/embedding"
)

embProvider := embedding.New(embedding.WithAPIKey("sk-..."))
embModel := embProvider.EmbeddingModel("text-embedding-3-small")

chatProvider := completions.New(completions.WithAPIKey("sk-..."))
chatModel := chatProvider.ChatModel("gpt-4o-mini")

// 1. Embed and retrieve relevant documents
queryVec, _ := sdk.Embed(ctx, userQuestion, sdk.WithEmbeddingModel(embModel))
relevantDocs := searchVectorDB(queryVec) // your vector DB

// 2. Generate answer with context
result, _ := chatModel.Generate(ctx, sdk.Request{
    System: "Answer based on the provided context.",
    Messages: []sdk.Message{
        sdk.UserMessage(fmt.Sprintf("Context:\n%s\n\nQuestion: %s",
            strings.Join(relevantDocs, "\n"), userQuestion)),
    },
})
text := result.Text
```

## Next Steps

- [Providers](providers.md) — learn about chat providers (OpenAI, Anthropic, Google)
- [Tool Calling](tools.md) — define tools and execute the calls a reply asks for
- [API Reference](api-reference.md) — complete type and function reference
