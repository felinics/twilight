// Package model holds the durable, provider-neutral model data a Run persists
// by digest (RUN-WIR-4): ModelRequest, Message, ToolDefinition, ModelResult,
// Usage and their parts, plus the canonical rendering of tool arguments.
//
// Every type is a closed, JSON-stable value with no SDK interface inside, so
// its canonical encoding is fixed for the life of the schema version that
// digests it. The package does not depend on the SDK; agent/run/model/sdkconv
// converts in both directions.
package model
