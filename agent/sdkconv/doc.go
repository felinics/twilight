// Package sdkconv converts between the SDK's model types and the persisted
// values of agentcore/run/model. Freeze* turns a caller-owned SDK value into
// a detached agent-owned value; the function named after a value type
// converts that value back into a detached SDK value.
//
// It is the agent tier's boundary with the SDK: agentcore never imports the
// SDK, and every conversion happens here or in an adapter beside it.
package sdkconv
