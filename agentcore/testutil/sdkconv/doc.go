// Package sdkconv provides test-side conversions between the SDK's model types and the persisted
// values of agentcore/run/model. Freeze* turns a caller-owned SDK value into
// a detached run value; the function named after a value type converts that
// value back into a detached SDK value.
//
// This package is test support, not part of Agent Core's production protocol.
package sdkconv
