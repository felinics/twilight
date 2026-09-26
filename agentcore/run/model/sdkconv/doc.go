// Package sdkconv converts between the SDK's model types and the persisted
// values of agent/run/model. Freeze* turns a caller-owned SDK value into a
// detached agent-owned value; the function named after a value type converts
// that value back into a detached SDK value.
//
// It is the only place the Run layer touches sdk: model, canonical and the
// Run core never import it.
package sdkconv
