// Package wire is the persisted protocol of Turn facts and commands: the sealed
// variant registry of each schema version, the typed fact payloads and the
// command envelope, and the codec that stores a
// MachineState as a durable snapshot.
// Decoding is strict: unknown fields, unknown discriminators and trailing data
// are rejected before wire data can enter a TurnStore.
package wire
