// Package wire is the persisted protocol of Run facts and commands: the sealed
// variant registry of each schema version, the typed fact payloads and the
// command envelope (RUN-WIR-2, RUN-WIR-3), and the codec that stores a
// MachineState as a durable snapshot.
//
// Decoding is strict: unknown fields, unknown discriminators and trailing data
// are rejected before wire data can enter a RunStore.
package wire
