// Package schema composes the Run protocol's contracts: the state machine,
// the fact and command codec, the digest rules, the snapshot codec, the
// identity derivation and the frozen-body codec. They have no version and
// cannot be replaced (RUN-CMT-8): identity derivation and digests are
// persisted semantics, and the shape of persisted facts evolves through each
// event type's payload version and its codec (SES-VER-1, EXT-REG-2).
package schema
