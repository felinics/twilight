package session

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SourceID and ModuleID identify a Session module: the source that publishes
// it and its name within that source (EXT-REG-1). They are declared here so
// the kernel can key extension slots by module without knowing the module
// framework; agentcore/session/extension aliases them.
type (
	SourceID string
	ModuleID string
)

// ModuleKey is the identity of one module: (Source, ID). On the wire it is
// the string "source/id".
type ModuleKey struct {
	Source SourceID
	ID     ModuleID
}

// String renders the key as "source/id".
func (k ModuleKey) String() string { return string(k.Source) + "/" + string(k.ID) }

// MarshalText renders the key as a JSON object key.
func (k ModuleKey) MarshalText() ([]byte, error) {
	if err := validIdentity("module source", string(k.Source)); err != nil {
		return nil, err
	}
	if strings.Contains(string(k.Source), "/") {
		return nil, fmt.Errorf("module source %q contains %q", k.Source, "/")
	}
	if err := validIdentity("module ID", string(k.ID)); err != nil {
		return nil, err
	}
	return []byte(k.String()), nil
}

// UnmarshalText parses "source/id"; the first separator splits the two.
func (k *ModuleKey) UnmarshalText(text []byte) error {
	source, id, ok := strings.Cut(string(text), "/")
	if !ok || source == "" || id == "" {
		return fmt.Errorf("module key %q is not source/id", text)
	}
	k.Source, k.ID = SourceID(source), ModuleID(id)
	return nil
}

// RawValue is one module's extension value: JSON the kernel stores and
// returns byte for byte without interpreting it.
type RawValue []byte

// MarshalJSON writes the bytes as they are; an empty value is null.
func (v RawValue) MarshalJSON() ([]byte, error) {
	if len(v) == 0 {
		return []byte("null"), nil
	}
	return v, nil
}

// UnmarshalJSON keeps the bytes as they are.
func (v *RawValue) UnmarshalJSON(raw []byte) error {
	*v = append((*v)[:0], raw...)
	return nil
}

// Extensions are the module extension slots of a header or a commit
// (SES-WIR-5): each module reads and writes its own key, the kernel stores
// every entry opaquely. A nil map is the absence of extensions.
type Extensions map[ModuleKey]RawValue

// ValidateExtensions checks the shape of an extension map: every key is a
// well-formed ModuleKey and every value is valid, non-empty JSON.
func ValidateExtensions(ext Extensions) error {
	for k, v := range ext {
		if _, err := k.MarshalText(); err != nil {
			return err
		}
		if len(v) == 0 {
			return fmt.Errorf("extension %s: empty value", k)
		}
		if !json.Valid(v) {
			return fmt.Errorf("extension %s: value is not valid JSON", k)
		}
	}
	return nil
}

// Equal reports whether e and other hold the same keys with the same bytes.
func (e Extensions) Equal(other Extensions) bool {
	if len(e) != len(other) {
		return false
	}
	for k, v := range e {
		if w, ok := other[k]; !ok || string(v) != string(w) {
			return false
		}
	}
	return true
}

// Clone returns an independent copy.
func (e Extensions) Clone() Extensions {
	if e == nil {
		return nil
	}
	out := make(Extensions, len(e))
	for k, v := range e {
		out[k] = append(RawValue(nil), v...)
	}
	return out
}
