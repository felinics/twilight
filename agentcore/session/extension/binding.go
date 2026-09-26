package extension

import (
	"errors"

	"github.com/felinics/twilight/agentcore/artifact"
)

type Cardinality struct {
	Min uint32
	Max *uint32
}

// BindingExtractor returns every Artifact reference inside a decoded typed
// value, in appearance order (EXT-REF-1).
type BindingExtractor interface {
	BindingIDs(value any) ([]artifact.BindingID, error)
}

// BindingExtractorFunc adapts a function to BindingExtractor.
type BindingExtractorFunc func(value any) ([]artifact.BindingID, error)

func (f BindingExtractorFunc) BindingIDs(value any) ([]artifact.BindingID, error) { return f(value) }

// BindingReferenceDefinition declares where an event may reference Artifacts
// and what admission requires of them (EXT-REF-2).
type BindingReferenceDefinition struct {
	Extractor          BindingExtractor
	Cardinality        Cardinality
	AllowedSchemes     []artifact.Scheme
	RequiredDurability artifact.Durability
}

func (d *BindingReferenceDefinition) validate() error {
	if d.Extractor == nil {
		return errors.New("binding declaration needs an Extractor")
	}
	if d.Cardinality.Max != nil && *d.Cardinality.Max < d.Cardinality.Min {
		return errors.New("cardinality max below min")
	}
	if d.RequiredDurability.Rank() < artifact.EventBound.Rank() {
		return errors.New("required durability must be at least event_bound")
	}
	return nil
}
