package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// File is the document a deployment hands the model backend: the entries
// of its catalog. In Kubernetes it is a Secret mounted as a file, so the
// credentials it carries never pass through the process environment or a
// command line.
type File struct {
	Models []Entry `json:"models"`
}

// Load reads a File from r and returns its entries.
func Load(r io.Reader) ([]Entry, error) {
	var f File
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("models: decode catalog: %w", err)
	}
	// One document: anything after it is a mistake in the file, not a
	// second catalog to ignore.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing content after the catalog document")
		}
		return nil, fmt.Errorf("models: decode catalog: %w", err)
	}
	if len(f.Models) == 0 {
		return nil, fmt.Errorf("models: catalog names no models")
	}
	return f.Models, nil
}

// LoadFile reads the File at path.
func LoadFile(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}
