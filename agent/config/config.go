// Package config loads the typed configuration documents of the reference
// agent's components (CLD-CMP): one JSON document per process, read from a
// file the deployment mounts, never from the process environment. Unknown
// fields and trailing content are errors, so a mistyped key cannot pass
// silently as a default.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Load decodes one document of type T from r.
func Load[T any](r io.Reader) (T, error) {
	var out T
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return out, fmt.Errorf("config: decode: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing content after the document")
		}
		return out, fmt.Errorf("config: decode: %w", err)
	}
	return out, nil
}

// LoadFile decodes the document at path.
func LoadFile[T any](path string) (T, error) {
	f, err := os.Open(path)
	if err != nil {
		var zero T
		return zero, err
	}
	defer f.Close()
	out, err := Load[T](f)
	if err != nil {
		return out, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

// Duration is a time.Duration written as a Go duration string ("30s").
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	if parsed < 0 {
		return fmt.Errorf("duration %q is negative", s)
	}
	*d = Duration(parsed)
	return nil
}

// Identity is a process identity: given inline, or read from a file the
// deployment mounts (the pod name through the downward API). Exactly one
// of the two is set.
type Identity struct {
	Name string `json:"name,omitempty"`
	File string `json:"file,omitempty"`
}

// Resolve returns the identity's name.
func (i Identity) Resolve() (string, error) {
	switch {
	case i.Name != "" && i.File != "":
		return "", errors.New("config: identity gives both name and file")
	case i.Name != "":
		return i.Name, nil
	case i.File != "":
		raw, err := os.ReadFile(i.File)
		if err != nil {
			return "", fmt.Errorf("config: identity file: %w", err)
		}
		name := strings.TrimSpace(string(raw))
		if name == "" {
			return "", fmt.Errorf("config: identity file %s is empty", i.File)
		}
		return name, nil
	default:
		return "", errors.New("config: identity requires name or file")
	}
}

// Store names one durable database (CLD-STO): a SQLite file for a single
// machine, or a Postgres database every replica shares. Exactly one is
// set.
type Store struct {
	SQLite   string    `json:"sqlite,omitempty"`
	Postgres *Postgres `json:"postgres,omitempty"`
}

// Validate checks that exactly one backend is named.
func (s Store) Validate() error {
	switch {
	case s.SQLite != "" && s.Postgres != nil:
		return errors.New("config: store names both sqlite and postgres")
	case s.SQLite == "" && s.Postgres == nil:
		return errors.New("config: store requires sqlite or postgres")
	case s.Postgres != nil:
		_, err := s.Postgres.Resolve()
		return err
	}
	return nil
}

// Postgres names a Postgres database by its connection string: inline, or
// in a file the deployment mounts (a Kubernetes Secret key), since the
// string carries the credential. Exactly one of the two is set.
type Postgres struct {
	DSN     string `json:"dsn,omitempty"`
	DSNFile string `json:"dsnFile,omitempty"`
}

// Resolve returns the connection string.
func (p Postgres) Resolve() (string, error) {
	switch {
	case p.DSN != "" && p.DSNFile != "":
		return "", errors.New("config: postgres gives both dsn and dsnFile")
	case p.DSN != "":
		return p.DSN, nil
	case p.DSNFile != "":
		raw, err := os.ReadFile(p.DSNFile)
		if err != nil {
			return "", fmt.Errorf("config: postgres dsn file: %w", err)
		}
		dsn := strings.TrimSpace(string(raw))
		if dsn == "" {
			return "", fmt.Errorf("config: postgres dsn file %s is empty", p.DSNFile)
		}
		return dsn, nil
	default:
		return "", errors.New("config: postgres requires dsn or dsnFile")
	}
}
