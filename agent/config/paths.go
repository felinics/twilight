package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Paths is the local agent directory layout. With no XDG variables set all
// paths intentionally share ~/.twilight, like Pi's ~/.pi/agent. When XDG is
// configured, configuration and mutable state are split into their standard
// XDG homes.
type Paths struct {
	ConfigDir string
	StateDir  string
	Models    string
	Secrets   string
}

// UserPaths resolves Twilight's user directories without creating them.
func UserPaths() (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, fmt.Errorf("config: home directory: %w", err)
	}

	if override := os.Getenv("TWILIGHT_HOME"); override != "" {
		if !filepath.IsAbs(override) {
			return Paths{}, errors.New("config: TWILIGHT_HOME must be an absolute path")
		}
		return pathsAt(override), nil
	}

	configSet := os.Getenv("XDG_CONFIG_HOME") != ""
	stateSet := os.Getenv("XDG_STATE_HOME") != ""
	if !configSet && !stateSet {
		return pathsAt(filepath.Join(home, ".twilight")), nil
	}

	configHome, err := xdgHome("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	if err != nil {
		return Paths{}, err
	}
	stateHome, err := xdgHome("XDG_STATE_HOME", filepath.Join(home, ".local", "state"))
	if err != nil {
		return Paths{}, err
	}
	return Paths{
		ConfigDir: filepath.Join(configHome, "twilight"),
		StateDir:  filepath.Join(stateHome, "twilight"),
		Models:    filepath.Join(configHome, "twilight", "models.json"),
		Secrets:   filepath.Join(configHome, "twilight", "secrets"),
	}, nil
}

func pathsAt(home string) Paths {
	return Paths{
		ConfigDir: home,
		StateDir:  home,
		Models:    filepath.Join(home, "models.json"),
		Secrets:   filepath.Join(home, "secrets"),
	}
}

// DiscoverLocalConfig returns the default local-agent config. settings.json
// follows Pi's convention; config.json remains the explicit/deployment name.
func DiscoverLocalConfig() (string, error) {
	paths, err := UserPaths()
	if err != nil {
		return "", err
	}
	candidates := []string{
		filepath.Join(paths.ConfigDir, "settings.json"),
		filepath.Join(paths.ConfigDir, "config.json"),
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("config: inspect %s: %w", path, err)
		}
	}
	return "", fmt.Errorf("config: no local config found; tried %s and %s", candidates[0], candidates[1])
}

func xdgHome(name, fallback string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	if !filepath.IsAbs(value) {
		return "", fmt.Errorf("config: %s must be an absolute path", name)
	}
	return value, nil
}
