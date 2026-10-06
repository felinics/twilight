// Package run is the shared main of the reference agent's binaries: parse
// the -config flag, load the component's document, compose it and serve it
// until a termination signal. Each cmd/* is this function applied to one
// component package.
package run

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/serve"
)

// Main runs one component: name is the binary's name for messages, compose
// builds the component from its document, listen extracts the address. The
// config path is required for service binaries.
func Main[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string) {
	mainWithConfig(name, compose, listen, nil)
}

// MainWithDefaultConfig is like Main, but discovers a config when -config is
// omitted. It is intended for the human-facing local agent, not service
// binaries whose deployment always mounts an explicit document.
func MainWithDefaultConfig[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string, discover func() (string, error)) {
	mainWithConfig(name, compose, listen, discover)
}

func mainWithConfig[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string, discover func() (string, error)) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	path := fs.String("config", "", "path of the component's configuration document")
	_ = fs.Parse(os.Args[1:])
	configPath := *path
	if configPath == "" && discover != nil {
		var err error
		configPath, err = discover()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(2)
		}
	}
	if configPath == "" {
		fmt.Fprintf(os.Stderr, "%s: -config is required\n", name)
		os.Exit(2)
	}
	cfg, err := config.LoadFile[C](configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(2)
	}
	addr := listen(cfg)
	if addr == "" {
		fmt.Fprintf(os.Stderr, "%s: listen address is required\n", name)
		os.Exit(2)
	}
	ctx := context.Background()
	component, err := compose(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: compose: %v\n", name, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "%s: serving on %s\n", name, addr)
	if err := serve.Run(ctx, addr, component, serve.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}
