// Command tool-backend is the tool sandbox backend component (CLD-TOL-1).
package main

import (
	"context"

	"github.com/felinics/twilight/agent/component/run"
	"github.com/felinics/twilight/agent/component/toolbackend"
	"github.com/felinics/twilight/agent/serve"
)

func main() {
	run.Main("tool-backend", func(ctx context.Context, cfg toolbackend.Config) (serve.Component, error) {
		return toolbackend.Compose(ctx, cfg)
	},
		func(cfg toolbackend.Config) string { return cfg.Listen })
}
