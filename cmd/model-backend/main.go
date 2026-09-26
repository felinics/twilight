// Command model-backend is the model backend component (CLD-MDL).
package main

import (
	"context"

	"github.com/felinics/twilight/agent/component/modelbackend"
	"github.com/felinics/twilight/agent/component/run"
	"github.com/felinics/twilight/agent/serve"
)

func main() {
	run.Main("model-backend", func(ctx context.Context, cfg modelbackend.Config) (serve.Component, error) {
		return modelbackend.Compose(ctx, cfg)
	},
		func(cfg modelbackend.Config) string { return cfg.Listen })
}
