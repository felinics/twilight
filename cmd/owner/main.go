// Command owner is the owner service component (CLD-OWN-1).
package main

import (
	"context"

	"github.com/felinics/twilight/agent/component/ownerservice"
	"github.com/felinics/twilight/agent/component/run"
	"github.com/felinics/twilight/agent/serve"
)

func main() {
	run.Main("owner", func(ctx context.Context, cfg ownerservice.Config) (serve.Component, error) {
		return ownerservice.Compose(ctx, cfg)
	},
		func(cfg ownerservice.Config) string { return cfg.Listen })
}
