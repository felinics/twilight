// Command worker is the executor worker component (CLD-EXE-1).
package main

import (
	"context"

	"github.com/felinics/twilight/agent/component/run"
	"github.com/felinics/twilight/agent/component/worker"
	"github.com/felinics/twilight/agent/serve"
)

func main() {
	run.Main("worker", func(ctx context.Context, cfg worker.Config) (serve.Component, error) { return worker.Compose(ctx, cfg) },
		func(cfg worker.Config) string { return cfg.Listen })
}
