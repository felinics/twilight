// Command twilight runs the single-process agent. Unlike the service
// binaries, its Worker, model executor and workspace sandbox are colocated
// and only the application command face is exposed over HTTP.
package main

import (
	"context"

	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/component/run"
	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/serve"
)

func main() {
	run.MainWithDefaultConfig("twilight", func(ctx context.Context, cfg localagent.FileConfig) (serve.Component, error) {
		return localagent.ComposeFile(ctx, cfg)
	}, func(cfg localagent.FileConfig) string {
		return cfg.ListenAddr()
	}, config.DiscoverLocalConfig)
}
