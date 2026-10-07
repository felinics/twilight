package ownerservice

import (
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agentcore/run/reconcile"
)

func TestMissingEffectsConfigParsing(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want reconcile.MissingPolicy
	}{
		{name: "omitted is dispose", raw: `{}`, want: reconcile.DisposeMissing},
		{name: "dispose", raw: `{"missingEffects":"dispose"}`, want: reconcile.DisposeMissing},
		{name: "redispatch", raw: `{"missingEffects":"redispatch","maxRedispatches":7,"redispatchRetry":"2s","orphanProbe":"3s"}`, want: reconcile.RedispatchMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.Load[Config](strings.NewReader(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			got, err := cfg.MissingEffects.runtimePolicy()
			if err != nil || got != tc.want {
				t.Fatalf("policy = %s, %v; want %s", got, err, tc.want)
			}
			if tc.want == reconcile.RedispatchMissing {
				if cfg.MaxRedispatches != 7 || cfg.RedispatchRetry.Std() != 2*time.Second || cfg.OrphanProbe.Std() != 3*time.Second {
					t.Fatalf("recovery config = budget %d retry %s probe %s", cfg.MaxRedispatches, cfg.RedispatchRetry.Std(), cfg.OrphanProbe.Std())
				}
			}
		})
	}

	for name, raw := range map[string]string{
		"unknown":      `{"missingEffects":"retry"}`,
		"empty":        `{"missingEffects":""}`,
		"non-string":   `{"missingEffects":1}`,
		"negative gap": `{"redispatchRetry":"-1s"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := config.Load[Config](strings.NewReader(raw)); err == nil {
				t.Fatalf("accepted %s", raw)
			}
		})
	}

	if _, err := (MissingEffectsPolicy("invalid")).runtimePolicy(); err == nil {
		t.Fatal("programmatic invalid missing-effects policy accepted")
	}
	for name, cfg := range map[string]Config{
		"negative budget": {MaxRedispatches: -1},
		"negative retry":  {RedispatchRetry: config.Duration(-time.Second)},
		"negative probe":  {OrphanProbe: config.Duration(-time.Second)},
	} {
		if _, err := cfg.validateRecovery(); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
