package run_test

import (
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
)

// inputDigest names an input body in tests: the Run stores only the digest,
// so any content-derived digest stands in for the chatlog's.
func inputDigest(raw string) run.Digest { return es.DigestBytes([]byte(raw)) }
