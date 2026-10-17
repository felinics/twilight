package turn_test

import (
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
)

// inputDigest names an input body in tests: the Turn stores only the digest,
// so any content-derived digest stands in for the chatlog's.
func inputDigest(raw string) turn.Digest { return jsonstable.DigestBytes([]byte(raw)) }
