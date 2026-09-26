// Package frozen is the frozen-body protocol of a Run (RUN-WIR-4). Facts name
// the model request, model result, tool output and external tool response by
// digest only; Store is the port to the content-addressed side store that holds
// the bodies, and Codec stores each body as the typed envelope its digest was
// computed from, so sha256(bytes) == digest and the body is addressable by its
// own name.
package frozen
