// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reqctx_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// WithTraceID documents that "the trace id is set in current ctx and on
// outgoing metadata". When the id comes from the metadata argument, a reader of
// the returned ctx must see that id, not UNKNOWN_ID.
func TestEdge_WithTraceIDFromMetadataIsReadableFromTheContext(t *testing.T) {
	ctx := reqctx.WithTraceID(context.Background(), metadata.Pairs("trace_id", "from-md"))

	out, _ := metadata.FromOutgoingContext(ctx)
	assert.Equal(t, []string{"from-md"}, out.Get("trace_id"), "outgoing metadata carries it")
	assert.Equal(t, "from-md", reqctx.TraceIDFromContext(ctx), "the current ctx must carry it too")
}

// Without metadata, an id already on the context is reused rather than replaced.
func TestEdge_WithTraceIDKeepsAnExistingContextID(t *testing.T) {
	base := reqctx.CtxWithTraceID(context.Background(), "existing")
	ctx := reqctx.WithTraceID(base, metadata.MD{})
	assert.Equal(t, "existing", reqctx.TraceIDFromContext(ctx))
	// The UNKNOWN_ID sentinel and the empty string are never stored.
	assert.Equal(t, reqctx.UnknownID, reqctx.TraceIDFromContext(reqctx.CtxWithTraceID(context.Background(), reqctx.UnknownID)))
	assert.Equal(t, reqctx.UnknownID, reqctx.TraceIDFromContext(reqctx.CtxWithTraceID(context.Background(), "")))
}
