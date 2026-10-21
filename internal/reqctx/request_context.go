// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package reqctx carries per-request values through a context.Context: the
// trace id, and the organization, person, connector and operation ids that
// logging and rate-limit keys read.
package reqctx

import (
	"context"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

type (
	ctxTraceIDKey     string
	ctxOrgIDKey       string
	ctxPersonIDKey    string
	ctxConnectorIDKey string
	ctxOperationIDKey string

	// ValueGetter interface for getting the values from a context
	ValueGetter interface {
		Value(key any) any
	}
)

const (
	traceIDKey                          = "trace_id"
	CtxTraceKey       ctxTraceIDKey     = traceIDKey
	CtxOrgIDKey       ctxOrgIDKey       = "org_id"
	CtxPersonIDKey    ctxPersonIDKey    = "person_id"
	CtxConnectorIDKey ctxConnectorIDKey = "connector_id"
	CtxOperationIDKey ctxOperationIDKey = "operation_id"

	UnknownID = "UNKNOWN_ID"
)

// WithTraceID checks the provided metadata for trace id first.
// If not found in the metadata, it will be taken from current ctx.
// If not found in the current context, a new trace id is generated.
// The trace id is set in current ctx and on outgoing metadata.
func WithTraceID(ctx context.Context, md metadata.MD) context.Context {
	traceID := ""
	if val := md.Get(traceIDKey); len(val) == 0 {
		traceID = getTraceID(ctx)
		ctx = context.WithValue(ctx, CtxTraceKey, traceID)
	} else {
		traceID = val[0]
		ctx = context.WithValue(ctx, CtxTraceKey, traceID)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, traceIDKey, traceID)
	return ctx
}

// TraceIDFromContext returns the trace-id from context, or "UNKNOWN_ID" if not set.
func TraceIDFromContext[T ValueGetter](ctx T) string {
	id := fromContextOrGRPCMetadata(ctx, traceIDKey, CtxTraceKey)
	if idStr, ok := id.(string); ok {
		return idStr
	}
	return UnknownID
}

// CtxWithTraceID injects given traceID in the ctx.
func CtxWithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == UnknownID || traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, CtxTraceKey, traceID)
}

// WithOrgID stores the organization ID in the context.
func WithOrgID(ctx context.Context, orgID string) context.Context {
	return context.WithValue(ctx, CtxOrgIDKey, orgID)
}

// OrgIDFromContext returns the organization ID from the context, or "" if not set.
func OrgIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxOrgIDKey).(string); ok {
		return v
	}
	return ""
}

// PersonIDFromContext returns the person ID from the context, or "" if not set.
func PersonIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxPersonIDKey).(string); ok {
		return v
	}
	return ""
}

// WithConnectorID stores the connector ID in the context.
func WithConnectorID(ctx context.Context, connectorID string) context.Context {
	return context.WithValue(ctx, CtxConnectorIDKey, connectorID)
}

// ConnectorIDFromContext returns the connector ID from the context, or "" if not set.
func ConnectorIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxConnectorIDKey).(string); ok {
		return v
	}
	return ""
}

// WithOperationID stores the operation ID in the context.
func WithOperationID(ctx context.Context, operationID string) context.Context {
	return context.WithValue(ctx, CtxOperationIDKey, operationID)
}

// OperationIDFromContext returns the operation ID from the context, or "" if not set.
func OperationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxOperationIDKey).(string); ok {
		return v
	}
	return ""
}

func getTraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}

	if ctx != nil {
		if traceID, ok := ctx.Value(CtxTraceKey).(string); ok {
			return traceID
		}
	}

	return uuid.New().String()
}

// fromContextMetadata looks up the gRPC metadata in the provided Context for the provided
// key and returns the array of values if present. Returns nil otherwise.
func fromContextMetadata(ctx context.Context, key string) []string {
	return metadata.ValueFromIncomingContext(ctx, key)
}

// fromContext looks up the Context for the provided key and returns the value if present. Returns nil otherwise.
func fromContext[T ValueGetter](ctx T, key interface{}) interface{} {
	if ValueGetter(ctx) != nil && ctx.Value(key) != nil {
		return ctx.Value(key)
	}
	return nil
}

// fromContextOrGRPCMetadata looks up for the provided key in metadata and then in the context.
func fromContextOrGRPCMetadata[T ValueGetter](ctx T, metadataKey string, ctxKey interface{}) interface{} {
	if goCtx, ok := ValueGetter(ctx).(context.Context); ok {
		if values := fromContextMetadata(goCtx, metadataKey); len(values) != 0 {
			return values[0]
		}
	}

	return fromContext(ctx, ctxKey)
}
