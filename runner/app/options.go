// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// GatewayBuilder builds the gateway of one run of a connector the host adds
// with WithConnector, from the organization's decrypted config. It is the
// sdk.VendorGatewayFactory of that one connector.
type GatewayBuilder func(ctx context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error)

// Option changes what New assembles. The zero set of options is the
// runner `serve` and `sync` run: the built-in connectors and gateways, no
// host services, the sink Config.SinkPath names and a StaticTenantDirectory
// from the configuration.
type Option func(*Options)

// Options is what the Option values passed to New and NewRegistryWith add up
// to. Build it with the With functions; the zero value changes nothing.
type Options struct {
	// Connectors are added to the built-in ones, in order, each with the
	// gateway its runs get.
	Connectors []CustomConnector
	// HostServices are wired into the orchestrator, and the schedule source
	// schedules the operations that need only these services.
	HostServices sdk.HostServices
	// Publisher, when set, receives every record instead of the sink
	// Config.SinkPath names. New does not close it.
	Publisher sink.Publisher
	// Tenants, when set, replaces the StaticTenantDirectory New builds from
	// the configuration, and TETRIZ_DEFAULT_TENANT_KIND and
	// TETRIZ_TENANT_KINDS_FILE are not read.
	Tenants sdk.TenantDirectory
	// WrapGateways, when set, wraps the gateway factory New builds (the
	// built-in gateways and the added connectors' builders), for example to
	// log or decorate every gateway.
	WrapGateways func(sdk.VendorGatewayFactory) sdk.VendorGatewayFactory
	// Logger, when set, receives every entry the runs, the scheduler and the
	// HTTP server log with a context. Nil keeps the process logger.
	Logger *slog.Logger
}

// CustomConnector is one connector added with WithConnector.
type CustomConnector struct {
	Connector sdk.Connector
	Gateway   GatewayBuilder
}

// WithConnector adds a connector of the host's own, with the builder of the
// gateway its runs get. Its id must not be a built-in connector's or another
// added one's: New and NewRegistryWith fail on a collision. `serve` schedules
// its pull operations like the built-in ones, and `sync`, `config put` and
// `cursor show` accept its id.
func WithConnector(c sdk.Connector, gateway GatewayBuilder) Option {
	return func(o *Options) {
		o.Connectors = append(o.Connectors, CustomConnector{Connector: c, Gateway: gateway})
	}
}

// WithLogger sends the entries the App's runs, scheduler and HTTP server log
// with a context to l, instead of the process logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *Options) { o.Logger = l }
}

// WithHostServices wires the host services into the orchestrator, so the
// operations that need them run, and tells the schedule source which ones are
// provided, so `serve` schedules those operations. A later call replaces an
// earlier one.
func WithHostServices(h sdk.HostServices) Option {
	return func(o *Options) { o.HostServices = h }
}

// WithPublisher sends every record to p, through sink.NewConnectorSink,
// instead of the sink Config.SinkPath names. The caller closes p.
func WithPublisher(p sink.Publisher) Option {
	return func(o *Options) { o.Publisher = p }
}

// WithTenantDirectory resolves each organization's tenant kind with d
// instead of the configuration's StaticTenantDirectory.
func WithTenantDirectory(d sdk.TenantDirectory) Option {
	return func(o *Options) { o.Tenants = d }
}

// WithGatewayWrapper wraps the gateway factory New builds. Several wrappers
// apply in order, the last one outermost.
func WithGatewayWrapper(wrap func(sdk.VendorGatewayFactory) sdk.VendorGatewayFactory) Option {
	return func(o *Options) {
		prev := o.WrapGateways
		if prev == nil {
			o.WrapGateways = wrap
			return
		}
		o.WrapGateways = func(f sdk.VendorGatewayFactory) sdk.VendorGatewayFactory { return wrap(prev(f)) }
	}
}

// CollectOptions returns what opts add up to.
func CollectOptions(opts ...Option) Options {
	var o Options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// ErrConnectorCollision reports a connector added with WithConnector whose id
// is already registered.
var ErrConnectorCollision = errors.New("connector id already registered")

// NewRegistryWith returns NewRegistry(blobs) with the connectors opts add.
// It fails when an added connector has no gateway builder, or its id is a
// built-in connector's or another added one's.
func NewRegistryWith(blobs sdk.BlobStore, opts ...Option) (*sdk.ConnectorRegistry, error) {
	return CollectOptions(opts...).registry(blobs)
}

func (o Options) registry(blobs sdk.BlobStore) (*sdk.ConnectorRegistry, error) {
	r := NewRegistry(blobs)
	for _, c := range o.Connectors {
		if c.Connector == nil {
			return nil, errors.New("WithConnector: nil connector")
		}
		id := c.Connector.ID()
		if _, err := r.Get(id); err == nil {
			return nil, fmt.Errorf("WithConnector %q: %w", id, ErrConnectorCollision)
		}
		if c.Gateway == nil {
			return nil, fmt.Errorf("WithConnector %q: no gateway builder", id)
		}
		r.Register(c.Connector)
	}
	return r, nil
}

// gatewayFactory routes each added connector to its builder and every other
// one to builtin, then applies WrapGateways.
func (o Options) gatewayFactory(builtin sdk.VendorGatewayFactory) sdk.VendorGatewayFactory {
	f := builtin
	if len(o.Connectors) > 0 {
		custom := make(map[sdk.ConnectorID]GatewayBuilder, len(o.Connectors))
		for _, c := range o.Connectors {
			custom[c.Connector.ID()] = c.Gateway
		}
		f = func(ctx context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
			if b, ok := custom[cc.ConnectorID]; ok {
				return b(ctx, cc)
			}
			return builtin(ctx, cc)
		}
	}
	if o.WrapGateways != nil {
		f = o.WrapGateways(f)
	}
	return f
}
