// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	"github.com/tetriz-ai/tetriz-connectors/connectors/anthropic"
	"github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	"github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/connectors/cursor"
	"github.com/tetriz-ai/tetriz-connectors/connectors/github"
	"github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	"github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	"github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
	"github.com/tetriz-ai/tetriz-connectors/internal/store/migrations"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/handler"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	pgsink "github.com/tetriz-ai/tetriz-connectors/sink/postgres"
)

// App is an assembled runner.
type App struct {
	cfg          Config
	sqlDB        *sql.DB
	DB           *gorm.DB
	Redis        *redis.Client
	Registry     *sdk.ConnectorRegistry
	Configs      *store.ConnectorConfigRepo
	States       *store.SyncStateRepo
	Runs         *store.SyncRunRepo
	Schedules    *store.ScheduleRepo
	Orchestrator *runner.Orchestrator
	// Tenants resolves each organization's tenant kind for scheduled runs.
	// New sets a StaticTenantDirectory from the configuration, or the
	// directory WithTenantDirectory gives; a host may also replace it before
	// calling Serve.
	Tenants sdk.TenantDirectory
	// hostServices are the services WithHostServices gave.
	hostServices sdk.HostServices
	// logger is what WithLogger gave; nil keeps the process logger.
	logger *slog.Logger
	// tokens is the gateways' access-token cache, which PutConfig forgets.
	tokens  auth.TokenCache
	closers []io.Closer
}

// GitHubCheckpointReserve is how long before a run's deadline the GitHub
// operations stop taking new items, so the run saves its position instead of
// being cut off mid-page.
const GitHubCheckpointReserve = 30 * time.Second

// githubPagination is the runner's GitHub page sizing. Operations it omits use
// the connector's defaults (25 per page).
func githubPagination() *config.GitHubPaginationConfig {
	return &config.GitHubPaginationConfig{Operations: map[string]*config.OperationPaginationConfig{
		"fetch_issues": {PageSize: 50, MinPageSize: 1, BackoffDivisor: 2},
	}}
}

// NewRegistry returns a registry holding every connector the module ships,
// with the defaults the runner uses.
func NewRegistry(blobs sdk.BlobStore) *sdk.ConnectorRegistry {
	r := sdk.NewConnectorRegistry()
	r.Register(github.NewConnector(githubPagination(), GitHubCheckpointReserve))
	r.Register(gitlab.NewConnector())
	r.Register(bitbucket.NewConnector(nil))
	r.Register(jira.NewConnector(nil))
	r.Register(linear.NewConnector())
	r.Register(cursor.NewConnector())
	r.Register(anthropic.NewConnector())
	r.Register(codex.NewConnector())
	r.Register(kiro.NewConnector())
	return r
}

// Migrate applies the connector store's migrations to dsn.
func Migrate(ctx context.Context, dsn string) ([]int64, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close() //nolint:errcheck // best-effort close after migrating
	applied, err := migrate.Up(ctx, db, migrations.FS)
	if err != nil {
		return applied, fmt.Errorf("migrate connector store: %w", err)
	}
	return applied, nil
}

// New connects to Postgres and Redis and assembles the runner. It applies
// the store migrations first. With no options it is the runner `serve` and
// `sync` run; opts add connectors and their gateways, host services, a
// publisher, a tenant directory or a gateway wrapper (see Option).
func New(ctx context.Context, cfg Config, opts ...Option) (*App, error) {
	o := CollectOptions(opts...)
	if err := validateNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	redisOpts, err := cfg.RedisOptions()
	if err != nil {
		return nil, err
	}
	blobs := blobstore.Dir{Root: cfg.BlobDir}
	registry, err := o.registry(blobs)
	if err != nil {
		return nil, err
	}
	if _, err := Migrate(ctx, cfg.PostgresDSN); err != nil {
		return nil, err
	}
	tenants := o.Tenants
	if tenants == nil {
		if tenants, err = NewTenantDirectory(cfg); err != nil {
			return nil, err
		}
	}
	a := &App{cfg: cfg, Tenants: tenants, Registry: registry, hostServices: o.HostServices, logger: o.Logger}
	sqlDB, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	a.sqlDB = sqlDB
	a.closers = append(a.closers, sqlDB)
	a.DB, err = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		_ = a.Close()
		return nil, fmt.Errorf("open gorm: %w", err)
	}
	a.Redis = redis.NewClient(redisOpts)
	a.closers = append(a.closers, a.Redis)

	var enc cryptutil.Encryptor
	if cfg.withoutCredentials {
		enc = noCredentialsEncryptor{}
	} else if cfg.CredentialsKey != "" {
		key, err := base64.StdEncoding.DecodeString(cfg.CredentialsKey)
		if err != nil {
			_ = a.Close()
			return nil, fmt.Errorf("credentials key: %w", err)
		}
		if enc, err = cryptutil.NewAESGCMEncryptor(key); err != nil {
			_ = a.Close()
			return nil, fmt.Errorf("credentials key: %w", err)
		}
	}

	a.Configs = store.NewConnectorConfigRepo(a.DB, enc)
	a.States = store.NewSyncStateRepo(a.DB)
	a.Runs = store.NewSyncRunRepo(a.DB)
	a.Schedules = store.NewScheduleRepo(a.DB)

	dataSink, err := a.newSink(ctx, o.Publisher)
	if err != nil {
		_ = a.Close()
		return nil, err
	}

	deps := RedisGatewayDeps(cfg, a.Redis, a.Configs, store.PostgresTxRunner{})
	deps.Encryptor = enc
	a.tokens = deps.Refresh.Cache
	builtin, err := NewGatewayFactory(deps)
	if err != nil {
		_ = a.Close()
		return nil, err
	}
	a.Orchestrator = runner.NewOrchestrator(a.Registry, a.Configs, a.States, a.Runs, dataSink, o.gatewayFactory(builtin)).
		WithBlobStore(blobs).
		WithBudgetObservers(deps.budgetObservers()).
		WithHostServices(o.HostServices).
		WithLogger(o.Logger).
		WithFetchCache(store.NewFetchCacheRepo(a.DB), func() int { return 256 * 1024 }).
		WithSimulatedOrgs(isSimulatorOrg)
	return a, nil
}

// isSimulatorOrg reports whether the gateway config routes orgID to the
// simulator, the same decision the gateway factory makes.
func isSimulatorOrg(orgID string) bool {
	cfg, err := gateway.LoadConfig()
	return err == nil && cfg.IsSimulatorOrg(orgID)
}

// ScheduleSource is the schedule source Serve runs: every enabled operation
// of every stored config, stamped with the tenant kind a.Tenants resolves.
// The source withholds every operation whose RequiresHost names a host
// service (sdk.HostServices) WithHostServices did not give, which without
// options is every one of them, and fetch_file_content stays behind its
// content gate, which New leaves closed.
func (a *App) ScheduleSource() *connectorsource.ConnectorScheduleSource {
	return connectorsource.NewConnectorScheduleSource(a.Configs, a.Schedules, a.Registry, a.Tenants, nil, nil).
		WithHostServices(a.hostServices.Provided()...)
}

// reconcileAbandonedRuns closes, every interval until ctx ends, the runs of
// every organization whose lease expired: a process that ran them was killed
// or lost the database before it could close them.
func (a *App) reconcileAbandonedRuns(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		a.closeAbandonedRuns(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// closeAbandonedRuns is one reconciler pass, in the system scope.
func (a *App) closeAbandonedRuns(ctx context.Context) {
	sctx, done := sourceScope(ctx)
	n, err := a.Runs.CloseAbandoned(sctx, "", "", "")
	if err = done(err); err != nil {
		logger.Error(ctx, "close abandoned runs", zap.Error(err))
		return
	}
	if n > 0 {
		logger.WarnWithCtx(ctx, "closed abandoned runs whose lease expired", zap.Int64("runs", n))
	}
}

// sourceScope runs each schedule read in the system scope: the connector
// source lists every organization's configs, which forced row-level security
// hides from an unscoped read.
func sourceScope(ctx context.Context) (context.Context, func(error) error) {
	sctx := tenantdb.NewContext(ctx, tenantdb.SystemTenantID, tenantdb.ScopeSystem)
	return sctx, func(err error) error { return tenantdb.CommitOrRollback(sctx, err) }
}

// NewTenantDirectory builds the runner's tenant directory from cfg: every
// organization has cfg.DefaultTenantKind (organization when unset) unless
// cfg.TenantKindsFile names it. It also publishes the individual
// organizations the file names, so the schedule source skips their
// multi-user operations and their metrics use the individual label.
func NewTenantDirectory(cfg Config) (StaticTenantDirectory, error) {
	d := StaticTenantDirectory{Default: cfg.DefaultTenantKind}
	if d.Default == sdk.TenantKindUnspecified {
		d.Default = sdk.TenantKindOrganization
	}
	if cfg.TenantKindsFile != "" {
		kinds, err := LoadTenantKinds(cfg.TenantKindsFile)
		if err != nil {
			return d, err
		}
		d.Kinds = kinds
	}
	if ind := d.Individuals(); len(ind) > 0 {
		tenantlabel.SetLiveIndividualSet(ind)
	}
	return d, nil
}

func (a *App) newSink(ctx context.Context, p sink.Publisher) (sdk.DataSink, error) {
	if p != nil {
		return sink.NewConnectorSink(p), nil
	}
	switch a.cfg.SinkPath {
	case "", "log":
		return sink.NewLoggingSink(nil), nil
	case "-":
		return sink.NewConnectorSink(sink.NewJSONLPublisher(os.Stdout)), nil
	case SinkPostgres:
		pg, err := pgsink.Open(ctx, a.cfg.sinkPostgresDSN())
		if err != nil {
			return nil, err
		}
		a.closers = append(a.closers, pg)
		return sink.NewConnectorSink(pg), nil
	default:
		f, err := os.OpenFile(a.cfg.SinkPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open sink file: %w", err)
		}
		a.closers = append(a.closers, f)
		return sink.NewConnectorSink(sink.NewJSONLPublisher(f)), nil
	}
}

// Close releases the database, Redis and the sink file or database.
func (a *App) Close() error {
	var errs []error
	for i := len(a.closers) - 1; i >= 0; i-- {
		if err := a.closers[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Serve runs the worker on every call and the scheduler under the leader
// lock, and serves /_health and /metrics, until ctx is cancelled.
func (a *App) Serve(ctx context.Context) error {
	ctx = logger.NewContext(ctx, a.logger)
	reg := prometheus.NewRegistry()
	schedulerMetrics, err := registerServeMetrics(reg, a.cfg.MetricsPrefix)
	if err != nil {
		return fmt.Errorf("register metrics: %w", err)
	}
	mux := asynq.NewServeMux()
	mux.Use(handler.ContextMiddleware(handler.WithTraceID()))
	mux.Use(handler.LoggingMiddleware(logger.Log))
	mux.Use(handler.RecoveryMiddleware(logger.Log))
	mux.Handle(schedule.JobTypeConnectorSync, connectorsource.NewConnectorSyncHandler(a.Orchestrator))

	source := a.ScheduleSource()
	s, err := schedule.NewScheduler(ctx, schedule.Options{
		Namespace:   a.cfg.Namespace,
		Concurrency: a.cfg.Concurrency,
		SourceScope: sourceScope,
		Metrics:     schedulerMetrics,
	}, a.Redis, []schedule.ScheduleSource{source}, mux, nil, schedule.NewDistLockFactory(a.Redis))
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}
	defer func() { _ = s.Stop(context.Background()) }()
	go a.reconcileAbandonedRuns(ctx, runner.DefaultRunLease)

	h := http.NewServeMux()
	h.HandleFunc("/_health", func(w http.ResponseWriter, r *http.Request) {
		if err := a.sqlDB.PingContext(r.Context()); err != nil {
			http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if err := a.Redis.Ping(r.Context()).Err(); err != nil {
			http.Error(w, "redis: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	h.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ln, err := net.Listen("tcp", a.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info(ctx, "serving /_health and /metrics", zap.String("addr", ln.Addr().String()))
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http server: %w", err)
	}
}

// errNoCredentialsKey is returned by every credential read or write of an App
// built from FromEnvWithoutCredentials.
var errNoCredentialsKey = errors.New("this command opens the store without TETRIZ_CREDENTIALS_KEY and reads no credentials")

// noCredentialsEncryptor refuses every credential read and write, so a store
// opened without the key cannot store credentials in the clear.
type noCredentialsEncryptor struct{}

func (noCredentialsEncryptor) Encrypt([]byte) ([]byte, error) { return nil, errNoCredentialsKey }
func (noCredentialsEncryptor) Decrypt([]byte) ([]byte, error) { return nil, errNoCredentialsKey }
func (noCredentialsEncryptor) KeyID() string                  { return "" }
