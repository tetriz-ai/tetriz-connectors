// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package cli is the tetriz-connectors command, importable: Main runs it
// with the arguments, the output streams and the app options a program
// passes. A binary built from Main with app.WithConnector options has every
// command of tetriz-connectors, and its `serve` and `sync` run the added
// connectors too. cmd/tetriz-connectors is Main with no options.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"

	"github.com/google/uuid"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	simserver "github.com/tetriz-ai/tetriz-connectors/internal/simulator/server"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Version is the version `version` prints. Empty reads the module version
// from the build info, or "dev". cmd/tetriz-connectors sets it from its
// main.version, which release builds set with -ldflags "-X main.version=...".
var Version = ""

const usage = `tetriz-connectors runs connector syncs against vendor APIs or the simulator.

Usage:
  tetriz-connectors <command> [flags]

Commands:
  serve        run the scheduler and worker, with /_health and /metrics
  sync         run one operation for one organization now and print the result
  config put   store an organization's connector config
  cursor show  print the stored cursor of one operation
  migrate      apply the connector store's migrations
  simulator    serve the vendor simulator
  connectors   list the connectors, their operations and default schedules
  schema       list the record schemas, or print one as JSON Schema
  version      print the version

Configuration comes from TETRIZ_* environment variables. The repository's
.env.example lists them.
`

// Main runs the command with args (without the program name), writing to
// stdout and stderr, and returns the exit code: 0 on success, 1 on a
// failure and 2 on a usage error. opts reach every app.New it calls, and the
// connector checks of sync, config put and cursor show and the connectors
// listing include the connectors opts add. Main stops on SIGINT or SIGTERM.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, opts ...app.Option) int {
	c := command{opts: opts}
	if len(args) == 0 {
		// No command is a usage error, so the usage goes to stderr.
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	}
	logger.Init(os.Getenv("TETRIZ_ENV"), nil)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch args[0] {
	case "serve":
		err = c.serve(ctx, args[1:], stdout)
	case "sync":
		err = c.sync(ctx, args[1:], stdout)
	case "config":
		if len(args) == 2 && isHelpFlag(args[1]) {
			args = []string{"config", "put", "--help"}
		}
		if len(args) < 2 || args[1] != "put" {
			_, _ = fmt.Fprintln(stderr, "usage: tetriz-connectors config put --org ID --connector ID [--settings JSON] [--credentials JSON] [--disabled]")
			return 2
		}
		err = c.configPut(ctx, args[2:], stdout)
	case "cursor":
		if len(args) == 2 && isHelpFlag(args[1]) {
			args = []string{"cursor", "show", "--help"}
		}
		if len(args) < 2 || args[1] != "show" {
			_, _ = fmt.Fprintln(stderr, "usage: tetriz-connectors cursor show --org ID --connector ID --op ID")
			return 2
		}
		err = c.cursorShow(ctx, args[2:], stdout)
	case "migrate":
		err = cmdMigrate(ctx, args[1:], stdout)
	case "simulator":
		err = cmdSimulator(ctx, args[1:], stdout)
	case "connectors":
		if err = parseFlags(flag.NewFlagSet("connectors", flag.ContinueOnError), "List the connectors, their operations, default schedules and the host services an operation needs.", args[1:], stdout); err == nil {
			err = c.connectors(stdout)
		}
	case "schema":
		err = cmdSchema(args[1:], stdout)
	case "version":
		if err = parseFlags(flag.NewFlagSet("version", flag.ContinueOnError), "Print the version.", args[1:], stdout); err == nil {
			_, _ = fmt.Fprintln(stdout, versionString())
		}
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	if errors.Is(err, errHelp) {
		return 0
	}
	if err != nil {
		var ue usageError
		if errors.As(err, &ue) {
			_, _ = fmt.Fprintln(stderr, err)
			return 2
		}
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

// isHelpFlag reports whether arg asks for help.
func isHelpFlag(arg string) bool { return arg == "-h" || arg == "--help" || arg == "-help" }

// errHelp reports that a command printed its help; the process exits 0.
var errHelp = errors.New("help printed")

// parseFlags parses a subcommand's flags. -h or --help prints the command's
// description and flags to stdout and returns errHelp before any side
// effect; a positional argument is a usage error.
func parseFlags(fs *flag.FlagSet, desc string, args []string, stdout io.Writer) error {
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	err := fs.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		hasFlags := false
		fs.VisitAll(func(*flag.Flag) { hasFlags = true })
		synopsis := fs.Name()
		if hasFlags {
			synopsis += " [flags]"
		}
		_, _ = fmt.Fprintf(stdout, "usage: tetriz-connectors %s\n\n%s\n", synopsis, desc)
		if hasFlags {
			_, _ = fmt.Fprintln(stdout, "\nFlags:")
			fs.SetOutput(stdout)
			fs.PrintDefaults()
		}
		return errHelp
	}
	if err != nil {
		return usageError{err.Error()}
	}
	if fs.NArg() > 0 {
		return usageError{fmt.Sprintf("tetriz-connectors %s: unexpected argument %q", fs.Name(), fs.Arg(0))}
	}
	return nil
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func versionString() string {
	if Version != "" {
		return Version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

func loadGatewayConfig() error {
	if _, err := gateway.LoadConfig(); err != nil {
		return fmt.Errorf("gateway config: %w", err)
	}
	return nil
}

// command is one Main call: the app options it passes on.
type command struct{ opts []app.Option }

func (c command) newApp(ctx context.Context) (*app.App, error) {
	return c.openApp(ctx, app.FromEnv)
}

// newAppWithoutCredentials opens the runner for a command that reads no
// credentials, so it runs without TETRIZ_CREDENTIALS_KEY.
func (c command) newAppWithoutCredentials(ctx context.Context) (*app.App, error) {
	return c.openApp(ctx, app.FromEnvWithoutCredentials)
}

func (c command) openApp(ctx context.Context, fromEnv func() (app.Config, error)) (*app.App, error) {
	cfg, err := fromEnv()
	if err != nil {
		return nil, err
	}
	if err := loadGatewayConfig(); err != nil {
		return nil, err
	}
	return app.New(ctx, cfg, c.opts...)
}

// registry is the registry the command checks ids against and lists.
func (c command) registry() (*sdk.ConnectorRegistry, error) {
	r, err := app.NewRegistryWith(nil, c.opts...)
	if err != nil {
		return nil, fmt.Errorf("connectors: %w", err)
	}
	return r, nil
}

func (c command) serve(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	if err := parseFlags(fs, "Run the scheduler and worker, with /_health and /metrics on TETRIZ_HTTP_ADDR. It needs TETRIZ_POSTGRES_DSN, TETRIZ_REDIS_ADDR and TETRIZ_CREDENTIALS_KEY. The repository's .env.example lists the rest.", args, stdout); err != nil {
		return err
	}
	a, err := c.newApp(ctx)
	if err != nil {
		return err
	}
	defer a.Close() //nolint:errcheck // shutdown path
	return a.Serve(ctx)
}

func (c command) sync(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	org := fs.String("org", "", "organization id, a UUID (required)")
	conn := fs.String("connector", "", "connector id, for example github (required)")
	op := fs.String("op", "", "operation id, for example fetch_prs (required)")
	tenant := fs.String("tenant", "organization", "tenant kind: organization or individual")
	if err := parseFlags(fs, "Run one operation for one organization now and print the run as JSON. Exits 1 when the run fails.", args, stdout); err != nil {
		return err
	}
	if *org == "" || *conn == "" || *op == "" {
		return usageError{"usage: tetriz-connectors sync --org ID --connector ID --op ID [--tenant organization|individual]"}
	}
	if err := c.validateIDs(org, *conn, *op); err != nil {
		return err
	}
	kind := sdk.TenantKindOrganization
	switch *tenant {
	case "organization":
	case "individual":
		kind = sdk.TenantKindIndividual
	default:
		return usageError{fmt.Sprintf("--tenant must be organization or individual, not %q", *tenant)}
	}
	a, err := c.newApp(ctx)
	if err != nil {
		return err
	}
	defer a.Close() //nolint:errcheck // one-shot command
	res, err := a.SyncOnce(ctx, *org, sdk.ConnectorID(*conn), sdk.OperationID(*op), kind)
	if err != nil && res.RunID == "" {
		return err // no run was recorded, so there is nothing to print
	}
	if pErr := printJSON(stdout, res); pErr != nil {
		return pErr
	}
	if res.Status == "failed" {
		return fmt.Errorf("run failed: %s", res.Error)
	}
	return err
}

func (c command) configPut(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("config put", flag.ContinueOnError)
	org := fs.String("org", "", "organization id, a UUID (required)")
	conn := fs.String("connector", "", "connector id, for example github (required)")
	settings := fs.String("settings", "", "settings JSON object; {} on the first put, and kept from the stored config when omitted")
	creds := fs.String("credentials", "", "credentials JSON object; required on the first put, and kept from the stored config when omitted")
	disabled := fs.Bool("disabled", false, "store the config disabled")
	if err := parseFlags(fs, "Store an organization's connector config. A write with --credentials resets the connector's health, so a paused connector runs again. A write without it keeps the stored credentials and health, and a write without --settings keeps the stored settings.", args, stdout); err != nil {
		return err
	}
	if *org == "" || *conn == "" {
		return usageError{"usage: tetriz-connectors config put --org ID --connector ID [--settings JSON] [--credentials JSON] [--disabled]"}
	}
	if err := c.validateIDs(org, *conn, ""); err != nil {
		return err
	}
	settingsGiven := false
	fs.Visit(func(f *flag.Flag) { settingsGiven = settingsGiven || f.Name == "settings" })
	for name, v := range map[string]string{"--settings": *settings, "--credentials": *creds} {
		if v == "" && (name == "--credentials" || !settingsGiven) {
			continue
		}
		if !isJSONObject(v) {
			return usageError{name + " must be a JSON object"}
		}
	}
	var settingsJSON json.RawMessage // nil keeps the stored settings
	if settingsGiven {
		settingsJSON = json.RawMessage(*settings)
	}
	a, err := c.newApp(ctx)
	if err != nil {
		return err
	}
	defer a.Close() //nolint:errcheck // one-shot command
	if *creds == "" {
		err := a.PutSettings(ctx, *org, sdk.ConnectorID(*conn), settingsJSON, !*disabled)
		if errors.Is(err, app.ErrNoStoredConfig) {
			return usageError{fmt.Sprintf("--credentials is required: %s has no stored %s config", *org, *conn)}
		}
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "stored %s config for %s (enabled=%t)\n", *conn, *org, !*disabled)
		return nil
	}
	cfg := &sdk.ConnectorConfig{
		OrganizationID: *org,
		ConnectorID:    sdk.ConnectorID(*conn),
		Credentials:    json.RawMessage(*creds),
		Settings:       settingsJSON,
		Enabled:        !*disabled,
		HealthState:    sdk.HealthStateHealthy,
	}
	if err := a.PutConfig(ctx, cfg); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "stored %s config for %s (enabled=%t)\n", *conn, *org, !*disabled)
	return nil
}

// validateIDs checks the --org, --connector and --op values before any store
// or vendor call: the org must be a UUID, and the connector and the operation
// (when op is not empty) must exist. Each failure is a usage error. It
// rewrites *org to the canonical lower-case UUID the store scopes to.
func (c command) validateIDs(org *string, conn, op string) error {
	u, err := uuid.Parse(*org)
	if err != nil {
		return usageError{fmt.Sprintf("--org must be a UUID, not %q", *org)}
	}
	*org = u.String()
	reg, err := c.registry()
	if err != nil {
		return err
	}
	if _, err := reg.Get(sdk.ConnectorID(conn)); err != nil {
		return usageError{fmt.Sprintf("unknown connector %q; tetriz-connectors connectors lists them", conn)}
	}
	if op == "" {
		return nil
	}
	if _, err := reg.GetOperation(sdk.ConnectorID(conn), sdk.OperationID(op)); err != nil {
		return usageError{fmt.Sprintf("connector %s has no operation %q; tetriz-connectors connectors lists them", conn, op)}
	}
	return nil
}

// isJSONObject reports whether s is a JSON object (not an array, a scalar or
// null).
func isJSONObject(s string) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal([]byte(s), &m) == nil && m != nil
}

func (c command) cursorShow(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("cursor show", flag.ContinueOnError)
	org := fs.String("org", "", "organization id, a UUID (required)")
	conn := fs.String("connector", "", "connector id, for example github (required)")
	op := fs.String("op", "", "operation id, for example fetch_prs (required)")
	if err := parseFlags(fs, "Print the stored cursor of one operation as JSON.", args, stdout); err != nil {
		return err
	}
	if *org == "" || *conn == "" || *op == "" {
		return usageError{"usage: tetriz-connectors cursor show --org ID --connector ID --op ID"}
	}
	if err := c.validateIDs(org, *conn, *op); err != nil {
		return err
	}
	a, err := c.newAppWithoutCredentials(ctx)
	if err != nil {
		return err
	}
	defer a.Close() //nolint:errcheck // one-shot command
	cur, err := a.Cursor(ctx, *org, sdk.ConnectorID(*conn), sdk.OperationID(*op))
	if err != nil {
		return err
	}
	if cur == nil {
		_, _ = fmt.Fprintln(stdout, "no cursor")
		return nil
	}
	return printJSON(stdout, cur)
}

func cmdMigrate(ctx context.Context, args []string, stdout io.Writer) error {
	if err := parseFlags(flag.NewFlagSet("migrate", flag.ContinueOnError), "Apply the connector store's migrations to TETRIZ_POSTGRES_DSN.", args, stdout); err != nil {
		return err
	}
	cfg, err := app.FromEnvWithoutCredentials()
	if err != nil {
		return err
	}
	applied, err := app.Migrate(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(stdout, appliedLine(len(applied)))
	return nil
}

// appliedLine is migrate's report of how many migrations it applied.
func appliedLine(n int) string {
	if n == 1 {
		return "applied 1 migration"
	}
	return fmt.Sprintf("applied %d migrations", n)
}

func cmdSimulator(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("simulator", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address; the simulator is unauthenticated, so bind it to all interfaces (\":8080\") only on a private network")
	seed := fs.Bool("seed", false, "seed the demo catalog before serving")
	if err := parseFlags(fs, "Serve the vendor simulator, which needs no credentials and checks none. TETRIZ_SIMULATOR_DSN names its Postgres database.", args, stdout); err != nil {
		return err
	}
	dsn := os.Getenv("TETRIZ_SIMULATOR_DSN")
	if dsn == "" {
		return usageError{"TETRIZ_SIMULATOR_DSN is required"}
	}
	if err := simserver.Migrate(ctx, dsn); err != nil {
		return err
	}
	if *seed {
		if err := simserver.Seed(ctx, dsn); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(stdout, "simulator: seeded the demo catalog")
	}
	_, _ = fmt.Fprintf(stdout, "simulator: listening on %s\n", *addr)
	return simserver.Serve(ctx, dsn, *addr)
}

func (c command) connectors(stdout io.Writer) error {
	r, err := c.registry()
	if err != nil {
		return err
	}
	provided := app.CollectOptions(c.opts...).HostServices
	conns := r.List()
	sort.Slice(conns, func(i, j int) bool { return conns[i].ID() < conns[j].ID() })
	for _, conn := range conns {
		_, _ = fmt.Fprintf(stdout, "%s\n", conn.ID())
		for _, op := range conn.Operations() {
			sched := op.DefaultSchedule
			if sched == "" {
				sched = "-"
			}
			// serve schedules no op that needs a host service it was not
			// given, and passes no content gate.
			var notes []string
			if missing := provided.Missing(op.RequiresHost); len(missing) > 0 {
				names := make([]string, len(missing))
				for i, svc := range missing {
					names[i] = string(svc)
				}
				notes = append(notes, "needs host: "+strings.Join(names, ", "))
			}
			if op.ID == vendorids.OpFetchFileContent {
				notes = append(notes, "content gate")
			}
			if len(notes) > 0 {
				sched += "  (" + strings.Join(notes, "; ") + ")"
			}
			_, _ = fmt.Fprintf(stdout, "  %-28s %s\n", op.ID, sched)
		}
	}
	return nil
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
