# tetriz-connectors

## What this is

A connector runner: `serve` schedules connector operations and runs them, and `sync` runs one
now, writing records to a sink and cursors, runs and health to Postgres. Diffs, file contents
and defect lineage need host services (`sdk.HostServices`) that only an embedding host provides.

## Layout

- `cmd/tetriz-connectors`: the command's `main`. Help, `connectors` and `schema` output are golden files in `testdata/`.
- `cli`: the command itself, importable as `cli.Main`, so a binary can add connectors and host services.
- `sdk`: the contract a connector implements. `testkit`: conformance suite and `fakevendor`.
- `connectors/<vendor>`: one package per connector, with fixture tests and `limitations_test.go`.
- `runner`: the orchestrator. `runner/schedule`: scheduler and schedule source. `runner/store`: Postgres store. `runner/app`: what `serve` and `sync` wire.
- `gateway`: vendor endpoints, rate limits and token refresh. `auth`: connect flows and OAuth.
- `internal`: everything that is not public API, such as `internal/gateway/<vendor>`, the mocks and the simulator.
- `connectorsvc`: the gRPC secret broker. `sink`, `blobstore`: where records and large bodies go. `sink/postgres`: the `connector_records` table.
- `gateway/ratelimiter/inprocess`: the gateways' Redis stores held in process memory, for embedding without Redis.
- `schemas/<connector>/<op>.schema.json`: one JSON Schema per record operation. `docs/records`: one page per connector.
- `internal/simulator`: vendor-shaped APIs for GitHub, GitLab, Bitbucket and Jira, with a seeded catalog.
- `examples/heartbeat`, `examples/customconnector`, `examples/embedgithub`: a scheduler job, a connector added to `cli.Main`, and the GitHub connector embedded without Redis. Each README holds one `sh` command and its output, and `scripts/run-examples.sh` runs the command and compares, with `go test` durations as `<time>`. A new example is listed in `examples/README.md`.
- `deploy/helm/tetriz-connectors`: the Helm chart. `deploy/gateway.demo.yaml`: the simulator routing the quick start uses.
- `proto`, `gen` and `internal/gen`: protobuf sources and generated code. `docs/adr`: design records.
- Tests sit beside the code they cover.

## Commands

The floor is Go 1.26.0. There is no hosted CI: `scripts/ci.sh` is the CI, run before every push (`scripts/install-hooks.sh` adds a pre-push hook). It tests 1.26.0, the latest 1.26 and 1.27, and lints with golangci-lint v2.14.0 on Go 1.27.

```sh
docker compose up -d postgres
go build ./...
go vet ./...
TETRIZ_REQUIRE_TEST_DB=1 go test -race -count=1 ./...
golangci-lint run ./...
scripts/check-generated.sh
scripts/run-examples.sh
```

`go test -tags=stress -run TestRateLimitStormSimulation ./connectors/bitbucket/` runs the Bitbucket rate-limit stress simulation, as `scripts/ci.sh` does.

`scripts/check-generated.sh` needs `buf` and `go-licenses` v1.6.0. With the compose stack up
(`docker compose up --build --wait`), `./scripts/acceptance.sh` runs the acceptance sync. `scripts/ci.sh helm` lints the Helm chart with helm v4.3.0 and validates it with kubeconform v0.8.0. CONTRIBUTING.md
has the `helm lint` lines to run after changing `deploy/helm`.

## Contract to keep

- No test reaches a real credential or vendor: a package's `TestMain` calls `testguard.Run`
  (`internal/testguard/testguard_test.go`), which refuses non-loopback HTTP through the default
  transport and fails the package that tried, and refuses a non-loopback test database unless
  `TETRIZ_TEST_ALLOW_REMOTE_DB=1`. A new test package adds a `guard_test.go` like the others.
- `sync` exits 1 when the run fails (`cmd/tetriz-connectors/sync_exit_test.go`, `TestSyncExitCodes`).
- Each README "Before you rely on it" row, and each row of `internal/simulator/README.md` "Limitations",
  is pinned by the test named beside it. Changing that behaviour changes the row and the test together.
- `serve` never schedules `fetch_file_content` or the host-only operations
  (`runner/app/source_scope_test.go`, `TestServeWithholdsHostOnlyOperations`).
- An operation whose `RequiresHost` names a missing service fails terminal `config_invalid` before it runs
  (`runner/host_services_test.go`, `TestOrchestrator_RefusesAnOperationMissingHostServices`). `sync` of one exits 1
  (`cmd/tetriz-connectors/boundary_reemission_test.go`, `TestSync_HostOperationWithoutAHostFails`).
- Every record-emitting operation has a schema (`schemas/completeness_test.go`, `TestEveryRecordOperationHasASchema`),
  and every simulated record matches it (`schemas/simulator_test.go`, `TestSimulatedPayloadsMatchSchemas`).
- Importing the module registers no metrics (`runner/app/metrics_import_test.go`). `serve`'s match `runner/app/testdata/serve-metrics.golden`.
- Before v1, a minor release may break the API, and the changelog marks each break.

## Making a change

- A behaviour change needs a test that fails before the change and passes after.
- A connector case goes in that connector's package. A README limitation goes in its `limitations_test.go`.
- Add a line under `[Unreleased]` in `CHANGELOG.md` for anything a user would notice. Tests and docs need none.
- Fix lint findings. Do not add `//nolint` to silence a new one.
- A payload, external id, data date or metadata change updates its schema and its `docs/records` page in the same change.
- After changing CLI help, `connectors` or `schema` output (a new schema changes `schema.golden`), run `go test ./cmd/tetriz-connectors/ -update` and review the golden diff.
- The README's Go blocks are copies of `*_test.go` files it names, such as `testkit/fakevendor/readme_conformance_test.go`. Change both together.
- After changing a proto, a dependency or an interface with a `//go:generate mockgen` line, run `scripts/check-generated.sh` and commit what it changes.
- Migrations (`internal/store/migrations`, `internal/simulator/migrations`): Until v0.1.0 is tagged, 000001 holds the whole schema and may change; after pulling a change to it, reset a local stack with `docker compose down -v`. From v0.1.0 on, a schema change is a new numbered migration, and a released migration is never edited.

## Do not

- Do not edit `gen/` or `third_party/licenses` by hand. `scripts/check-generated.sh` regenerates them.
- Do not edit NOTICE's toolchain line by hand: `scripts/third-party-licenses.sh` writes it from the Dockerfile's golang stage.
- Do not drop `TETRIZ_REQUIRE_TEST_DB=1`: without it the store tests skip and pass.
- Do not raise the Go floor in `go.mod` without `scripts/ci.sh`'s Go versions and the README.
- On macOS with Podman, resync the VM clock first, as CONTRIBUTING.md shows, or two store tests fail.
