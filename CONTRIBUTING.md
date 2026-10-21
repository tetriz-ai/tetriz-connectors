# Contributing

## CI

There is no hosted CI. `scripts/ci.sh` runs every check on your machine: the tests on Go
1.26.0, the latest 1.26 and the latest 1.27, lint, the vulnerability check, the examples,
generated code and licenses, the image, a compose smoke run and the Helm chart. Run it before
every push and push only when it ends with `CI passed`. `scripts/ci.sh test lint` runs only the
jobs named. It starts its own Postgres, Redis and compose stack (project `tetriz-ci`) with
docker or podman and removes them afterwards.

`scripts/install-hooks.sh` installs a git pre-push hook that runs it. `git push --no-verify`
skips the hook once, and `CI_JOBS="test lint" git push` runs a subset.

## Build and test

You need Go 1.26 or newer and, for the store tests, Postgres.

```sh
docker compose up -d postgres
go build ./...
go vet ./...
TETRIZ_REQUIRE_TEST_DB=1 go test -race -count=1 ./...
```

`go test -tags=stress -run TestRateLimitStormSimulation ./connectors/bitbucket/` runs the Bitbucket rate-limit stress simulation, as `scripts/ci.sh` does.

The store tests create and drop their own databases through `TETRIZ_TEST_POSTGRES_DSN`
(default: the compose Postgres on `127.0.0.1:55432`). Without a database they skip, unless
`TETRIZ_REQUIRE_TEST_DB=1` makes them fail.

Every test runs under a guard that points `HOME` at an empty temporary directory and makes AWS
credential loading panic, so no test can reach a real credential. The guard also refuses any
HTTP request through the default transport to a host that is not loopback (`localhost`,
127.0.0.0/8, ::1), before any name is resolved, and fails the package that made one. Tests never
call a vendor; point them at an httptest server, the simulator or `testkit/fakevendor`. A test
database DSN must name a loopback host or a local socket; set `TETRIZ_TEST_ALLOW_REMOTE_DB=1` to
run the store tests against a remote one.

On macOS with Podman, the virtual machine's clock drifts from the host's, and two store tests
compare a database timestamp with an application one. Resync the clock before a test run:

```sh
podman machine ssh "sudo date -u -s @$(python3 -c 'import time;print(repr(time.time()))')"
```

## Lint

`scripts/ci.sh` pins golangci-lint v2.14.0, run on Go 1.27:

```sh
golangci-lint run ./...
```

## Examples

Each example's README holds one command and its output. `scripts/run-examples.sh` runs every one and compares the output:

```sh
scripts/run-examples.sh
```

## Migrations

The store's migrations are in `internal/store/migrations` and the simulator's in
`internal/simulator/migrations`. Until v0.1.0 is tagged, 000001 holds the whole schema and may change; after pulling a change to it, reset a local stack with `docker compose down -v`. From v0.1.0 on, a schema change is a new numbered migration, and a released migration is never edited.

`migrate` (and `serve` at start) records each migration's SHA-256 in the version table
(`schema_migrations` for the store, `simulator_schema_migrations` for the simulator) and
checks it on every run. An applied migration whose file changed is refused, naming the
file. A row applied before checksums existed gets its checksum filled in.

## Golden files

The command's help, `connectors` and `schema` output are compared with
`cmd/tetriz-connectors/testdata`. After changing any of them, rewrite them and review the diff:

```sh
go test ./cmd/tetriz-connectors/ -update
```

## Record schemas

Every operation that emits records has a schema in `schemas/<connector>/<operation>.schema.json`
and a section on its connector's page in `docs/records`. A change to a payload, an external id,
a data date or a metadata key updates both in the same pull request. `go test ./schemas/` fails
when an operation has no schema or a simulated record does not match its schema.

## Helm chart

`scripts/ci.sh helm` lints and renders the chart in `deploy/helm/tetriz-connectors`, then validates the output with
kubeconform. With `helm` and `kubeconform` installed, run the same lines locally:

```sh
CHART=deploy/helm/tetriz-connectors
required=(--set config.redisAddr=valkey:6379 --set secrets.postgresDsn=dsn --set secrets.credentialsKey=key)
helm lint --strict "$CHART" "${required[@]}"
helm lint --strict "$CHART" -f "$CHART/values-demo.yaml"
helm lint --strict "$CHART" -f "$CHART/ci/full-values.yaml"
```

`scripts/ci.sh` pins helm v4.3.0 and kubeconform v0.8.0. A new value goes in `values.yaml` with a comment,
and in the chart README's Values table when users set it often.

## Generated code

`gen/` and `internal/gen/` are generated from `proto/` with the plugin versions pinned in
`buf.gen.yaml`, the mocks under `internal/mocks` by `go tool mockgen`, and every source file
carries a license header. After changing a proto or an interface with a `//go:generate mockgen`
line, run `scripts/check-generated.sh`. It needs buf 1.71.0 and go-licenses v1.6.0, the versions
`scripts/ci.sh` pins. `scripts/ci.sh` runs it too and fails on any difference. It also checks `third_party/licenses` and
NOTICE's module list, which `scripts/third-party-licenses.sh` regenerates after a dependency
change.

## Pull requests

- Every pull request needs a test that fails without the change.
- A change to documented behaviour, or to a limitation listed in the README, updates the README
  and its pinning test in the same pull request.
- Commit messages are short imperative sentences ("Add a Linear projects operation").

## Licensing of contributions

No sign-off line and no separately signed agreement are needed. By opening a pull request you
agree that your contribution is released under the project's [LICENSE](LICENSE).
