<!-- Copyright 2026 Workloom Solutions Private Limited -->
<!-- SPDX-License-Identifier: MIT -->

# tetriz-connectors Helm chart

This chart runs `tetriz-connectors serve` on Kubernetes. It can also run the vendor simulator for a demo.

## What you provide

The chart bundles no stores. You need:

- A Postgres database. Any operator or chart works, such as CloudNativePG or a managed database.
- A Redis or Valkey. Any chart works, such as the Valkey chart. `secrets.redisPassword` sets its password, and `secrets.redisUrl` takes a `redis://` or `rediss://` (TLS) URL instead.
- A credentials key, from `openssl rand -base64 32`. Keep it stable. Credentials stored under one key do not decrypt under another.

Connect to Postgres as the tables' owner. A superuser or a `BYPASSRLS` role skips row-level security.

## Install

Put the secrets in a Secret you manage. Each key is the variable's name.

```sh
kubectl create secret generic tetriz-secrets \
  --from-literal=TETRIZ_POSTGRES_DSN="host=pg.example port=5432 user=tetriz password=... dbname=tetriz sslmode=require" \
  --from-literal=TETRIZ_CREDENTIALS_KEY="$(openssl rand -base64 32)"
```

Then install the chart:

```sh
helm install tetriz deploy/helm/tetriz-connectors \
  --set config.redisAddr=valkey.example:6379 \
  --set secrets.existingSecret=tetriz-secrets
```

Without `existingSecret`, the chart creates the Secret from `secrets.postgresDsn`, `secrets.credentialsKey` and the other `secrets.*` values. With `existingSecret`, the Secret can hold `TETRIZ_REDIS_URL` in place of `config.redisAddr`. The URL wins at run time.

The install notes print the commands that store a config and run one sync.

## Try the demo

`values-demo.yaml` turns on the simulator and syncs the demo organization with no vendor account. It expects Services named `postgres` and `valkey` in the release's namespace. Change `config.redisAddr` and `secrets.postgresDsn` for yours.

```sh
helm install tetriz deploy/helm/tetriz-connectors -f deploy/helm/tetriz-connectors/values-demo.yaml
kubectl exec deploy/tetriz-tetriz-connectors -- tetriz-connectors config put \
  --org 00000000-0000-4000-8000-00000000d3e0 --connector github \
  --settings '{"organization":"demo-gh-startup"}' --credentials '{}'
kubectl exec deploy/tetriz-tetriz-connectors -- tetriz-connectors sync \
  --org 00000000-0000-4000-8000-00000000d3e0 --connector github --op fetch_repos
```

The demo stores credentials unencrypted. Never use it with real credentials.

## How it runs

- **Replicas.** Several replicas are safe. One holds the leader lock in Redis and enqueues the schedules. Every replica runs queued tasks.
- **Migrations.** `serve` applies the store's migrations at start. They run under a Postgres advisory lock, so replicas that start together apply them in turn. `migrations.initContainer=true` also runs `tetriz-connectors migrate` in an init container.
- **Health.** `/_health` on port 9090 answers `ok` while Postgres and Redis respond. The startup, liveness and readiness probes use it.
- **Metrics.** `/metrics` is on the same port. `serviceMonitor.enabled=true` adds a Prometheus Operator ServiceMonitor.
- **Shutdown.** On SIGTERM the worker drains running tasks for up to 30 seconds. The grace period is 60 seconds.
- **Filesystem.** The root filesystem is read-only, and the image runs as uid 10001. `/data` is an emptyDir by default. It holds the blob directory and any file sink, so a file sink is per pod. `data.volume` takes any volume source, such as a PVC.
- **Gateway.** `gateway.config` or `gateway.existingConfigMap` replaces the built-in vendor endpoints and rates. The file is mounted at `gateway.mountPath`, and `TETRIZ_GATEWAY_CONFIG` points at it.
- **Shared Redis.** `config.namespace` prefixes the queues, the leader lock and every other key the runner writes, so several releases can share one Redis.
- **Postgres sink.** `config.sink=postgres` writes records to the `connector_records` table. It is in `secrets.sinkPostgresDsn`, or else the store's database.
- **Other settings.** `config.*` sets the plain `TETRIZ_*` variables. `extraEnv` and `extraEnvFrom` add any others. The repository's `.env.example` lists them all.

## Values

`values.yaml` documents every value. The ones you set most:

| Value | Sets |
|---|---|
| `config.redisAddr` | `TETRIZ_REDIS_ADDR`. Required unless `secrets.redisUrl` is set or `secrets.existingSecret` holds `TETRIZ_REDIS_URL` |
| `secrets.redisUrl` | `TETRIZ_REDIS_URL`. It wins over `config.redisAddr` and `secrets.redisPassword` |
| `secrets.redisPassword` | `TETRIZ_REDIS_PASSWORD` |
| `config.namespace` | `TETRIZ_NAMESPACE` |
| `secrets.existingSecret` | A Secret whose keys are `TETRIZ_*` names |
| `secrets.postgresDsn` | `TETRIZ_POSTGRES_DSN`, when the chart creates the Secret |
| `secrets.credentialsKey` | `TETRIZ_CREDENTIALS_KEY`, when the chart creates the Secret |
| `config.githubAppId`, `secrets.githubAppPrivateKey` | The GitHub App |
| `config.oauthClientIds`, `secrets.oauthClientSecrets` | OAuth applications for token refresh |
| `config.sink` | `TETRIZ_SINK` |
| `secrets.sinkPostgresDsn` | `TETRIZ_SINK_POSTGRES_DSN`, for `config.sink=postgres` |
| `replicaCount` | Runner pods |
| `image.tag`, `image.digest` | The image. The default tag is `v` + the chart's appVersion |
| `simulator.enabled` | The demo simulator and its gateway routing |
