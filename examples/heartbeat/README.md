This example is a periodic job of your own on the scheduler: a `schedule.ScheduleSource` that returns one job firing every ten seconds, and the `asynq.Handler` that runs it. You need it when your service runs work on a schedule beside the connector syncs. `REDIS_ADDR=localhost:56379 go run ./examples/heartbeat/cmd` runs it against the quick start's Valkey, with the runner stopped (`docker compose stop runner`), until you stop it, and logs each fire: a `serve` on the same Redis would take its tasks and fail them. The command below runs the example's compared-output test, which needs no Redis:

```sh
go test -count=1 -run '^ExampleSource$' ./examples/heartbeat
```

```text
ok  	github.com/tetriz-ai/tetriz-connectors/examples/heartbeat	<time>
```
