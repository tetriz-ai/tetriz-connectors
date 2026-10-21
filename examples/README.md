# Examples

| You want | Example |
|---|---|
| Your own periodic job on the scheduler, beside the connector syncs | [heartbeat](heartbeat/) |
| A connector of your own in the `tetriz-connectors` command | [customconnector](customconnector/) |
| The GitHub connector inside your own Go program, with no Postgres or Redis | [embedgithub](embedgithub/) |

`scripts/run-examples.sh` runs every example's command from the repository root and compares its output with the example's README. The examples need no vendor account and make no network call off the machine. The runner rewrites the duration `go test` prints to `<time>`.
