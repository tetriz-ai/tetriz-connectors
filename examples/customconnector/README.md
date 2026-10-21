This example adds a `todo` connector to the `tetriz-connectors` command with `cli.Main` and `app.WithConnector`, without forking the command. You need it when a vendor has no built-in connector: `serve` then schedules `fetch_todos` beside the built-in operations, and `sync`, `config put` and `cursor show` accept `--connector todo`. The command lists the connector:

```sh
go run ./examples/customconnector connectors 2>/dev/null | grep -A1 '^todo'
```

```text
todo
  fetch_todos                  */15 * * * *
```
