This example syncs the repositories a GitHub personal access token can see from a Go program of its own, with the built-in GitHub connector and gateway, an in-memory store, a channel sink and in-process rate limits: no Postgres and no Redis. You need it when your program decides when a sync runs. `GITHUB_TOKEN=your-personal-access-token go run ./examples/embedgithub` prints the full name of each repository. The command below runs the example's test, which syncs from a fake GitHub API on the loopback interface and checks the names it prints:

```sh
go test -count=1 ./examples/embedgithub
```

```text
ok  	github.com/tetriz-ai/tetriz-connectors/examples/embedgithub	<time>
```
