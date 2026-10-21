# Simulator

The simulator serves vendor-shaped APIs (GitHub, GitLab, Bitbucket, Jira)
from seeded fake data, so the connectors can run end to end without a vendor
account. It is what the docker-compose stack and `scripts/acceptance.sh` sync
against.

```bash
export TETRIZ_SIMULATOR_DSN="host=localhost port=55432 user=tetriz password=tetriz-local-only dbname=tetriz sslmode=disable"
tetriz-connectors simulator --seed --addr 127.0.0.1:8080
```

`--seed` loads the demo catalog (`demoseed/catalog.go`) before serving; the
migrations in `migrations/` run first either way.

The simulator is unauthenticated: it accepts any token and serves its whole
catalog to anyone who can reach it. `--addr` defaults to `127.0.0.1:8080`. Bind
it to all interfaces (`--addr :8080`) only on a private network, as the compose
stack and the Helm chart do inside theirs.

## Routing an organization to the simulator

The runner sends an organization's traffic to the simulator when the gateway
configuration lists it:

```yaml
SimulatorOrgIDs: ["00000000-0000-4000-8000-00000000d3e0"]
SimulatorBaseURL: http://simulator:8080
```

`deploy/gateway.demo.yaml` is a complete example. A simulated organization
gets a fixed bearer token and no OAuth refresh, so its credentials can be
`{}`. Its settings name the simulated vendor organization, which is the
catalog name:

```bash
tetriz-connectors config put --org 00000000-0000-4000-8000-00000000d3e0 \
  --connector github --settings '{"organization":"demo-gh-startup"}' --credentials '{}'
```

### Personal access token configs

A GitHub config with `personal_access_token` credentials lists repositories
with `GET /user/repos`, whose path names no organization, and the runner
sends a simulated organization's requests with a fixed token, so the
request carries nothing that names one. The simulator answers it as GitHub
would for a token owner who is a member of every simulated GitHub
organization (`github/user_repos.go`):

- It lists the repositories of every GitHub organization in the store,
  sorted by `full_name`, each with `owner` (the organization) and
  `visibility` filled in. A request with the header
  `X-Simulator-Organization: <org>[,<org>...]` lists only those
  organizations; a name that matches a fault rule answers with that fault.
- `visibility` (`all`, `public`, `private`), `affiliation` (an
  `affiliation` without `organization_member` lists nothing) and `type`
  (`owner` lists nothing) filter as on GitHub, and `type` with
  `visibility` or `affiliation` is a 422.
- `per_page` (default 30, at most 100) and `page` page the listing, with
  GitHub's `Link` header (`next`, `last`, `prev`, `first`).
- `GET /user` answers with the simulated user `simulator-user`.

So the listing holds every simulated GitHub organization's public
repositories (all of them with `"include_private":true`). The connector keeps
only `settings.organization`'s when the config names one, as it does against
GitHub. Without `organization`, a PAT config's `fetch_repos` emits them all.

```bash
tetriz-connectors config put --org 00000000-0000-4000-8000-00000000d3e0 \
  --connector github --settings '{"organization":"demo-gh-startup"}' \
  --credentials '{"personal_access_token":"simulated"}'
```

## Catalog

| Organization | Vendor | Issue tracker | Scenario |
|---|---|---|---|
| `demo-gh-enterprise` | GitHub | Jira | Large multi-repository organization, Jira-linked pull requests, heavy review |
| `demo-gh-startup` | GitHub | GitHub Issues | Trunk-based, high merge rate |
| `demo-gh-oss` | GitHub | GitHub Issues | Fork-and-pull, outside contributors |
| `demo-gl-regulated` | GitLab | GitLab Issues | Strict review gates |
| `demo-gl-scaleup` | GitLab | GitLab Issues | Mixed workflow, growing team |
| `demo-gl-solo` | GitLab | GitLab Issues | One developer, direct pushes |
| `demo-bb-enterprise` | Bitbucket | Jira | Gitflow, heavy branching |
| `demo-bb-startup` | Bitbucket | Jira | Trunk-based, small team |
| `demo-bb-solo` | Bitbucket | none | One developer, direct pushes |

GitLab group ids are synthetic integers (91001 to 91003).

**Jira is an overlay, not a separate organization.** Each organization whose
issue tracker is Jira also gets Jira projects, issues and sprints, and its
pull request titles and branches carry the matching issue keys. A Jira
connector config for it needs `settings.cloud_id`, not
`settings.organization`: Jira URLs carry no organization slug, and a config
with `organization` instead fails `not_found`. A `cloud_id` that
names no simulated organization fails `not_found`, as an unknown site does
against Atlassian, and so does a GitHub `organization` the catalog lacks.

## Fault injection

`github/rules.go`, `gitlab/rules.go`, `bitbucket/rules.go` and `jira/rules.go` let a test make
the simulator answer with a chosen status, body and header for one
organization, for example a 429 with `Retry-After`.

## Scope

The simulator answers the endpoints the GitHub, GitLab, Bitbucket and Jira
connectors call in those vendors' shapes, with their paging and `since`
filters, and with a 404 for an unknown GitHub organization or repository
and an unknown Jira site or issue. Two things are outside what it sets out
to do; each is pinned by the test named beside it.

| Out of scope | Why | Test |
|---|---|---|
| Vendors other than GitHub, GitLab, Bitbucket and Jira | The simulator backs the quick start and the acceptance sync, which run these four. The other connectors' tests run against `testkit/fakevendor` or their own fixtures | `TestSimulatorScope_ServesFourVendors` |
| Which organizations a personal access token belongs to | A simulated organization's requests carry a fixed token and `/user/repos` names no organization, so the simulator answers as for a token owner in every simulated GitHub organization (see [Personal access token configs](#personal-access-token-configs)). A token config without `organization` emits every simulated organization's repositories from `fetch_repos` | `TestUserRepos_ListsEveryOrganization` |
