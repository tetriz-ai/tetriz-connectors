# ADR-0002: Credential-scoped vendor gateways

## Status

Accepted.

## Context

A connector reaches its vendor through a `sdk.VendorGateway`. Each
organization has its own credentials, stored with its connector config, and a
gateway embeds its token when it is built. A single gateway injected into
the orchestrator at construction could therefore serve only one tenant.

A deployment may also want the component that reaches the internet to be
separate from the one that stores credentials. That raises the question of
how per-tenant credentials reach the outbound side.

Three patterns are common in integration platforms:

1. **Per-request credential injection (proxy).** The caller sends each
   request through a proxy that attaches the credential. Examples:
   [Nango](https://nango.dev/api-integrations), [Envoy Gateway credential
   injection](https://gateway.envoyproxy.io/docs/tasks/security/credential-injection/),
   [Azure API Management credential
   manager](https://learn.microsoft.com/en-us/azure/api-management/credentials-overview).
2. **Credential store lookup (vault).** The outbound side receives a tenant
   identifier and resolves the credential from a secret store. Examples:
   [Airbyte data-plane
   isolation](https://airbyte.com/data-engineering-resources/data-plane-isolation-enterprise-credentials-local),
   [HashiCorp Vault with
   Boundary](https://developer.hashicorp.com/boundary/tutorials/credential-management/hcp-private-vault-cred-injection),
   [AWS SaaS isolation with
   Vault](https://aws.amazon.com/blogs/apn/saas-data-isolation-with-dynamic-credentials-using-hashicorp-vault-in-amazon-eks/).
3. **Per-execution gateway factory.** The orchestrator builds a
   credential-scoped gateway for each run from the loaded config. It is the
   in-process form of the first pattern. Example:
   [Vessel passthrough](https://docs.vessel.dev/pages/home/about).

| | Proxy | Vault | Factory |
|---|---|---|---|
| New infrastructure | A proxy | A secret store | None |
| Works across a network split | Yes | Yes | No |

## Decision

The orchestrator takes a `sdk.VendorGatewayFactory` and calls it once per
`RunOperation`, after loading the organization's decrypted config:

```go
type VendorGatewayFactory func(ctx context.Context, cfg *sdk.ConnectorConfig) (sdk.VendorGateway, error)
```

The factory dispatches on `cfg.ConnectorID` and builds that vendor's gateway
with that organization's credentials. `runner/app` holds the factory the
runner ships; a host passes its own to `runner.NewOrchestrator`.

The signature carries the connector, the organization and the credentials,
so the factory can later make a call to a separate outbound service
(pattern 1) or resolve credentials from a store (pattern 2) without changing
the orchestrator.

## Consequences

- Each run gets a gateway scoped to one organization's credentials.
- A new connector adds a case to the factory; the orchestrator is unchanged.
- Tests inject a factory that returns a fake gateway.
- A gateway is built per run. Construction is a struct allocation and a
  config read, but it happens on every run.
- Credentials are read from the store on every run.
- The factory works only in-process. Splitting the outbound side into its
  own service means replacing the factory with a client for that service.

## References

- [OWASP Microservices Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Microservices_Security_Cheat_Sheet.html)
- [ADR-0001](0001-asynq-generalized-scheduler.md): the scheduler that triggers the runs.
