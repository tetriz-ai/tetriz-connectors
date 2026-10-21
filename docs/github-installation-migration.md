# GitHub App installation authorization

`ValidateInstallation` and `SaveInstallation` enforce server-side authorization
of an installation, including installations nobody has connected yet. App JWT
access alone proves that the App owns an installation, not that the caller can use it.

Hosts embedding `auth/integrations.Service` use this callback flow:

1. Configure both the App's **Setup URL** and its **user authorization callback
   URL** on the host. These are different redirect steps: the Setup URL receives
   `installation_id`, while the OAuth callback is documented to receive `code`
   and `state`. Do not assume all three arrive in one redirect. Use this explicit
   two-step flow, leaving **Request user authorization (OAuth) during installation**
   disabled:
   - Start with the service's existing installation URL and retain its unpredictable
     state server-side in the signed-in host session, bound to its tenant.
   - At the Setup URL, verify the callback state against that session and retain
     the candidate installation ID server-side with the state. This host session
     check must not consume the service's Redis CSRF state: only the final
     `ValidateInstallation` call consumes it. Do not save
     connector credentials or trust the ID yet.
   - Redirect the user to `https://github.com/login/oauth/authorize` with the
     same App's `client_id` and the retained `state`. Configure the intended OAuth
     callback as the **first** registered callback URL (the default when no
     `redirect_uri` is supplied).
   - At the OAuth callback, verify session state again and retrieve the candidate
     installation ID from that session. Forward `code`, `state` and that retained
     ID to `ValidateInstallation`. The service consumes its Redis state and verifies
     user visibility before issuing a save receipt.
   Enforce the signed-in user's permission to administer the tenant throughout.
   Automatic OAuth during installation uses the first OAuth callback and is an
   alternative only when the host has explicitly implemented installation selection;
   its callback is not documented to include the Setup URL's installation ID.
2. Supply `connectors/config.Config.GitHubAppClientSecret` from the host's secret
   store or environment, alongside `GitHubApp.ClientID` from the same App. For an
   environment-backed host, explicitly assign
   `cfg.GitHubAppClientSecret = os.Getenv("TETRIZ_GITHUB_APP_CLIENT_SECRET")`.
   The standalone runner does not expose these installation-management RPCs and
   does not read this host configuration automatically. Never place this secret
   in YAML, browser code, logs or callback URLs.
3. Set `ValidateInstallationRequest.code` to the callback code. Validation consumes
   the existing tenant-bound CSRF state, exchanges the code server-side, and
   requires the installation and matching account metadata to appear in
   `GET /user/installations` using the resulting **user** token. Visibility alone
   admits collaborators with access to a single repository and is insufficient
   to authorize the saved installation-wide credential. For organization installs,
   the App registration must request **Organization permissions → Members: read**,
   and existing installations must approve that permission change. Validation
   additionally calls `GET /user/memberships/orgs/{org}` using the same user token
   and requires active membership with role `admin` in the exact account ID and
   login returned by the App installation. Missing permission, ordinary members,
   repository collaborators and pending memberships are denied. For personal
   installs, `GET /user` must return the installation account's exact ID; another
   user's repository access is insufficient. The token is never stored or returned. No extra OAuth
   scopes are invented: GitHub App permissions and user access govern this API.
4. Keep the existing individual-owner pin. Then send the response's opaque
   `validation_token` unchanged in `SaveInstallationRequest.validation_token`,
   alongside the same tenant and installation and existing `expected_account_id`.
   Save atomically consumes this five-minute, single-use receipt and verifies the
   fresh installation still has the validated account before writing credentials.
   A failed or expired save requires a fresh connect; replay is rejected.

Redis is required for both steps. Missing code, OAuth credentials or receipt,
wrong tenant/installation, changed account and unavailable validation storage all
fail closed. Requests that omit the required fields are rejected.
The visibility check examines at most ten pages of 100 installations and bounds
responses to 1 MiB with a 30-second overall deadline; larger listings fail closed.
The host must continue to authorize tenant administration for every management RPC;
user installation visibility does not establish a platform tenant's membership.

Primary references:

- [GitHub: generating a user access token for a GitHub App](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app)
- [GitHub: list app installations accessible to the user access token](https://docs.github.com/en/rest/apps/installations#list-app-installations-accessible-to-the-user-access-token)

- [GitHub: about the Setup URL and spoofed installation IDs](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/about-the-setup-url)
- [GitHub: about the user authorization callback URL](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/about-the-user-authorization-callback-url)

- [GitHub: authenticated user organization membership and required Members permission](https://docs.github.com/en/rest/orgs/members#get-an-organization-membership-for-the-authenticated-user)
- [GitHub: authenticated user identity](https://docs.github.com/en/rest/users/users#get-the-authenticated-user)
