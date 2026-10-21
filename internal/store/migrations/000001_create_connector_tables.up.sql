-- Copyright 2026 Workloom Solutions Private Limited
-- SPDX-License-Identifier: MIT

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- connector_configs: per-organization connector configuration.
--
-- health_state gates scheduled syncs independently of `enabled`: a connector
-- whose credential is confirmed dead (auth_revoked) is paused until a fresh
-- credential write resets it, instead of retrying every scheduled tick. Every
-- read and write of it goes through the primary key.
--
-- credentials_updated_at is bumped only by a credential write (an OAuth or PAT
-- reconnect, or a disconnect), never by `updated_at`, which every settings or
-- cache write on the row also advances. A sync run fences its health-state
-- write on this column, so a run that wrote settings first can still mark a
-- dead credential auth_revoked.
CREATE TABLE IF NOT EXISTS connector_configs (
    connector_id   VARCHAR(255)   NOT NULL,
    organization_id VARCHAR(255)  NOT NULL,
    credentials    JSONB          NOT NULL DEFAULT '{}',
    settings       JSONB          NOT NULL DEFAULT '{}',
    operation_overrides JSONB     NOT NULL DEFAULT '{}',
    enabled        BOOLEAN        NOT NULL DEFAULT true,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    deleted_at     TIMESTAMP WITH TIME ZONE,
    health_state   VARCHAR(50)    NOT NULL DEFAULT 'healthy',
    health_state_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    credentials_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connector_configs_pkey PRIMARY KEY (organization_id, connector_id)
);

-- settings and operation_overrides hold JSON objects. A row holding any other
-- JSON value (an array, a string) is read as an unreadable row and left out of
-- the lists; these checks reject such a write.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connector_configs_settings_is_object'
                   AND conrelid = 'public.connector_configs'::regclass) THEN
        ALTER TABLE public.connector_configs
            ADD CONSTRAINT connector_configs_settings_is_object
            CHECK (jsonb_typeof(settings) = 'object') NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connector_configs_operation_overrides_is_object'
                   AND conrelid = 'public.connector_configs'::regclass) THEN
        ALTER TABLE public.connector_configs
            ADD CONSTRAINT connector_configs_operation_overrides_is_object
            CHECK (jsonb_typeof(operation_overrides) = 'object') NOT VALID;
    END IF;
END
$$;

-- connector_sync_states: cursor persistence for incremental sync.
CREATE TABLE IF NOT EXISTS connector_sync_states (
    organization_id VARCHAR(255)  NOT NULL,
    connector_id    VARCHAR(255)  NOT NULL,
    operation_id    VARCHAR(255)  NOT NULL,
    cursor_data     JSONB,
    -- Counts saves; SaveCursor saves only over the version the writer read.
    version         BIGINT        NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    deleted_at      TIMESTAMP WITH TIME ZONE,
    CONSTRAINT connector_sync_states_pkey PRIMARY KEY (organization_id, connector_id, operation_id)
);

-- connector_sync_runs: execution history for audit and debugging.
CREATE TABLE IF NOT EXISTS connector_sync_runs (
    id              UUID           DEFAULT uuid_generate_v4() NOT NULL
        CONSTRAINT connector_sync_runs_pkey PRIMARY KEY,
    organization_id VARCHAR(255)   NOT NULL,
    connector_id    VARCHAR(255)   NOT NULL,
    operation_id    VARCHAR(255)   NOT NULL,
    status          VARCHAR(50)    NOT NULL,
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at    TIMESTAMP WITH TIME ZONE,
    records_emitted BIGINT         NOT NULL DEFAULT 0,
    error_message   TEXT,
    error_kind      VARCHAR(100),
    cursor_before   JSONB,
    cursor_after    JSONB,
    trigger         VARCHAR(100)   NOT NULL,
    -- A running run past this time is abandoned: its heartbeat stopped.
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    deleted_at      TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_org_connector_op
    ON connector_sync_runs (organization_id, connector_id, operation_id, started_at DESC);

-- Serves "did any operation complete for this (organization_id, connector_id)
-- recently?". The query names no operation_id, so the index above degrades to
-- its two-column prefix for it. Partial on status: only completed runs are
-- evidence, so failed and running runs stay out of it.
-- Serves the abandoned-run reconciler: running runs by lease.
CREATE INDEX IF NOT EXISTS idx_sync_runs_running_lease
    ON connector_sync_runs (lease_expires_at)
    WHERE status = 'running' AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sync_runs_org_connector_completed
    ON connector_sync_runs (organization_id, connector_id, completed_at DESC)
    WHERE status = 'completed' AND deleted_at IS NULL;

-- connector_schedules: cron-based schedule overrides.
CREATE TABLE IF NOT EXISTS connector_schedules (
    organization_id VARCHAR(255)  NOT NULL,
    connector_id    VARCHAR(255)  NOT NULL,
    operation_id    VARCHAR(255)  NOT NULL,
    cron_expression VARCHAR(255)  NOT NULL,
    enabled         BOOLEAN       NOT NULL DEFAULT true,
    last_run_at     TIMESTAMP WITH TIME ZONE,
    next_run_at     TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    deleted_at      TIMESTAMP WITH TIME ZONE,
    CONSTRAINT connector_schedules_pkey PRIMARY KEY (organization_id, connector_id, operation_id)
);

-- connector_fetch_cache: per-entity memoization cache. One row per cache
-- entry, so a cache write touches one small row. The table keeps the default
-- replica identity (the primary key); a host that replicates it and needs full
-- before-and-after images can set REPLICA IDENTITY FULL itself.
CREATE TABLE IF NOT EXISTS connector_fetch_cache (
    -- organization_id is VARCHAR to match the other connector tables' RLS policies.
    organization_id  VARCHAR(255)             NOT NULL,
    connector_id     VARCHAR(255)             NOT NULL,
    cache_type       VARCHAR(100)             NOT NULL,
    entry_key        VARCHAR(500)             NOT NULL,
    value            JSONB                    NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),  -- row first inserted
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),  -- last real value change (write-path invariant)
    last_accessed_at TIMESTAMP WITH TIME ZONE,                         -- reserved for LRU eviction, not populated
    CONSTRAINT connector_fetch_cache_pkey
        PRIMARY KEY (organization_id, connector_id, cache_type, entry_key)
);

-- Row-level security for tenant isolation. The policies read
-- current_setting('app.current_tenant'), which the application sets with
-- SET LOCAL in each transaction. FORCE applies them to the table owner too,
-- which the runner usually connects as, so a missing tenant scope reads no
-- tenant's rows; the '__system__' scope is the only bypass. DDL and TRUNCATE
-- are not subject to row-level security, so migrations still run as the owner.

ALTER TABLE connector_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_configs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON connector_configs;
CREATE POLICY tenant_isolation ON connector_configs
  USING (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  )
  WITH CHECK (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  );

ALTER TABLE connector_sync_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_sync_states FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON connector_sync_states;
CREATE POLICY tenant_isolation ON connector_sync_states
  USING (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  )
  WITH CHECK (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  );

ALTER TABLE connector_sync_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_sync_runs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON connector_sync_runs;
CREATE POLICY tenant_isolation ON connector_sync_runs
  USING (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  )
  WITH CHECK (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  );

ALTER TABLE connector_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_schedules FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON connector_schedules;
CREATE POLICY tenant_isolation ON connector_schedules
  USING (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  )
  WITH CHECK (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  );

ALTER TABLE connector_fetch_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_fetch_cache FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON connector_fetch_cache;
CREATE POLICY tenant_isolation ON connector_fetch_cache
  USING (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  )
  WITH CHECK (
    (SELECT current_setting('app.current_tenant', true)) = '__system__'
    OR organization_id::text = (SELECT current_setting('app.current_tenant', true))
  );

-- One vendor resource that the runner reaches with access it holds or mints,
-- rather than a credential the organization brings, belongs to one
-- organization. The checks live in the schema because a read-then-write in the
-- application cannot be atomic: two submissions naming the same resource could
-- both read "unclaimed" and both succeed. The keys sit in settings, not
-- credentials, because credentials are encrypted into a ciphertext envelope, so
-- an expression index over them would match nothing.
--
-- Kiro reports bucket: one organization's reports must not be collected into
-- another's tenant. Only an enabled, live row holds the claim, so an
-- organization whose connectors were disabled at offboarding releases it. Rows
-- with no bucket are excluded, because an absent key reads as null and a
-- cleared one as '', and both occur.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_kiro_bucket_per_org
    ON public.connector_configs ((settings ->> 'bucket'))
    WHERE connector_id = 'kiro'
      AND deleted_at IS NULL
      AND enabled = true
      AND settings ->> 'bucket' IS NOT NULL
      AND settings ->> 'bucket' <> '';

-- GitHub App installation: the app mints the installation's tokens, so an
-- organization that names another's installation id would read its
-- repositories. Every live row holds the claim, enabled or not: a suspended
-- installation is still that organization's, and an unsuspend must be able to
-- re-enable it.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_github_installation_per_org
    ON public.connector_configs ((settings ->> 'installation_id'))
    WHERE connector_id = 'github'
      AND deleted_at IS NULL
      AND settings ->> 'installation_id' IS NOT NULL
      AND settings ->> 'installation_id' <> '';

-- Claude Code organization and Codex workspace: an admin API key reads a whole
-- vendor organization or workspace, so one is collected into one tenant. As
-- for the Kiro bucket, only an enabled row holds the claim.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_anthropic_organization_per_org
    ON public.connector_configs ((settings ->> 'anthropic_organization_id'))
    WHERE connector_id = 'claude-code'
      AND deleted_at IS NULL
      AND enabled = true
      AND settings ->> 'anthropic_organization_id' IS NOT NULL
      AND settings ->> 'anthropic_organization_id' <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uniq_codex_workspace_per_org
    ON public.connector_configs ((settings ->> 'workspace_id'))
    WHERE connector_id = 'codex'
      AND deleted_at IS NULL
      AND enabled = true
      AND settings ->> 'workspace_id' IS NOT NULL
      AND settings ->> 'workspace_id' <> '';
