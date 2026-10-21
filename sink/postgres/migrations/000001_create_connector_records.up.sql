-- Copyright 2026 Workloom Solutions Private Limited
-- SPDX-License-Identifier: MIT

-- connector_records: the latest copy of each record a connector emitted.
--
-- The key is the one the sink documents for deduplication: organization,
-- connector, operation, external id and data date. data_date is NULL when the
-- record has none; data_date_key stands in for it in the key, so records
-- without a date still collapse to one row.
CREATE TABLE IF NOT EXISTS connector_records (
    organization_id  TEXT        NOT NULL,
    connector_id     TEXT        NOT NULL,
    operation_id     TEXT        NOT NULL,
    external_id      TEXT        NOT NULL,
    data_date        TIMESTAMPTZ,
    data_date_key    TIMESTAMPTZ NOT NULL
        GENERATED ALWAYS AS (COALESCE(data_date, '-infinity'::timestamptz)) STORED,
    -- The vendor payload as JSON, or, when payload_encoding is 'base64', a
    -- JSON string holding the base64 of bytes that are not valid JSON.
    payload          JSONB       NOT NULL,
    payload_encoding TEXT        NOT NULL DEFAULT 'json'
        CONSTRAINT connector_records_payload_encoding_check CHECK (payload_encoding IN ('json', 'base64')),
    metadata         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    fetched_at       TIMESTAMPTZ NOT NULL,
    sync_run_id      TEXT,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connector_records_pkey
        PRIMARY KEY (organization_id, connector_id, operation_id, external_id, data_date_key)
);

-- Changed since: "pull requests updated in the last day".
CREATE INDEX IF NOT EXISTS idx_connector_records_op_updated
    ON connector_records (connector_id, operation_id, updated_at);

-- By business date: "usage on 1 March".
CREATE INDEX IF NOT EXISTS idx_connector_records_op_data_date
    ON connector_records (connector_id, operation_id, data_date);
