-- Copyright 2026 Workloom Solutions Private Limited
-- SPDX-License-Identifier: MIT

-- Dropping a table removes its indexes, constraints, policies and row-level
-- security state.
DROP TABLE IF EXISTS connector_fetch_cache;
DROP TABLE IF EXISTS connector_schedules;
DROP TABLE IF EXISTS connector_sync_runs;
DROP TABLE IF EXISTS connector_sync_states;
DROP TABLE IF EXISTS connector_configs;
