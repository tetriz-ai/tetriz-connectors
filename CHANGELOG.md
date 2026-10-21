# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-21

### Added

- Initial public release.
- Nine connectors: GitHub, GitLab, Bitbucket, Jira, Linear, Cursor, Claude Code, Codex and Kiro, with a JSON Schema for every record payload.
- The `tetriz-connectors` CLI: scheduled serving, one-off syncs, configuration, cursors, store migrations and a vendor simulator.
- A Go connector SDK, orchestrator, conformance test suite and runnable embedding examples.
- GitHub App installation authorization with organization-admin or personal-owner verification and single-use validation receipts.
- PostgreSQL storage, file and PostgreSQL sinks, and a Helm chart for Kubernetes.

[Unreleased]: https://github.com/tetriz-ai/tetriz-connectors/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tetriz-ai/tetriz-connectors/releases/tag/v0.1.0
