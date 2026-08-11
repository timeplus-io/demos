# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A collection of independent Timeplus demo assets — not a software product. No repo-wide build, tests, lint, or CI.

- `cases/` — 12 self-contained demo use cases, each a folder of Timeplus streaming-SQL files plus optional `dashboard.json` (console dashboard export) and `readme.md`
- `k8s/` — Helm values + Makefiles for the shared GCP demo cluster (Timeplus, Kafka, Grafana, OpenSearch, Splunk, NATS, etc.). Each subdir's Makefile uses the same verbs: `init`, `install`, `upgrade`, `uninstall`, `forward`, `logs`, `clean`
- `apps/alert-server/` — small Go SSE alert-viewer webhook receiver (`make run`, `make build`, `make docker_build`); zero dependencies, port hardcoded to 8080 in main.go

## Running SQL

- SQL is applied via the ClickHouse-compatible HTTP interface on port 8123.
- Which Timeplus instance to target varies by demo — **ask before executing SQL**; don't assume localhost.
- Files prefixed `0_`, `1_`, … must be executed in numeric order (e.g. `cases/invest_insights/`).
- Demos generate their own data with `CREATE RANDOM STREAM … SETTINGS eps = N` — no external producers needed for most cases.
- Some demos install Python packages *into the server* (`SYSTEM INSTALL PYTHON PACKAGE '…'` or the `install_dep` Makefile target in `cases/realtime_features_game/`). `GITHUB_TOKEN` for `cases/github` must be set in the timeplusd server environment, not the client.

## Conventions for a new case

- Folder under `cases/<case_name>/`; SQL starts with `CREATE DATABASE IF NOT EXISTS <case_name>;` and namespaces everything in that database (one database per case).
- Entry file is `resources.sql`; larger cases split into numbered files `0_…`, `1_…` run in order.
- Include a `dashboard.json` console export and an explanatory `readme.md` (narrative query walkthrough, not setup steps).
- Naming: `mv_*` materialized views, `v_*` views, `*_ext` / `topic_*` external Kafka streams, `*_mut` mutable streams, `ex_s3_*` external S3 tables.
- SQL style: lowercase Timeplus types (`string`, `float64`, `datetime64(3, 'UTC')`), uppercase DDL keywords, `--` comments.

## Credentials and endpoints

The committed credentials (`admin`/`Password!`, Splunk HEC token, demo-cluster IPs like Kafka `10.138.0.23:9092`) are intentional demo-grade values — reuse them in new demo files for consistency. Kafka brokers and other service endpoints point at the shared demo cluster, not localhost.

## Gotchas

- The Timeplus Helm chart is a local tarball (`k8s/timeplus/timeplus-enterprise-v<VERSION>.tgz`, gitignored) — download it before `make install`.
- The DDoS demo attack is toggled with `SYSTEM PAUSE|RESUME MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack`.
- `k8s/marimo/Dockerfile` references `app.py`/`requirements.txt` that aren't in the repo — it can't be built as-is.
- Some Makefile targets hardcode the personal namespace `user-gang` (`k8s/timeplus` `ano`, `k8s/data/githubdemo`) — check the namespace before running.
- `k8s/data/cardemo/Makefile` uses spaces instead of tabs and fails under GNU make.
