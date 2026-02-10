# pause-altdata-ingest

Ingest public alternative data into Postgres using reproducible, idempotent bash + jq + SQL workflows.

**Currently supported**
- Senate LDA API ingestion
- Staging filings and activities
- Entity matching (via `government_entities`)
- S3a intensity diagnostics (read-only)

## Prerequisites
- `bash`
- `jq`
- `psql` (Postgres client)
- Postgres access with access to the target DB
- LDA API key (see `.env.example`)

## Quickstart
1. Copy `.env.example` to `.env` and fill in values.
2. Initialize DB schema:
   - `make init-db`
3. Ingest raw data:
   - `make ingest`
4. Normalize to JSONL:
   - `make normalize`
5. Load to staging:
   - `make load`
6. Derive entity hits:
   - `make derive`
7. Run diagnostics:
   - `make diag`
8. Verify:
   - `make verify`

## Make Targets
- `make init-db` — create schemas and staging tables
- `make ingest` — fetch raw LDA JSON (paged)
- `make normalize` — produce JSONL for filings + activities
- `make load` — load JSONL into staging tables
- `make derive` — compute entity hits
- `make diag` — run S3a intensity diagnostics (no inserts)
- `make verify` — run verification queries

## Notes
- Downloaded and intermediate files live under `data/` (gitignored).
- Scripts are idempotent by default; use `FORCE=1` to overwrite outputs.
