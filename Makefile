SHELL := /bin/bash

ENV_FILE := .env
DATA_DIR ?= data

.PHONY: help init-db ingest normalize load derive diag verify

help:
	@echo "Targets:"
	@echo "  init-db   Create schemas and staging tables"
	@echo "  ingest    Fetch raw LDA JSON (paged)"
	@echo "  normalize Produce JSONL for filings + activities"
	@echo "  load      Load JSONL into staging tables"
	@echo "  derive    Compute entity hits"
	@echo "  diag      Run S3a intensity diagnostics (no inserts)"
	@echo "  verify    Run verification queries"

init-db:
	@bin/run_sql.sh sql/00_schema.sql
	@bin/run_sql.sh sql/01_staging.sql
	@bin/run_sql.sh sql/02_derived.sql

ingest:
	@sources/lda/ingest.sh

normalize:
	@sources/lda/normalize_filings.sh
	@sources/lda/normalize_activities.sh

load:
	@sources/lda/load_staging.sh

derive:
	@bin/run_sql.sh sources/lda/derive_entity_hits.sql

diag:
	@bin/run_sql.sh sources/lda/diagnostic_s3a.sql

verify:
	@bin/run_sql.sh sources/lda/verify.sql
