CREATE SCHEMA IF NOT EXISTS lda;

CREATE TABLE IF NOT EXISTS lda.client (
    client_id BIGINT PRIMARY KEY,
    client_external_id TEXT,
    name TEXT,
    general_description TEXT,
    client_government_entity BOOLEAN,
    client_self_select BOOLEAN,
    state TEXT,
    country TEXT,
    ppb_state TEXT,
    ppb_country TEXT,
    effective_date DATE,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lda.registrant (
    registrant_id BIGINT PRIMARY KEY,
    house_registrant_id TEXT,
    name TEXT,
    description TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    country TEXT,
    ppb_country TEXT,
    contact_name TEXT,
    contact_telephone TEXT,
    dt_updated TIMESTAMPTZ,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lda.filing (
    filing_uuid TEXT PRIMARY KEY,
    filing_type TEXT,
    filing_type_display TEXT,
    filing_year INTEGER,
    filing_period TEXT,
    filing_period_display TEXT,
    income NUMERIC,
    expenses NUMERIC,
    expenses_method TEXT,
    expenses_method_display TEXT,
    dt_posted TIMESTAMPTZ,
    termination_date DATE,
    filing_url TEXT,
    filing_document_url TEXT,
    filing_document_content_type TEXT,
    posted_by_name TEXT,
    registrant_id BIGINT REFERENCES lda.registrant(registrant_id),
    client_id BIGINT REFERENCES lda.client(client_id),
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lda_filing_year_type ON lda.filing (filing_year, filing_type);
CREATE INDEX IF NOT EXISTS idx_lda_filing_dt_posted ON lda.filing (dt_posted);
CREATE INDEX IF NOT EXISTS idx_lda_filing_client ON lda.filing (client_id);
CREATE INDEX IF NOT EXISTS idx_lda_filing_registrant ON lda.filing (registrant_id);

CREATE TABLE IF NOT EXISTS lda.activity (
    activity_id BIGSERIAL PRIMARY KEY,
    filing_uuid TEXT NOT NULL REFERENCES lda.filing(filing_uuid) ON DELETE CASCADE,
    activity_index INTEGER NOT NULL,
    general_issue_code TEXT,
    general_issue_display TEXT,
    description TEXT,
    foreign_entity_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_lda_activity_filing_index UNIQUE (filing_uuid, activity_index)
);

CREATE INDEX IF NOT EXISTS idx_lda_activity_filing ON lda.activity (filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lda_activity_issue ON lda.activity (general_issue_code);

CREATE TABLE IF NOT EXISTS lda.government_entity (
    government_entity_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lda.activity_government_entity (
    activity_id BIGINT NOT NULL REFERENCES lda.activity(activity_id) ON DELETE CASCADE,
    government_entity_id BIGINT NOT NULL REFERENCES lda.government_entity(government_entity_id),
    PRIMARY KEY (activity_id, government_entity_id)
);

CREATE INDEX IF NOT EXISTS idx_lda_activity_gov_entity_entity
    ON lda.activity_government_entity (government_entity_id);

CREATE TABLE IF NOT EXISTS lda.lobbyist (
    lobbyist_id BIGINT PRIMARY KEY,
    prefix TEXT,
    first_name TEXT,
    nickname TEXT,
    middle_name TEXT,
    last_name TEXT,
    suffix TEXT,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lda.activity_lobbyist (
    activity_id BIGINT NOT NULL REFERENCES lda.activity(activity_id) ON DELETE CASCADE,
    lobbyist_id BIGINT NOT NULL REFERENCES lda.lobbyist(lobbyist_id),
    covered_position TEXT,
    is_new BOOLEAN,
    source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (activity_id, lobbyist_id)
);

CREATE INDEX IF NOT EXISTS idx_lda_activity_lobbyist_lobbyist
    ON lda.activity_lobbyist (lobbyist_id);

CREATE TABLE IF NOT EXISTS lda.ingest_run (
    ingest_run_id BIGSERIAL PRIMARY KEY,
    filing_year INTEGER NOT NULL,
    filing_type TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    pages_processed INTEGER NOT NULL DEFAULT 0,
    filings_processed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
