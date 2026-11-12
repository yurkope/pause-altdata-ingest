-- Raw LDA ingestion validation.
-- Run after a smoke test or full load.

SELECT
    COUNT(*) AS filings,
    COUNT(*) FILTER (WHERE filing_type IN ('Q1','Q2','Q3','Q4')) AS quarterly_reports,
    COUNT(*) FILTER (WHERE income IS NOT NULL) AS with_income,
    COUNT(*) FILTER (WHERE expenses IS NOT NULL) AS with_expenses
FROM lda.filing;

SELECT
    COUNT(*) AS activities,
    COUNT(DISTINCT filing_uuid) AS filings_with_activities,
    COUNT(DISTINCT general_issue_code) AS distinct_issue_codes
FROM lda.activity;

SELECT
    COUNT(*) AS activity_government_entity_links,
    COUNT(DISTINCT government_entity_id) AS distinct_government_entities
FROM lda.activity_government_entity;

SELECT
    COUNT(*) AS activity_lobbyist_links,
    COUNT(DISTINCT lobbyist_id) AS distinct_lobbyists
FROM lda.activity_lobbyist;

SELECT
    filing_uuid,
    COUNT(*) AS activity_count
FROM lda.activity
GROUP BY filing_uuid
HAVING COUNT(*) <> COUNT(DISTINCT activity_index)
LIMIT 10;
