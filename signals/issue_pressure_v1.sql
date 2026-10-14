-- Issue Spend / Pressure Signal v1
-- Read-only research query; creates no database objects.
-- Grain: one LDA issue code for 2025 Q4.
--
-- Spend allocation: deduplicate issue codes within each filing, then split
-- COALESCE(income, expenses) equally across the filing's distinct issues.
-- This conserves disclosed dollars and avoids rewarding repeated activity rows.
--
-- Pressure score (0-100 cross-sectional percentile):
--   45% allocated spend, 25% client breadth,
--   20% government-entity reach, 10% lobbyist breadth.
--
-- dt_posted is disclosure timing, not lobbying activity timing. Do not call
-- posted-date changes lobbying momentum. True momentum should compare equivalent
-- quarterly observations after multiple quarters are loaded.

WITH issue_per_filing AS (
    SELECT DISTINCT
        f.filing_uuid,
        f.client_id,
        COALESCE(f.income, f.expenses, 0)::numeric AS filing_amount,
        a.general_issue_code AS issue_code
    FROM lda.filing f
    JOIN lda.activity a USING (filing_uuid)
    WHERE f.filing_year = 2025
      AND f.filing_type = 'Q4'
      AND a.general_issue_code IS NOT NULL
),
issue_counts AS (
    SELECT filing_uuid, COUNT(*) AS issue_count
    FROM issue_per_filing
    GROUP BY filing_uuid
),
money AS (
    SELECT
        i.issue_code,
        SUM(i.filing_amount / NULLIF(c.issue_count, 0)) AS allocated_spend,
        COUNT(DISTINCT i.filing_uuid) AS filings,
        COUNT(DISTINCT i.client_id) AS clients
    FROM issue_per_filing i
    JOIN issue_counts c USING (filing_uuid)
    GROUP BY i.issue_code
),
agency AS (
    SELECT
        a.general_issue_code AS issue_code,
        COUNT(DISTINCT age.government_entity_id) AS agencies,
        COUNT(*) FILTER (WHERE age.government_entity_id IS NOT NULL) AS government_links
    FROM lda.activity a
    JOIN lda.filing f USING (filing_uuid)
    LEFT JOIN lda.activity_government_entity age USING (activity_id)
    WHERE f.filing_year = 2025
      AND f.filing_type = 'Q4'
      AND a.general_issue_code IS NOT NULL
    GROUP BY a.general_issue_code
),
lobby AS (
    SELECT
        a.general_issue_code AS issue_code,
        COUNT(DISTINCT al.lobbyist_id) AS lobbyists
    FROM lda.activity a
    JOIN lda.filing f USING (filing_uuid)
    LEFT JOIN lda.activity_lobbyist al USING (activity_id)
    WHERE f.filing_year = 2025
      AND f.filing_type = 'Q4'
      AND a.general_issue_code IS NOT NULL
    GROUP BY a.general_issue_code
),
features AS (
    SELECT m.*, ag.agencies, ag.government_links, l.lobbyists
    FROM money m
    JOIN agency ag USING (issue_code)
    JOIN lobby l USING (issue_code)
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY LN(1 + allocated_spend)) AS spend_pct,
        PERCENT_RANK() OVER (ORDER BY LN(1 + clients)) AS breadth_pct,
        PERCENT_RANK() OVER (ORDER BY LN(1 + agencies)) AS agency_pct,
        PERCENT_RANK() OVER (ORDER BY LN(1 + lobbyists)) AS lobbyist_pct
    FROM features
)
SELECT
    issue_code,
    ROUND(allocated_spend, 2) AS allocated_spend,
    filings,
    clients,
    agencies,
    government_links,
    lobbyists,
    ROUND((100 * (
        0.45 * spend_pct +
        0.25 * breadth_pct +
        0.20 * agency_pct +
        0.10 * lobbyist_pct
    ))::numeric, 1) AS pressure_score
FROM scored
ORDER BY pressure_score DESC, allocated_spend DESC;
