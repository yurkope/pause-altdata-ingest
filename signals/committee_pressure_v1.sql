-- Committee Pressure v1
-- Candidate-associated pressure from LDA filing-level disclosures and Congress.gov committee evidence.
-- IMPORTANT: allocated_amount is NOT direct spending on a committee.
-- It is filing-level disclosed lobbying amount conservatively associated through candidate bill mentions.
--
-- Score weights:
--   45% allocated amount percentile
--   25% client breadth percentile
--   20% lobbyist breadth percentile
--   10% bill breadth percentile
--
-- Dollar conservation: each filing amount is split across DISTINCT pressure-eligible committees
-- reached by its candidate/verified bill mentions. Multiple activities/bills to the same committee
-- do not multiply the filing amount.

WITH params AS (
    SELECT 2025::int AS filing_year, 'Q4'::text AS filing_type
),
eligible_activity_committee AS (
    SELECT DISTINCT
        bm.activity_id,
        bc.committee_code,
        bm.congress,
        bm.bill_type,
        bm.bill_number,
        bm.resolution_status
    FROM policy.bill_mention bm
    JOIN policy.bill_committee bc
      USING (congress, bill_type, bill_number)
    WHERE bm.resolution_status IN ('candidate','verified')
      AND COALESCE((bc.activity_json->>'pressure_eligible')::boolean, false)
),
filing_committee AS (
    SELECT DISTINCT
        a.filing_uuid,
        e.committee_code
    FROM eligible_activity_committee e
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    JOIN params p
      ON f.filing_year = p.filing_year
     AND f.filing_type = p.filing_type
),
filing_committee_count AS (
    SELECT filing_uuid, COUNT(*)::numeric AS committee_count
    FROM filing_committee
    GROUP BY filing_uuid
),
allocated AS (
    SELECT
        fc.committee_code,
        fc.filing_uuid,
        COALESCE(f.income, f.expenses, 0)::numeric / NULLIF(fcc.committee_count, 0) AS allocated_amount
    FROM filing_committee fc
    JOIN filing_committee_count fcc USING (filing_uuid)
    JOIN lda.filing f USING (filing_uuid)
),
client_breadth AS (
    SELECT fc.committee_code, COUNT(DISTINCT f.client_id) AS distinct_clients
    FROM filing_committee fc
    JOIN lda.filing f USING (filing_uuid)
    GROUP BY fc.committee_code
),
bill_breadth AS (
    SELECT
        e.committee_code,
        COUNT(DISTINCT (e.congress, e.bill_type, e.bill_number)) AS distinct_bills
    FROM eligible_activity_committee e
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    JOIN params p
      ON f.filing_year = p.filing_year
     AND f.filing_type = p.filing_type
    GROUP BY e.committee_code
),
lobbyist_breadth AS (
    SELECT
        fc.committee_code,
        COUNT(DISTINCT al.lobbyist_id) AS distinct_lobbyists
    FROM filing_committee fc
    JOIN lda.activity a USING (filing_uuid)
    JOIN lda.activity_lobbyist al USING (activity_id)
    GROUP BY fc.committee_code
),
raw AS (
    SELECT
        c.committee_code,
        c.chamber,
        c.name AS committee_name,
        COUNT(DISTINCT al.filing_uuid) AS distinct_filings,
        cb.distinct_clients,
        lb.distinct_lobbyists,
        bb.distinct_bills,
        SUM(al.allocated_amount) AS allocated_amount
    FROM allocated al
    JOIN policy.committee c USING (committee_code)
    JOIN client_breadth cb USING (committee_code)
    JOIN lobbyist_breadth lb USING (committee_code)
    JOIN bill_breadth bb USING (committee_code)
    GROUP BY c.committee_code, c.chamber, c.name,
             cb.distinct_clients, lb.distinct_lobbyists, bb.distinct_bills
),
ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY allocated_amount) AS amount_pct,
        PERCENT_RANK() OVER (ORDER BY distinct_clients) AS clients_pct,
        PERCENT_RANK() OVER (ORDER BY distinct_lobbyists) AS lobbyists_pct,
        PERCENT_RANK() OVER (ORDER BY distinct_bills) AS bills_pct
    FROM raw
)
SELECT
    committee_code,
    chamber,
    committee_name,
    distinct_filings,
    distinct_clients,
    distinct_lobbyists,
    distinct_bills,
    ROUND(allocated_amount, 2) AS candidate_associated_allocated_amount,
    ROUND((100 * (
        0.45 * amount_pct +
        0.25 * clients_pct +
        0.20 * lobbyists_pct +
        0.10 * bills_pct
    ))::numeric, 1) AS committee_pressure_score
FROM ranked
ORDER BY committee_pressure_score DESC, allocated_amount DESC;
