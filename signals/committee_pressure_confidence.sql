-- Committee Pressure: confidence-aware sensitivity analysis
-- IMPORTANT: allocated_amount is filing-level disclosed lobbying amount ASSOCIATED
-- through bill/committee evidence. It is not direct spending on a committee.
--
-- Evidence universes are intentionally reported side-by-side:
--   broad     = candidate or verified (legacy v1 universe)
--   moderate  = candidate/verified with confidence >= 0.65
--   strong    = candidate/verified with confidence >= 0.85
--   verified  = verified mentions only
--
-- Each universe independently conserves filing dollars across DISTINCT
-- pressure-eligible committees reached by qualifying mentions. Scores are
-- percentile ranks within each universe, so use rank stability across universes
-- as robustness evidence; do not interpret score differences as momentum.

WITH params AS (
    SELECT 2025::int AS filing_year, 'Q4'::text AS filing_type
),
universes AS (
    SELECT * FROM (VALUES
        ('broad'::text,    0.000::numeric, false),
        ('moderate'::text, 0.650::numeric, false),
        ('strong'::text,   0.850::numeric, false),
        ('verified'::text, 1.000::numeric, true)
    ) v(universe, min_confidence, verified_only)
),
eligible_activity_committee AS (
    SELECT DISTINCT
        u.universe,
        bm.activity_id,
        bc.committee_code,
        bm.congress,
        bm.bill_type,
        bm.bill_number
    FROM universes u
    JOIN policy.bill_mention bm
      ON bm.resolution_status IN ('candidate','verified')
     AND COALESCE(bm.resolution_confidence, 0) >= u.min_confidence
     AND (NOT u.verified_only OR bm.resolution_status = 'verified')
    JOIN policy.bill_committee bc
      USING (congress, bill_type, bill_number)
    WHERE COALESCE((bc.activity_json->>'pressure_eligible')::boolean, false)
),
filing_committee AS (
    SELECT DISTINCT e.universe, a.filing_uuid, e.committee_code
    FROM eligible_activity_committee e
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    JOIN params p ON f.filing_year=p.filing_year AND f.filing_type=p.filing_type
),
filing_committee_count AS (
    SELECT universe, filing_uuid, COUNT(*)::numeric committee_count
    FROM filing_committee GROUP BY universe, filing_uuid
),
allocated AS (
    SELECT fc.universe,fc.committee_code,fc.filing_uuid,
           COALESCE(f.income,f.expenses,0)::numeric / NULLIF(fcc.committee_count,0) allocated_amount
    FROM filing_committee fc
    JOIN filing_committee_count fcc USING(universe,filing_uuid)
    JOIN lda.filing f USING(filing_uuid)
),
client_breadth AS (
    SELECT fc.universe,fc.committee_code,COUNT(DISTINCT f.client_id) distinct_clients
    FROM filing_committee fc JOIN lda.filing f USING(filing_uuid)
    GROUP BY fc.universe,fc.committee_code
),
bill_breadth AS (
    SELECT e.universe,e.committee_code,
           COUNT(DISTINCT (e.congress,e.bill_type,e.bill_number)) distinct_bills
    FROM eligible_activity_committee e
    JOIN lda.activity a USING(activity_id)
    JOIN lda.filing f USING(filing_uuid)
    JOIN params p ON f.filing_year=p.filing_year AND f.filing_type=p.filing_type
    GROUP BY e.universe,e.committee_code
),
lobbyist_breadth AS (
    SELECT e.universe,e.committee_code,COUNT(DISTINCT al.lobbyist_id) distinct_lobbyists
    FROM eligible_activity_committee e
    JOIN lda.activity_lobbyist al USING(activity_id)
    GROUP BY e.universe,e.committee_code
),
raw AS (
    SELECT al.universe,c.committee_code,c.chamber,c.name committee_name,
           COUNT(DISTINCT al.filing_uuid) distinct_filings,
           cb.distinct_clients,lb.distinct_lobbyists,bb.distinct_bills,
           SUM(al.allocated_amount) allocated_amount
    FROM allocated al
    JOIN policy.committee c USING(committee_code)
    JOIN client_breadth cb USING(universe,committee_code)
    JOIN lobbyist_breadth lb USING(universe,committee_code)
    JOIN bill_breadth bb USING(universe,committee_code)
    GROUP BY al.universe,c.committee_code,c.chamber,c.name,
             cb.distinct_clients,lb.distinct_lobbyists,bb.distinct_bills
),
ranked AS (
    SELECT *,
      PERCENT_RANK() OVER(PARTITION BY universe ORDER BY allocated_amount) amount_pct,
      PERCENT_RANK() OVER(PARTITION BY universe ORDER BY distinct_clients) clients_pct,
      PERCENT_RANK() OVER(PARTITION BY universe ORDER BY distinct_lobbyists) lobbyists_pct,
      PERCENT_RANK() OVER(PARTITION BY universe ORDER BY distinct_bills) bills_pct
    FROM raw
),
scored AS (
    SELECT *,
      ROUND((100*(0.45*amount_pct+0.25*clients_pct+0.20*lobbyists_pct+0.10*bills_pct))::numeric,1) committee_pressure_score
    FROM ranked
),
final AS (
    SELECT *,
      RANK() OVER(PARTITION BY universe ORDER BY committee_pressure_score DESC,allocated_amount DESC) pressure_rank
    FROM scored
)
SELECT universe,pressure_rank,committee_code,chamber,committee_name,
       distinct_filings,distinct_clients,distinct_lobbyists,distinct_bills,
       ROUND(allocated_amount,2) associated_allocated_amount,
       committee_pressure_score
FROM final
ORDER BY CASE universe WHEN 'broad' THEN 1 WHEN 'moderate' THEN 2 WHEN 'strong' THEN 3 ELSE 4 END,
         pressure_rank,committee_code;
