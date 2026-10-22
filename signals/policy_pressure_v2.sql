-- Policy Pressure v2
-- Headline pressure ranks substantive policies only.
-- Parent/fallback and context labels remain in the conserved allocation layer.
-- associated_allocation is an analytical allocation of filing-level disclosed amounts,
-- not money paid to or spent on a policy.
-- IMPORTANT: financial exposure and activity breadth are aggregated separately to
-- prevent one-to-many activity joins from multiplying filing-level allocations.
-- Weights: 40% allocated exposure, 30% client breadth, 20% activity breadth, 10% filing breadth.
-- Momentum uses exact adjacent filing quarters only.

WITH fa AS (
    SELECT a.*
    FROM policy.filing_policy_allocation_v2 a
    JOIN policy.canonical_policy p USING (policy_code)
    WHERE p.allocation_role = 'substantive'
),
financial AS (
    SELECT
        fa.filing_type,
        CASE fa.filing_type WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END AS quarter_num,
        fa.policy_code,
        SUM(fa.associated_allocation)::numeric AS allocated_amount,
        COUNT(DISTINCT f.client_id) AS clients,
        COUNT(DISTINCT fa.filing_uuid) AS filings
    FROM fa
    JOIN lda.filing f USING (filing_uuid)
    GROUP BY 1,2,3
),
activity_breadth AS (
    SELECT
        f.filing_type,
        r.policy_code,
        COUNT(DISTINCT r.activity_id) AS activities
    FROM policy.activity_policy_resolved_v2 r
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    WHERE f.filing_year = 2025
      AND f.filing_type IN ('Q1','Q2','Q3','Q4')
      AND EXISTS (
          SELECT 1 FROM fa x
          WHERE x.filing_uuid = f.filing_uuid
            AND x.policy_code = r.policy_code
      )
    GROUP BY 1,2
),
raw AS (
    SELECT financial.*, COALESCE(activity_breadth.activities,0) AS activities
    FROM financial
    LEFT JOIN activity_breadth USING (filing_type,policy_code)
),
ranked AS (
    SELECT r.*,
        percent_rank() OVER (PARTITION BY filing_type ORDER BY allocated_amount) AS amount_pct,
        percent_rank() OVER (PARTITION BY filing_type ORDER BY clients) AS clients_pct,
        percent_rank() OVER (PARTITION BY filing_type ORDER BY activities) AS activities_pct,
        percent_rank() OVER (PARTITION BY filing_type ORDER BY filings) AS filings_pct
    FROM raw r
),
scored AS (
    SELECT *,
        ROUND((100 * (0.40*amount_pct + 0.30*clients_pct + 0.20*activities_pct + 0.10*filings_pct))::numeric,1) AS pressure_score
    FROM ranked
),
lagged AS (
    SELECT s.*,
        lag(quarter_num) OVER (PARTITION BY policy_code ORDER BY quarter_num) AS prior_quarter_num,
        lag(pressure_score) OVER (PARTITION BY policy_code ORDER BY quarter_num) AS prior_score
    FROM scored s
)
SELECT *,
    CASE WHEN prior_quarter_num = quarter_num - 1 THEN ROUND(pressure_score-prior_score,1) END AS score_change,
    CASE
        WHEN prior_quarter_num IS NULL OR prior_quarter_num <> quarter_num - 1 THEN 'NEW_EMERGING'
        WHEN pressure_score-prior_score >= 5 THEN 'ACCELERATING'
        WHEN pressure_score-prior_score <= -5 THEN 'COOLING'
        ELSE 'PERSISTENT'
    END AS signal_state
FROM lagged;
