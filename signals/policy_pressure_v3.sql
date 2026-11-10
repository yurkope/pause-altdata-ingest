-- Policy Pressure v3 DESIGN / SHADOW ONLY.
--
-- v3 preserves v2 financial exposure and breadth metrics, then adds independent
-- corroboration as a quality dimension.
--
-- Design weights:
--   35% allocated financial exposure
--   25% client breadth
--   15% activity breadth
--   10% filing breadth
--   15% independent-evidence quality
--
-- Rationale: money remains the largest component; corroboration improves signal
-- quality without changing conserved dollar allocation.

WITH fa AS (
    SELECT a.*
    FROM policy.filing_policy_allocation_v3_shadow a
    JOIN policy.canonical_policy p USING (policy_code)
    WHERE p.allocation_role = 'substantive'
),
financial AS (
    SELECT
        fa.filing_type,
        CASE fa.filing_type
            WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2
            WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4
        END AS quarter_num,
        fa.policy_code,
        SUM(fa.associated_allocation)::numeric AS allocated_amount,
        COUNT(DISTINCT f.client_id) AS clients,
        COUNT(DISTINCT fa.filing_uuid) AS filings
    FROM fa
    JOIN lda.filing f USING (filing_uuid)
    GROUP BY 1,2,3
),
activity_quality AS (
    SELECT
        f.filing_type,
        r.policy_code,
        COUNT(DISTINCT r.activity_id) AS activities,
        COUNT(DISTINCT r.activity_id)
            FILTER (WHERE r.independently_corroborated) AS independently_corroborated_activities,
        AVG(r.evidence_family_count::numeric) AS avg_evidence_family_count
    FROM policy.activity_policy_resolved_v3_shadow r
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    WHERE f.filing_year = 2025
      AND f.filing_type IN ('Q1','Q2','Q3','Q4')
      AND EXISTS (
          SELECT 1
          FROM fa x
          WHERE x.filing_uuid = f.filing_uuid
            AND x.policy_code = r.policy_code
      )
    GROUP BY 1,2
),
raw AS (
    SELECT
        financial.*,
        COALESCE(aq.activities,0) AS activities,
        COALESCE(aq.independently_corroborated_activities,0)
            AS independently_corroborated_activities,
        COALESCE(aq.avg_evidence_family_count,0)::numeric
            AS avg_evidence_family_count,
        CASE
            WHEN COALESCE(aq.activities,0) = 0 THEN 0::numeric
            ELSE aq.independently_corroborated_activities::numeric / aq.activities
        END AS independent_corroboration_rate
    FROM financial
    LEFT JOIN activity_quality aq USING (filing_type,policy_code)
),
ranked AS (
    SELECT
        r.*,
        percent_rank() OVER (
            PARTITION BY filing_type ORDER BY allocated_amount
        ) AS amount_pct,
        percent_rank() OVER (
            PARTITION BY filing_type ORDER BY clients
        ) AS clients_pct,
        percent_rank() OVER (
            PARTITION BY filing_type ORDER BY activities
        ) AS activities_pct,
        percent_rank() OVER (
            PARTITION BY filing_type ORDER BY filings
        ) AS filings_pct,
        percent_rank() OVER (
            PARTITION BY filing_type ORDER BY independent_corroboration_rate
        ) AS corroboration_pct
    FROM raw r
),
scored AS (
    SELECT
        *,
        ROUND((
            100 * (
                0.35 * amount_pct +
                0.25 * clients_pct +
                0.15 * activities_pct +
                0.10 * filings_pct +
                0.15 * corroboration_pct
            )
        )::numeric,1) AS pressure_score_v3
    FROM ranked
),
lagged AS (
    SELECT
        s.*,
        lag(quarter_num) OVER (
            PARTITION BY policy_code ORDER BY quarter_num
        ) AS prior_quarter_num,
        lag(pressure_score_v3) OVER (
            PARTITION BY policy_code ORDER BY quarter_num
        ) AS prior_score
    FROM scored s
)
SELECT
    *,
    CASE
        WHEN prior_quarter_num = quarter_num - 1
        THEN ROUND(pressure_score_v3-prior_score,1)
    END AS score_change,
    CASE
        WHEN prior_quarter_num IS NULL
          OR prior_quarter_num <> quarter_num - 1 THEN 'NEW_EMERGING'
        WHEN pressure_score_v3-prior_score >= 5 THEN 'ACCELERATING'
        WHEN pressure_score_v3-prior_score <= -5 THEN 'COOLING'
        ELSE 'PERSISTENT'
    END AS signal_state
FROM lagged;
