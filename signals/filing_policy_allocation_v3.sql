-- Filing-policy allocation v3 DESIGN / SHADOW ONLY.
-- Dollar conservation is intentionally independent of corroboration strength.
--
-- A filing's disclosed amount is allocated once across distinct eligible leaf
-- policies. Evidence count/family count NEVER multiplies dollars.

WITH fp AS (
    SELECT DISTINCT
        f.filing_type,
        f.filing_uuid,
        r.policy_code,
        r.allocation_role
    FROM policy.activity_policy_resolved_v3_shadow r
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    WHERE f.filing_year = 2025
      AND f.filing_type IN ('Q1','Q2','Q3','Q4')
      AND r.allocation_role <> 'context'
),
leaf AS (
    SELECT fp.*
    FROM fp
    WHERE fp.allocation_role = 'substantive'
       OR (
            fp.allocation_role = 'parent'
            AND NOT EXISTS (
                SELECT 1
                FROM fp ch
                JOIN policy.canonical_policy cp
                  ON cp.policy_code = ch.policy_code
                WHERE ch.filing_uuid = fp.filing_uuid
                  AND cp.parent_policy_code = fp.policy_code
            )
       )
),
counted AS (
    SELECT
        leaf.*,
        COUNT(*) OVER (PARTITION BY filing_uuid)::numeric AS label_count
    FROM leaf
)
SELECT
    c.filing_type,
    c.filing_uuid,
    c.policy_code,
    c.label_count,
    COALESCE(f.income,f.expenses,0)::numeric / c.label_count
        AS associated_allocation
FROM counted c
JOIN lda.filing f USING (filing_uuid);

-- HARD INVARIANT:
-- For every filing represented in this allocation table:
-- SUM(associated_allocation) = COALESCE(f.income,f.expenses,0)
-- within rounding tolerance.
--
-- independently_corroborated is deliberately NOT a WHERE condition here.
