-- Activity-policy resolver v3 DESIGN / SHADOW ONLY.
-- Do not promote directly to production.
--
-- v3 changes corroboration from "distinct evidence methods" to
-- "distinct independent evidence families".
--
-- CRITICAL SEMANTICS
-- 1. bill_anchor and bill_provision refinement are the SAME family:
--    legislative_reference.
-- 2. Provision refinement must not create a second independent vote.
-- 3. Allocation eligibility is unchanged from v2.
-- 4. This layer does not allocate or multiply filing dollars.
--
-- Storage-light POC design: evidence_family is derived, not persisted in
-- policy.activity_policy_evidence.

WITH institutional_target_map(policy_code, entity_pattern) AS (
    -- Keep synchronized with signals/institutional_target_policy_map_v1.csv.
    -- Only mappings that passed the 2026-11-10 specificity/control audit belong here.
    VALUES
        ('TRADE_TARIFFS','U.S. Trade Representative%'),
        ('TRADE_TARIFFS','Intl Trade Administration%'),
        ('TRADE_TARIFFS','U.S. Customs & Border Protection%'),
        ('FARM_BILL','Agriculture, Dept of%'),
        ('MEDICAID','Centers For Medicare and Medicaid Services%'),
        ('RAIL_SAFETY','Federal Railroad Administration'),
        ('AUTONOMOUS_VEHICLES','Natl Highway Traffic Safety Administration (NHTSA)'),
        ('BANK_CAPITAL','Federal Reserve System'),
        ('PATENT_POLICY','Patent & Trademark Office (PTO)'),
        ('BROADBAND_BEAD','Natl Telecommunications & Information Administration (NTIA)'),
        ('EXPORT_CONTROLS','Bureau of Industry and Security (BIS)'),
        ('FAA_AVIATION','Federal Aviation Administration (FAA)'),
        ('FOREST_WILDFIRE','U.S. Forest Service'),
        ('DIGITAL_ASSETS','Commodity Futures Trading Commission (CFTC)'),
        ('DIGITAL_ASSETS','Securities & Exchange Commission (SEC)'),
        ('WATER_INFRASTRUCTURE','Bureau of Reclamation'),
        ('TELEHEALTH','Substance Abuse & Mental Health Services Administration (SAMHSA)'),
        ('PERMITTING_NEPA','Council on Environmental Quality (CEQ)'),
        ('AI_POLICY','Office of Science & Technology Policy (OSTP)')
),
stored_evidence AS (
    SELECT
        e.activity_id,
        e.policy_code,
        e.evidence_method,
        e.classifier_version,
        e.confidence,
        e.evidence_key,
        CASE
            WHEN e.evidence_method = 'bill_anchor'
                THEN 'legislative_reference'
            WHEN e.evidence_method = 'activity_regex'
                THEN 'activity_text'
            WHEN e.evidence_method = 'corpus_anchor'
                THEN 'corpus_context'
            WHEN e.evidence_method = 'semantic'
                THEN 'semantic_model'
            ELSE 'unknown'
        END AS evidence_family
    FROM policy.activity_policy_evidence e
),
institutional_evidence AS (
    -- Derived shadow evidence: no rows are inserted into activity_policy_evidence.
    -- Government targets are separately reported LDA fields and therefore can
    -- provide an independent institutional family when the mapping is specific.
    SELECT DISTINCT
        e.activity_id,
        m.policy_code,
        'institutional_target'::text AS evidence_method,
        'institutional-target-v1'::text AS classifier_version,
        NULL::numeric AS confidence,
        g.name::text AS evidence_key,
        'institutional_target'::text AS evidence_family
    FROM policy.activity_policy_evidence e
    JOIN institutional_target_map m
      ON m.policy_code = e.policy_code
    JOIN lda.activity_government_entity age
      ON age.activity_id = e.activity_id
    JOIN lda.government_entity g
      ON g.government_entity_id = age.government_entity_id
    WHERE g.name LIKE m.entity_pattern
),
evidence_base AS (
    SELECT * FROM stored_evidence
    UNION ALL
    SELECT * FROM institutional_evidence
),
unknown_guard AS (
    -- Kept as a CTE so audit SQL can assert this count is zero.
    SELECT count(*) AS unknown_evidence_rows
    FROM evidence_base
    WHERE evidence_family = 'unknown'
),
agg AS (
    SELECT
        activity_id,
        policy_code,
        MAX(confidence) AS best_confidence,
        COUNT(*) AS evidence_count,
        COUNT(DISTINCT evidence_method) AS evidence_method_count,
        COUNT(DISTINCT evidence_family)
            FILTER (WHERE evidence_family <> 'unknown') AS evidence_family_count,
        string_agg(DISTINCT evidence_method, ',' ORDER BY evidence_method)
            AS evidence_methods,
        string_agg(DISTINCT evidence_family, ',' ORDER BY evidence_family)
            FILTER (WHERE evidence_family <> 'unknown') AS evidence_families
    FROM evidence_base
    GROUP BY 1,2
),
enriched AS (
    SELECT
        a.*,
        cp.policy_level,
        cp.allocation_role,
        cp.parent_policy_code,
        (a.evidence_family_count >= 2) AS independently_corroborated,
        -- Compatibility diagnostic only. Do not use this as v3 corroboration.
        (a.evidence_method_count >= 2) AS legacy_method_corroborated
    FROM agg a
    JOIN policy.canonical_policy cp USING (policy_code)
)
SELECT
    e.*,
    CASE
        WHEN e.allocation_role = 'substantive' THEN true
        WHEN e.allocation_role = 'parent' THEN NOT EXISTS (
            SELECT 1
            FROM enriched ch
            WHERE ch.activity_id = e.activity_id
              AND ch.parent_policy_code = e.policy_code
        )
        ELSE false
    END AS allocation_eligible
FROM enriched e;

-- Expected materialized shadow shape:
-- policy.activity_policy_resolved_v3_shadow
--
-- UNIQUE(activity_id, policy_code)
--
-- Promotion gate:
-- SELECT count(*) FROM unknown_guard must equal zero.
