-- Hierarchy v3 permitting child classifier (audit/dry-run)
-- Multi-label by design: one PERMITTING_NEPA activity may emit multiple child labels.
-- These labels are evidence dimensions only. Do not sum filing dollars across child labels.

WITH permitting AS (
    SELECT DISTINCT
        r.activity_id,
        a.description,
        f.filing_type,
        f.client_id,
        f.filing_uuid
    FROM policy.activity_policy_resolved_v2 r
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    WHERE r.policy_code = 'PERMITTING_NEPA'
      AND f.filing_year = 2025
      AND f.filing_type IN ('Q1','Q2','Q3','Q4')
),
rules(child_policy_code, pattern) AS (
    VALUES
      ('NEPA_REVIEW', '(NEPA|National Environmental Policy Act|environmental review|environmental impact statement|environmental assessment)'),
      ('ENERGY_INFRA_PERMITTING', '(pipeline|natural gas|LNG|oil and gas|drilling|energy project|energy infrastructure|hydrogen|geothermal)'),
      ('MINING_PERMITTING', '(mine|mining|mineral|hardrock|critical mineral)'),
      ('TRANSMISSION_PERMITTING', '(transmission|electric grid|power line|interconnection)'),
      ('WATER_PERMITTING', '(Clean Water Act|section 404|water permit|wetland|WOTUS|waters of the United States)'),
      ('AIR_PERMITTING', '(New Source Review|Clean Air Act|air permit|air quality)'),
      ('BROADBAND_PERMITTING', '(broadband|telecom|wireless|communications deployment)')
),
matches AS (
    SELECT
        p.activity_id,p.filing_type,p.client_id,p.filing_uuid,
        r.child_policy_code,
        'activity_regex'::text AS evidence_method,
        0.950::numeric(4,3) AS confidence,
        'permit-child:' || lower(r.child_policy_code) AS evidence_key,
        left(p.description,2000) AS evidence_text,
        'permitting-child-v1'::text AS classifier_version
    FROM permitting p
    CROSS JOIN rules r
    WHERE p.description ~* r.pattern
)
SELECT * FROM matches
ORDER BY activity_id, child_policy_code;
