-- Institutional-target evidence candidate v1.
-- SHADOW / READ-ONLY DESIGN. Does NOT write activity_policy_evidence.
--
-- Purpose: test whether independently reported lobbying targets provide a useful
-- second evidence family for activity-policy pairs currently supported only by
-- activity text. This is "institutional_target", not another text classifier.
--
-- IMPORTANT: House/Senate and generic executive offices are intentionally
-- excluded. A match is a consistency signal, not yet production corroboration.

WITH expected(policy_code, entity_pattern) AS (
 VALUES
 ('TRADE_TARIFFS','U.S. Trade Representative%'),
 ('TRADE_TARIFFS','Intl Trade Administration%'),
 ('TRADE_TARIFFS','U.S. Customs & Border Protection%'),
 ('MEDICAID','Centers For Medicare and Medicaid Services%'),
 ('MEDICAID','Health & Human Services, Dept of%'),
 ('CRITICAL_MINERALS_SUPPLY_CHAIN','Energy, Dept of'),
 ('CRITICAL_MINERALS_SUPPLY_CHAIN','Bureau of Industry and Security%'),
 ('CRITICAL_MINERALS_SUPPLY_CHAIN','Defense, Dept of%'),
 ('CRITICAL_MINERALS_SUPPLY_CHAIN','U.S. Trade Representative%'),
 ('TAX_CORPORATE','Treasury, Dept of'),
 ('TAX_CORPORATE','Internal Revenue Service%'),
 ('CLEAN_ENERGY_TAX_IRA','Treasury, Dept of'),
 ('CLEAN_ENERGY_TAX_IRA','Internal Revenue Service%'),
 ('CLEAN_ENERGY_TAX_IRA','Energy, Dept of'),
 ('FARM_BILL','Agriculture, Dept of%'),
 ('DRUG_PRICING','Centers For Medicare and Medicaid Services%'),
 ('DRUG_PRICING','Health & Human Services, Dept of%'),
 ('DATA_PRIVACY','Federal Trade Commission%'),
 ('DATA_PRIVACY','Federal Communications Commission%')
), text_only AS (
 SELECT DISTINCT e.activity_id,e.policy_code
 FROM policy.activity_policy_evidence e
 WHERE e.evidence_method='activity_regex'
 AND NOT EXISTS (
   SELECT 1 FROM policy.activity_policy_evidence x
   WHERE x.activity_id=e.activity_id AND x.policy_code=e.policy_code
     AND x.evidence_method<>'activity_regex'
 )
), candidate AS (
 SELECT DISTINCT t.activity_id,t.policy_code,g.name government_entity
 FROM text_only t
 JOIN expected x USING(policy_code)
 JOIN lda.activity_government_entity age USING(activity_id)
 JOIN lda.government_entity g USING(government_entity_id)
 WHERE g.name LIKE x.entity_pattern
)
SELECT * FROM candidate;
