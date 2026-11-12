-- Exact production manifest and promotion procedure for bill-policy-v2.
-- Promoted 2026-10-23 after audit on bill-multilabel-v2-audit.
-- Semantic rule: associated_allocation is an analytical allocation of filing-level disclosed amounts,
-- not money paid to or spent specifically on a bill, policy, committee, or person.
--
-- Expected bill stage: 26,988 rows; 11,335 activities; 24 policies; 272 bills.
-- Expected resolver: 75,397 pairs; 49,876 activities; 79 policies; 3,078 corroborated.
-- Expected change vs prior production resolver: +993 / -0.
-- Expected allocation conservation:
-- Q2 1,013,836,743.05; Q3 998,225,628.20; Q4 1,024,842,854.63.

BEGIN;

DROP TABLE IF EXISTS policy._bill_policy_map_promote_v2;
CREATE TABLE policy._bill_policy_map_promote_v2 (
  congress integer NOT NULL,
  bill_type text NOT NULL,
  bill_number integer NOT NULL,
  policy_code text NOT NULL REFERENCES policy.canonical_policy(policy_code),
  PRIMARY KEY (congress,bill_type,bill_number,policy_code)
);

INSERT INTO policy._bill_policy_map_promote_v2(congress,bill_type,bill_number,policy_code) VALUES
  (119, 'HR', 20, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 161, 'PERMITTING_NEPA'),
  (119, 'HR', 178, 'FOREST_WILDFIRE'),
  (119, 'HR', 247, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 262, 'FOREST_WILDFIRE'),
  (119, 'HR', 471, 'FOREST_WILDFIRE'),
  (119, 'HR', 539, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 573, 'PERMITTING_NEPA'),
  (119, 'HR', 582, 'FOREST_WILDFIRE'),
  (119, 'HR', 633, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 703, 'TAX_BUSINESS_INNOVATION'),
  (119, 'HR', 842, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 872, 'CYBERSECURITY'),
  (119, 'HR', 879, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 890, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 912, 'CYBERSECURITY'),
  (119, 'HR', 913, 'FAA_AVIATION'),
  (119, 'HR', 929, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 946, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 1000, 'CYBERSECURITY'),
  (119, 'HR', 1047, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 1078, 'HOUSING'),
  (119, 'HR', 1262, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 1267, 'ENVIRONMENTAL_REGULATION'),
  (119, 'HR', 1301, 'TAX_BUSINESS_INNOVATION'),
  (119, 'HR', 1319, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 1366, 'PERMITTING_NEPA'),
  (119, 'HR', 1407, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1492, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 1509, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1566, 'RIGHT_TO_REPAIR'),
  (119, 'HR', 1574, 'PATENT_POLICY'),
  (119, 'HR', 1614, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1650, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1655, 'FOREST_WILDFIRE'),
  (119, 'HR', 1681, 'PERMITTING_NEPA'),
  (119, 'HR', 1683, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1709, 'CYBERSECURITY'),
  (119, 'HR', 1766, 'CYBERSECURITY'),
  (119, 'HR', 1818, 'FAA_AVIATION'),
  (119, 'HR', 1843, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 1897, 'ENVIRONMENTAL_REGULATION'),
  (119, 'HR', 1899, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1909, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 1923, 'FOREST_WILDFIRE'),
  (119, 'HR', 1968, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 1981, 'HOUSING'),
  (119, 'HR', 1990, 'TAX_BUSINESS_INNOVATION'),
  (119, 'HR', 2111, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 2145, 'RECYCLING_CIRCULAR_ECONOMY'),
  (119, 'HR', 2199, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 2214, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 2385, 'AI_POLICY'),
  (119, 'HR', 2392, 'DIGITAL_ASSETS'),
  (119, 'HR', 2433, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 2450, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 2483, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 2550, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 2591, 'FAA_AVIATION'),
  (119, 'HR', 2592, 'FAA_AVIATION'),
  (119, 'HR', 2709, 'FOREST_WILDFIRE'),
  (119, 'HR', 2725, 'HOUSING'),
  (119, 'HR', 2757, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 2794, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 2821, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 2840, 'HOUSING'),
  (119, 'HR', 2853, 'RETAIL_CRIME'),
  (119, 'HR', 3010, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 3062, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 3069, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3149, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 3151, 'MARITIME_SHIPPING'),
  (119, 'HR', 3152, 'PATENT_POLICY'),
  (119, 'HR', 3160, 'PATENT_POLICY'),
  (119, 'HR', 3164, 'DRUG_ACCESS_COMPETITION'),
  (119, 'HR', 3183, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3410, 'FAA_AVIATION'),
  (119, 'HR', 3419, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3434, 'AI_POLICY'),
  (119, 'HR', 3467, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3493, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 3514, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3593, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3616, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 3632, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 3633, 'DIGITAL_ASSETS'),
  (119, 'HR', 3699, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 3838, 'DEFENSE_AUTHORIZATION'),
  (119, 'HR', 3890, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 3898, 'PERMITTING_NEPA'),
  (119, 'HR', 3935, 'PERMITTING_NEPA'),
  (119, 'HR', 3944, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4016, 'DEFENSE_APPROPRIATIONS'),
  (119, 'HR', 4121, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4121, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 4206, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 4213, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4249, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4273, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 4275, 'MARITIME_SHIPPING'),
  (119, 'HR', 4313, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 4317, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 4366, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 4475, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 4503, 'PERMITTING_NEPA'),
  (119, 'HR', 4552, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4552, 'HOUSING'),
  (119, 'HR', 4553, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4570, 'PATENT_POLICY'),
  (119, 'HR', 4576, 'HOUSING'),
  (119, 'HR', 4619, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 4659, 'HOUSING'),
  (119, 'HR', 4710, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 4731, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 4754, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4776, 'PERMITTING_NEPA'),
  (119, 'HR', 4779, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 4896, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 5081, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 5166, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 5301, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'HR', 5304, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 5316, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 5342, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 5343, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 5371, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 5408, 'LABOR_EMPLOYMENT'),
  (119, 'HR', 5450, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 5451, 'FAA_AVIATION'),
  (119, 'HR', 5454, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 5455, 'FAA_AVIATION'),
  (119, 'HR', 5509, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 5582, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 5784, 'AI_POLICY'),
  (119, 'HR', 5885, 'AI_POLICY'),
  (119, 'HR', 6086, 'FAA_AVIATION'),
  (119, 'HR', 6160, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 6166, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'HR', 6291, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 6333, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 6373, 'PERMITTING_NEPA'),
  (119, 'HR', 6423, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 6484, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'HR', 6500, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 6566, 'RECYCLING_CIRCULAR_ECONOMY'),
  (119, 'HR', 6607, 'FAA_AVIATION'),
  (119, 'HR', 6609, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'HR', 6644, 'HOUSING'),
  (119, 'HR', 6703, 'HEALTHCARE_COSTS_PBM'),
  (119, 'HR', 6875, 'AI_POLICY'),
  (119, 'HR', 6938, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 7006, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 7148, 'APPROPRIATIONS_OTHER'),
  (119, 'HR', 8580, 'FOREST_WILDFIRE'),
  (119, 'HR', 9495, 'DEFENSE_APPROPRIATIONS'),
  (119, 'HR', 10136, 'FOREST_WILDFIRE'),
  (119, 'S', 46, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 106, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 140, 'FOREST_WILDFIRE'),
  (119, 'S', 146, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 213, 'TAX_BUSINESS_INNOVATION'),
  (119, 'S', 245, 'CYBERSECURITY'),
  (119, 'S', 266, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 339, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 355, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 394, 'DIGITAL_ASSETS'),
  (119, 'S', 441, 'FOREST_WILDFIRE'),
  (119, 'S', 470, 'HOUSING'),
  (119, 'S', 524, 'MARITIME_SHIPPING'),
  (119, 'S', 526, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 544, 'PERMITTING_NEPA'),
  (119, 'S', 587, 'TAX_BUSINESS_INNOVATION'),
  (119, 'S', 652, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 668, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 705, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 708, 'PATENT_POLICY'),
  (119, 'S', 752, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 754, 'CYBERSECURITY'),
  (119, 'S', 763, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 836, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 844, 'LABOR_EMPLOYMENT'),
  (119, 'S', 852, 'LABOR_EMPLOYMENT'),
  (119, 'S', 864, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 866, 'PERMITTING_NEPA'),
  (119, 'S', 890, 'HOUSING'),
  (119, 'S', 902, 'FOREST_WILDFIRE'),
  (119, 'S', 919, 'DIGITAL_ASSETS'),
  (119, 'S', 924, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 932, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1040, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1041, 'DRUG_ACCESS_COMPETITION'),
  (119, 'S', 1045, 'FAA_AVIATION'),
  (119, 'S', 1050, 'FOREST_WILDFIRE'),
  (119, 'S', 1071, 'DEFENSE_AUTHORIZATION'),
  (119, 'S', 1096, 'DRUG_ACCESS_COMPETITION'),
  (119, 'S', 1097, 'PATENT_POLICY'),
  (119, 'S', 1173, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1260, 'HOUSING'),
  (119, 'S', 1261, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1299, 'HOUSING'),
  (119, 'S', 1302, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1337, 'CYBERSECURITY'),
  (119, 'S', 1367, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 1379, 'RIGHT_TO_REPAIR'),
  (119, 'S', 1404, 'RETAIL_CRIME'),
  (119, 'S', 1414, 'DRUG_ACCESS_COMPETITION'),
  (119, 'S', 1462, 'FOREST_WILDFIRE'),
  (119, 'S', 1482, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1506, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1515, 'HOUSING'),
  (119, 'S', 1527, 'HOUSING'),
  (119, 'S', 1541, 'MARITIME_SHIPPING'),
  (119, 'S', 1546, 'PATENT_POLICY'),
  (119, 'S', 1553, 'PATENT_POLICY'),
  (119, 'S', 1582, 'DIGITAL_ASSETS'),
  (119, 'S', 1586, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 1587, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1631, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1640, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1699, 'AI_POLICY'),
  (119, 'S', 1717, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1748, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 1759, 'FAA_AVIATION'),
  (119, 'S', 1775, 'AI_POLICY'),
  (119, 'S', 1776, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1785, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1792, 'AI_POLICY'),
  (119, 'S', 1816, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1818, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1862, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 1874, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 1875, 'CYBERSECURITY'),
  (119, 'S', 1921, 'HOUSING'),
  (119, 'S', 1945, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'S', 1996, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2068, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 2121, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2153, 'ONLINE_SAFETY_PRIVACY'),
  (119, 'S', 2207, 'DIGITAL_ASSETS'),
  (119, 'S', 2228, 'LABOR_EMPLOYMENT'),
  (119, 'S', 2237, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2256, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2256, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 2257, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2292, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 2296, 'DEFENSE_AUTHORIZATION'),
  (119, 'S', 2354, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2355, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 2363, 'HOUSING'),
  (119, 'S', 2367, 'AI_POLICY'),
  (119, 'S', 2416, 'HOUSING'),
  (119, 'S', 2420, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 2426, 'DRUG_ACCESS_COMPETITION'),
  (119, 'S', 2431, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2439, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2465, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2465, 'HOUSING'),
  (119, 'S', 2480, 'CYBERSECURITY'),
  (119, 'S', 2528, 'AI_POLICY'),
  (119, 'S', 2572, 'DEFENSE_APPROPRIATIONS'),
  (119, 'S', 2587, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2613, 'LABOR_EMPLOYMENT'),
  (119, 'S', 2615, 'AI_POLICY'),
  (119, 'S', 2651, 'HOUSING'),
  (119, 'S', 2709, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2837, 'LABOR_EMPLOYMENT'),
  (119, 'S', 2879, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2882, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 2903, 'HEALTHCARE_ACCESS_DELIVERY'),
  (119, 'S', 2975, 'ENERGY_GRID_INFRASTRUCTURE'),
  (119, 'S', 2983, 'CYBERSECURITY'),
  (119, 'S', 3044, 'FOREST_WILDFIRE'),
  (119, 'S', 3122, 'DRUG_BIOMEDICAL_POLICY'),
  (119, 'S', 3150, 'AI_POLICY'),
  (119, 'S', 3251, 'CYBERSECURITY'),
  (119, 'S', 3257, 'FAA_AVIATION'),
  (119, 'S', 3290, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 3293, 'APPROPRIATIONS_OTHER'),
  (119, 'S', 3345, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 3385, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 3389, 'HEALTHCARE_COSTS_PBM'),
  (119, 'S', 3435, 'FAA_AVIATION'),
  (119, 'S', 4532, 'FOREST_WILDFIRE'),
  (119, 'S', 5579, 'HOUSING');

DROP TABLE IF EXISTS policy._bill_anchor_promote_v2;
CREATE TABLE policy._bill_anchor_promote_v2 AS
SELECT DISTINCT
  bm.activity_id,
  m.policy_code,
  bm.resolution_confidence::numeric(4,3) AS confidence,
  format('bill:%s:%s:%s',bm.congress,bm.bill_type,bm.bill_number) AS evidence_key,
  LEFT(COALESCE(bm.evidence_context,a.description,''),2000) AS evidence_text,
  bm.congress AS source_congress,
  bm.bill_type AS source_bill_type,
  bm.bill_number AS source_bill_number,
  f.filing_type,
  f.filing_uuid
FROM policy.bill_mention bm
JOIN policy._bill_policy_map_promote_v2 m USING(congress,bill_type,bill_number)
JOIN lda.activity a USING(activity_id)
JOIN lda.filing f USING(filing_uuid)
WHERE f.filing_year=2025
  AND f.filing_type IN('Q2','Q3','Q4')
  AND bm.resolution_status IN('candidate','verified')
  AND COALESCE(bm.resolution_confidence,0)>=.85;

CREATE UNIQUE INDEX _bill_anchor_promote_v2_pk
ON policy._bill_anchor_promote_v2(activity_id,policy_code,evidence_key);

DELETE FROM policy.activity_policy_evidence WHERE classifier_version='bill-policy-v2';
INSERT INTO policy.activity_policy_evidence(
  activity_id,policy_code,evidence_method,confidence,evidence_key,evidence_text,
  classifier_version,source_congress,source_bill_type,source_bill_number
)
SELECT activity_id,policy_code,'bill_anchor',confidence,evidence_key,evidence_text,
       'bill-policy-v2',source_congress,source_bill_type,source_bill_number
FROM policy._bill_anchor_promote_v2;

DROP TABLE IF EXISTS policy._activity_policy_resolved_promote_v2;
CREATE TABLE policy._activity_policy_resolved_promote_v2 AS
WITH ev AS (
  SELECT * FROM policy.activity_policy_evidence
  WHERE classifier_version IN('activity-policy-v1','bill-policy-v2')
), agg AS (
  SELECT activity_id,policy_code,MAX(confidence) best_confidence,
         COUNT(DISTINCT evidence_method) method_count,
         string_agg(DISTINCT evidence_method,',' ORDER BY evidence_method) evidence_methods,
         COUNT(*) evidence_count
  FROM ev GROUP BY 1,2
), enriched AS (
  SELECT a.*,cp.policy_level,cp.allocation_role,cp.parent_policy_code,
         (a.method_count>1) corroborated
  FROM agg a JOIN policy.canonical_policy cp USING(policy_code)
)
SELECT e.*,
       CASE WHEN e.allocation_role='substantive' THEN true
            WHEN e.allocation_role='parent' THEN NOT EXISTS (
              SELECT 1 FROM enriched ch
              WHERE ch.activity_id=e.activity_id AND ch.parent_policy_code=e.policy_code
            )
            ELSE false END AS allocation_eligible
FROM enriched e;

CREATE UNIQUE INDEX _activity_policy_resolved_promote_v2_pk
ON policy._activity_policy_resolved_promote_v2(activity_id,policy_code);

DROP TABLE IF EXISTS policy._filing_policy_allocation_promote_v2;
CREATE TABLE policy._filing_policy_allocation_promote_v2 AS
WITH fp AS (
  SELECT DISTINCT f.filing_type,f.filing_uuid,r.policy_code,r.allocation_role
  FROM policy._activity_policy_resolved_promote_v2 r
  JOIN lda.activity a ON a.activity_id=r.activity_id
  JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
  WHERE f.filing_year=2025
    AND f.filing_type IN('Q2','Q3','Q4')
    AND r.allocation_role<>'context'
), leaf AS (
  SELECT fp.*
  FROM fp
  WHERE allocation_role='substantive'
     OR (allocation_role='parent' AND NOT EXISTS (
       SELECT 1 FROM fp ch
       JOIN policy.canonical_policy cp ON cp.policy_code=ch.policy_code
       WHERE ch.filing_uuid=fp.filing_uuid AND cp.parent_policy_code=fp.policy_code
     ))
), counted AS (
  SELECT leaf.*,COUNT(*) OVER(PARTITION BY filing_uuid)::numeric label_count FROM leaf
)
SELECT c.filing_type,c.filing_uuid,c.policy_code,c.label_count,
       COALESCE(f.income,f.expenses,0)::numeric/c.label_count AS associated_allocation
FROM counted c JOIN lda.filing f USING(filing_uuid);

CREATE UNIQUE INDEX _filing_policy_allocation_promote_v2_pk
ON policy._filing_policy_allocation_promote_v2(filing_uuid,policy_code);

-- Validate these staging outputs against the expected counts above before swapping.
DROP TABLE policy.activity_policy_resolved_v2;
ALTER TABLE policy._activity_policy_resolved_promote_v2 RENAME TO activity_policy_resolved_v2;
ALTER INDEX policy._activity_policy_resolved_promote_v2_pk RENAME TO activity_policy_resolved_v2_pkey;
CREATE INDEX activity_policy_resolved_v2_policy_idx
ON policy.activity_policy_resolved_v2(policy_code,allocation_eligible);

DROP TABLE policy.filing_policy_allocation_v2;
ALTER TABLE policy._filing_policy_allocation_promote_v2 RENAME TO filing_policy_allocation_v2;
ALTER INDEX policy._filing_policy_allocation_promote_v2_pk RENAME TO filing_policy_allocation_v2_pkey;
CREATE INDEX filing_policy_allocation_v2_policy_idx
ON policy.filing_policy_allocation_v2(filing_type,policy_code);

COMMIT;

-- Post-promotion verification.
SELECT count(*) rows,count(distinct activity_id) activities,count(distinct policy_code) policies,
       count(distinct(source_congress,source_bill_type,source_bill_number)) bills
FROM policy.activity_policy_evidence WHERE classifier_version='bill-policy-v2';

SELECT count(*) pairs,count(distinct activity_id) activities,count(distinct policy_code) policies,
       count(*) FILTER(WHERE corroborated) corroborated
FROM policy.activity_policy_resolved_v2;

SELECT filing_type,count(distinct filing_uuid) eligible_filings,count(*) pairs,
       round(sum(associated_allocation),2) allocation
FROM policy.filing_policy_allocation_v2 GROUP BY 1 ORDER BY 1;
