-- Audit companion for signals/permitting_child_multilabel_v1.sql
WITH permitting AS (
 SELECT DISTINCT r.activity_id,a.description,f.filing_type,f.client_id
 FROM policy.activity_policy_resolved_v2 r JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid)
 WHERE r.policy_code='PERMITTING_NEPA' AND f.filing_year=2025
),
rules(child_policy_code,pattern) AS (VALUES
 ('NEPA_REVIEW','(NEPA|National Environmental Policy Act|environmental review|environmental impact statement|environmental assessment)'),
 ('ENERGY_INFRA_PERMITTING','(pipeline|natural gas|LNG|oil and gas|drilling|energy project|energy infrastructure|hydrogen|geothermal)'),
 ('MINING_PERMITTING','(mine|mining|mineral|hardrock|critical mineral)'),
 ('TRANSMISSION_PERMITTING','(transmission|electric grid|power line|interconnection)'),
 ('WATER_PERMITTING','(Clean Water Act|section 404|water permit|wetland|WOTUS|waters of the United States)'),
 ('AIR_PERMITTING','(New Source Review|Clean Air Act|air permit|air quality)'),
 ('BROADBAND_PERMITTING','(broadband|telecom|wireless|communications deployment)')
), matches AS (
 SELECT p.activity_id,p.filing_type,p.client_id,r.child_policy_code
 FROM permitting p CROSS JOIN rules r WHERE p.description~*r.pattern
), counts AS (
 SELECT p.activity_id,p.filing_type,p.client_id,count(m.child_policy_code) label_count
 FROM permitting p LEFT JOIN matches m USING(activity_id,filing_type,client_id)
 GROUP BY 1,2,3
)
SELECT label_count,count(*) activities,count(distinct client_id) clients,
 count(*) FILTER(WHERE filing_type='Q4') q4_activities
FROM counts GROUP BY 1 ORDER BY 1;

-- Pairwise overlap:
WITH permitting AS (
 SELECT DISTINCT r.activity_id,a.description
 FROM policy.activity_policy_resolved_v2 r JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid)
 WHERE r.policy_code='PERMITTING_NEPA' AND f.filing_year=2025
), rules(child_policy_code,pattern) AS (VALUES
 ('NEPA_REVIEW','(NEPA|National Environmental Policy Act|environmental review|environmental impact statement|environmental assessment)'),
 ('ENERGY_INFRA_PERMITTING','(pipeline|natural gas|LNG|oil and gas|drilling|energy project|energy infrastructure|hydrogen|geothermal)'),
 ('MINING_PERMITTING','(mine|mining|mineral|hardrock|critical mineral)'),
 ('TRANSMISSION_PERMITTING','(transmission|electric grid|power line|interconnection)'),
 ('WATER_PERMITTING','(Clean Water Act|section 404|water permit|wetland|WOTUS|waters of the United States)'),
 ('AIR_PERMITTING','(New Source Review|Clean Air Act|air permit|air quality)'),
 ('BROADBAND_PERMITTING','(broadband|telecom|wireless|communications deployment)')
), m AS (
 SELECT p.activity_id,r.child_policy_code FROM permitting p CROSS JOIN rules r WHERE p.description~*r.pattern
)
SELECT a.child_policy_code child_a,b.child_policy_code child_b,count(*) activities
FROM m a JOIN m b ON a.activity_id=b.activity_id AND a.child_policy_code<b.child_policy_code
GROUP BY 1,2 ORDER BY activities DESC;
