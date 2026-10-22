-- Activity-text canonical policy signals v1.
-- Deterministic multi-label extraction. Filing amounts are divided across distinct
-- matched subissues and are analytical allocations, not direct policy spending.
WITH base AS (
 SELECT f.filing_type,f.filing_uuid,f.client_id,a.activity_id,a.description
 FROM lda.activity a JOIN lda.filing f USING(filing_uuid)
 WHERE f.filing_year=2025 AND f.filing_type IN('Q1','Q2','Q3','Q4')
), labels AS (
 SELECT filing_type,filing_uuid,client_id,activity_id,label
 FROM base b CROSS JOIN LATERAL (VALUES
 ('DRUG_340B', b.description ~* '340B'),
 ('TRADE_TARIFFS', b.description ~* '(tariff|Section 301|Section 232)'),
 ('EXPORT_CONTROLS', b.description ~* '(export control|Export Administration Regulations|Bureau of Industry and Security)'),
 ('DRUG_PRICING', b.description ~* '(drug pricing|prescription drug pric|Medicare.*negotiat|negotiat.*drug)'),
 ('CMS_MEDICARE_PAYMENT', b.description ~* '(Medicare Physician Fee Schedule|physician fee schedule|CMS payment|Medicare payment|reimbursement rate)'),
 ('TAX_RD', b.description ~* '(research and development tax|R&D tax|research.*tax credit|Section 174)'),
 ('TAX_CORPORATE', b.description ~* '(corporate tax|corporate income tax|business tax)'),
 ('TAX_INTERNATIONAL', b.description ~* '(international tax|GILTI|FDII|BEAT|Pillar Two|OECD.*tax)'),
 ('DEFENSE_APPROPRIATIONS', b.description ~* '(defense appropriations|Department of Defense appropriations)'),
 ('ENERGY_WATER_APPROPRIATIONS', b.description ~* '(Energy and Water.*appropriation|Energy & Water.*appropriation)'),
 ('THUD_APPROPRIATIONS', b.description ~* '(Transportation.*HUD.*appropriation|THUD.*appropriation)'),
 ('EARMARKS_CPF_CDS', b.description ~* '(Community Project Funding|Congressionally Directed Spending)')
 ) v(label,is_match) WHERE is_match
), fl AS (SELECT DISTINCT filing_type,filing_uuid,client_id,label FROM labels),
n AS (SELECT filing_type,filing_uuid,count(*)::numeric n FROM fl GROUP BY 1,2),
allocated AS (
 SELECT fl.*,coalesce(f.income,f.expenses,0)::numeric/n.n associated_amount
 FROM fl JOIN n USING(filing_type,filing_uuid) JOIN lda.filing f USING(filing_uuid)
), breadth AS (
 SELECT filing_type,label,count(distinct activity_id) activities FROM labels GROUP BY 1,2
), raw AS (
 SELECT a.filing_type,a.label,count(distinct a.filing_uuid) filings,count(distinct a.client_id) clients,
 max(b.activities) activities,sum(a.associated_amount) associated_amount
 FROM allocated a JOIN breadth b USING(filing_type,label) GROUP BY 1,2
), scored AS (
 SELECT *,round((100*(.45*percent_rank()over(partition by filing_type order by associated_amount)
 +.30*percent_rank()over(partition by filing_type order by clients)
 +.25*percent_rank()over(partition by filing_type order by activities)))::numeric,1) policy_pressure
 FROM raw
), lagged AS (
 SELECT *,lag(policy_pressure)over(partition by label order by filing_type) prior_pressure,
 lag(clients)over(partition by label order by filing_type) prior_clients,
 lag(associated_amount)over(partition by label order by filing_type) prior_amount
 FROM scored
)
SELECT filing_type,label,filings,clients,activities,round(associated_amount,2) associated_amount,policy_pressure,
 prior_pressure,round((policy_pressure-prior_pressure)::numeric,1) pressure_change,
 prior_clients,(clients-prior_clients) client_change,
 CASE WHEN prior_amount>0 THEN round((100*(associated_amount-prior_amount)/prior_amount)::numeric,1) END associated_amount_qoq_pct
FROM lagged ORDER BY filing_type DESC,policy_pressure DESC;