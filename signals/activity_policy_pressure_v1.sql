-- Activity-text canonical policy signals v1.
-- Deterministic multi-label extraction. Filing amounts are divided across distinct
-- matched subissues and are analytical allocations, not direct policy spending.
WITH base AS (
 SELECT f.filing_type,f.filing_uuid,f.client_id,a.activity_id,a.general_issue_code,a.description
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
 ('EARMARKS_CPF_CDS', b.description ~* '(Community Project Funding|Congressionally Directed Spending)'),
 ('MEDICAID', b.description ~* 'Medicaid' AND b.general_issue_code IN('HCR','MMM','BUD','INS')),
 ('CRITICAL_MINERALS_SUPPLY_CHAIN', b.description ~* '(critical mineral|critical material|rare earth|supply chain)' AND b.general_issue_code IN('TRD','MAN','DEF','ENG','NAT','SCI','ECN')),
 ('AI_POLICY', b.description ~* '(artificial intelligence|AI regulation|AI policy|AI governance)' AND b.general_issue_code IN('SCI','CPI','TEC','COM','CSP','GOV','CPT','DEF')),
 ('CLEAN_ENERGY_TAX_IRA', b.description ~* '(Inflation Reduction Act|clean energy tax credit|45Q|45V|45X|48E|45Y)' AND b.general_issue_code IN('TAX','ENG','ENV','MAN','TRA','TRD')),
 ('CYBERSECURITY', b.description ~* '(cybersecurity|cyber security|CISA)' AND b.general_issue_code IN('HOM','CPI','SCI','TEC','DEF','GOV','BAN','FIN')),
 ('FARM_BILL', b.description ~* 'Farm Bill' AND b.general_issue_code IN('AGR','FOO','BUD','NAT','TRD','WEL')),
 ('PERMITTING_NEPA', b.description ~* '(NEPA|National Environmental Policy Act|permitting reform|permit reform)' AND b.general_issue_code IN('ENV','NAT','ENG','TRA','CAW','MAN','UTI','AGR')),
 ('DATA_PRIVACY', b.description ~* '(data privacy|consumer privacy|privacy legislation|American Privacy Rights Act)' AND b.general_issue_code IN('CPI','TEC','COM','CSP','SCI','CPT','GOV')),
 ('DIGITAL_ASSETS', b.description ~* '(digital asset|cryptocurrency|crypto|stablecoin)' AND b.general_issue_code IN('FIN','BAN','MON','CPI','SCI','TAX')),
 ('WATER_INFRASTRUCTURE', b.description ~* '(water infrastructure|drinking water|wastewater)' AND b.general_issue_code IN('CAW','ENV','TRA','UTI','BUD','NAT')),
 ('TELEHEALTH', b.description ~* '(telehealth|telemedicine)' AND b.general_issue_code IN('HCR','MMM','MED','TEC','INS','VET')),
 ('FOREST_WILDFIRE', b.description ~* '(wildfire|forest management)' AND b.general_issue_code IN('NAT','ENV','AGR','BUD','DIS')),
 ('SNAP_NUTRITION', b.description ~* '(SNAP|Supplemental Nutrition Assistance Program)' AND b.general_issue_code IN('AGR','FOO','WEL','BUD','HCR')),
 ('PFAS', b.description ~* '(PFAS|per- and polyfluoroalkyl)' AND b.general_issue_code IN('ENV','CAW','CHM','DEF','NAT','WAS')),
 ('CHIPS_SEMICONDUCTORS', b.description ~* '(CHIPS( and Science)? Act|semiconductor)' AND b.general_issue_code IN('MAN','SCI','CPI','TRD','DEF','TAX','ECN')),
 ('FAA_AVIATION', b.description ~* '(FAA reauthorization|Federal Aviation Administration)' AND b.general_issue_code IN('AVI','TRA','AER','BUD')),
 ('PBM', b.description ~* '(pharmacy benefit manager|PBM)' AND b.general_issue_code IN('HCR','MMM','INS','PHA')),
 ('ACA', b.description ~* '(Affordable Care Act|ACA marketplace|ACA exchange)' AND b.general_issue_code IN('HCR','INS','MMM','BUD')),
 ('PRIOR_AUTH', b.description ~* '(prior authorization|prior auth)' AND b.general_issue_code IN('HCR','MMM','INS','MED')),
 ('SITE_NEUTRAL', b.description ~* '(site-neutral|site neutral)' AND b.general_issue_code IN('HCR','MMM','BUD')),
 ('STUDENT_LOANS', b.description ~* '(student loan|student debt)' AND b.general_issue_code IN('EDU','FIN','BUD')),
 ('CREDIT_CARD_INTERCHANGE', b.description ~* '(interchange fee|credit card competition|credit card fee)' AND b.general_issue_code IN('BAN','FIN','CSP')),
 ('FENTANYL_OPIOIDS', b.description ~* '(fentanyl|opioid)' AND b.general_issue_code IN('HCR','LAW','HOM','PHA','MED')),
 ('NUCLEAR_ENERGY', b.description ~* '(nuclear energy|nuclear power|advanced nuclear|small modular reactor|SMR)' AND b.general_issue_code IN('ENG','ENV','SCI','UTI','NAT')),
 ('BROADBAND_BEAD', b.description ~* '(BEAD|Broadband Equity, Access, and Deployment|broadband deployment)' AND b.general_issue_code IN('TEC','COM','CPI','BUD')),
 ('PATENT_POLICY', b.description ~* '(patent eligibility|patent reform|USPTO|Patent Trial and Appeal Board|PTAB)' AND b.general_issue_code IN('CPT','SCI','HCR','PHA')),
 ('RARE_DISEASE', b.description ~* '(rare disease|orphan drug)' AND b.general_issue_code IN('HCR','MED','PHA','SCI')),
 ('ANTITRUST', b.description ~* '(antitrust|competition policy|merger review)' AND b.general_issue_code IN('LBR','LAW','FIN','CSP','CPI','TEC')),
 ('CARBON_CAPTURE', b.description ~* '(carbon capture|CCUS|carbon sequestration)' AND b.general_issue_code IN('ENG','ENV','TAX','NAT')),
 ('AUTONOMOUS_VEHICLES', b.description ~* '(autonomous vehicle|self-driving)' AND b.general_issue_code IN('TRA','AUT','SCI','CSP')),
 ('HYDROGEN', b.description ~* '(clean hydrogen|hydrogen tax|hydrogen hub|45V)' AND b.general_issue_code IN('ENG','ENV','TAX','FUE')),
 ('LNG', b.description ~* '(liquefied natural gas|LNG export|LNG)' AND b.general_issue_code IN('ENG','FUE','TRD','ENV')),
 ('BANK_CAPITAL', b.description ~* '(Basel III|bank capital|capital requirements)' AND b.general_issue_code IN('BAN','FIN')),
 ('RIGHT_TO_REPAIR', b.description ~* 'right to repair' AND b.general_issue_code IN('AUT','CSP','CPT','MAN','TRA')),
 ('RAIL_SAFETY', b.description ~* '(rail safety|railroad safety)' AND b.general_issue_code IN('RRR','TRA')),
 ('BUDGET_RECONCILIATION_OBBBA', b.description ~* '(One Big Beautiful Bill Act|H[.]R[.]? ?1)' AND b.general_issue_code IN('BUD','TAX','HCR','MMM','ENG','EDU','AGR','IMM','HOM','FIN')),
 ('INFRASTRUCTURE_IIJA', b.description ~* 'Infrastructure Investment and Jobs Act' AND b.general_issue_code IN('TRA','ENG','TEC','CAW','BUD','ECN','UTI','ROD','RRR')),
 ('CONTINUING_APPROPRIATIONS', b.description ~* '(Full-Year Continuing Appropriations and Extensions Act|Continuing Appropriations and Extensions Act)' AND b.general_issue_code IN('BUD','HCR','HOM','DEF','EDU','TRA','AGR')),
 ('HOMELAND_SECURITY_APPROPRIATIONS', b.description ~* 'Department of Homeland Security Appropriations Act' AND b.general_issue_code IN('BUD','HOM','IMM','DIS','AVI')),
 ('FIN_SERVICES_APPROPRIATIONS', b.description ~* 'Financial Services and General Government Appropriations Act' AND b.general_issue_code IN('BUD','FIN','BAN','GOV')),
 ('WRDA_WATER_RESOURCES', b.description ~* 'Water Resources Development Act' AND b.general_issue_code IN('CAW','TRA','ENV','NAT','BUD','MAR')),
 ('RENEWABLE_FUEL_STANDARD', b.description ~* 'Renewable Fuel Standard' AND b.general_issue_code IN('AGR','ENG','ENV','FUE','CAW')),
 ('WORKFORCE_WIOA', b.description ~* 'Workforce Innovation and Opportunity Act' AND b.general_issue_code IN('EDU','LBR','BUD')),
 ('ROAD_TO_HOUSING', b.description ~* 'Road to Housing Act' AND b.general_issue_code IN('HOU','BAN','FIN','RES')),
 ('FIX_OUR_FORESTS', b.description ~* 'Fix Our Forests Act' AND b.general_issue_code IN('NAT','ENV','AGR','DIS')),
 ('AM_RADIO_VEHICLES', b.description ~* 'AM Radio for Every Vehicle Act' AND b.general_issue_code IN('AUT','COM','TEC','TRA','MAN')),
 ('TAX_CUTS_JOBS_ACT', b.description ~* 'Tax Cuts and Jobs Act' AND b.general_issue_code IN('TAX','FIN','BAN','ENG','MAN')),
 ('HIGHER_EDUCATION_ACT', b.description ~* 'Higher Education Act' AND b.general_issue_code IN('EDU','LBR')),
 ('NASA_AUTHORIZATION', b.description ~* 'NASA Transition Authorization Act' AND b.general_issue_code IN('AER','SCI')),
 ('BUY_AMERICA', b.description ~* 'Buy America Act' AND b.general_issue_code IN('TRA','BUD','ENG','HOU','ROD','UTI')),
 ('ENDANGERED_SPECIES_ACT', b.description ~* 'Endangered Species Act' AND b.general_issue_code IN('ENV','NAT','ANI','MAR','CAW')),
 ('VOTING_RIGHTS', b.description ~* 'John R[.] Lewis Voting Rights Advancement Act' AND b.general_issue_code IN('CIV','CON','GOV'))
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
), periodized AS (
 SELECT *,substring(filing_type from 2 for 1)::int quarter_num FROM scored
), lagged AS (
 SELECT *,lag(quarter_num)over(partition by label order by quarter_num) prior_quarter_num,
 lag(policy_pressure)over(partition by label order by quarter_num) lag_pressure,
 lag(clients)over(partition by label order by quarter_num) lag_clients,
 lag(associated_amount)over(partition by label order by quarter_num) lag_amount
 FROM periodized
)
SELECT filing_type,label,filings,clients,activities,round(associated_amount,2) associated_amount,policy_pressure,
 CASE WHEN prior_quarter_num=quarter_num-1 THEN lag_pressure END prior_pressure,
 CASE WHEN prior_quarter_num=quarter_num-1 THEN round((policy_pressure-lag_pressure)::numeric,1) END pressure_change,
 CASE WHEN prior_quarter_num=quarter_num-1 THEN lag_clients END prior_clients,
 CASE WHEN prior_quarter_num=quarter_num-1 THEN clients-lag_clients END client_change,
 CASE WHEN prior_quarter_num=quarter_num-1 AND lag_amount>0
      THEN round((100*(associated_amount-lag_amount)/lag_amount)::numeric,1) END associated_amount_qoq_pct,
 CASE
   WHEN prior_quarter_num IS NULL OR prior_quarter_num<>quarter_num-1 THEN 'NEW_EMERGING'
   WHEN policy_pressure-lag_pressure >= 5 THEN 'ACCELERATING'
   WHEN policy_pressure-lag_pressure <= -5 THEN 'COOLING'
   ELSE 'PERSISTENT'
 END signal_state
FROM lagged ORDER BY quarter_num DESC,policy_pressure DESC;