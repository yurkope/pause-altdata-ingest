-- Canonical Policy Pressure v1
-- Strong-evidence (>=0.85) bill anchors -> controlled canonical subissues.
-- Unmapped bills are retained as UNKNOWN_OTHER for coverage auditing.
-- Filing dollars are allocated across DISTINCT canonical subissues within each filing,
-- conserving the classified filing-dollar universe.
WITH params AS (SELECT 2025::int filing_year,.850::numeric min_confidence),
bill_map AS (
 SELECT congress,bill_type,bill_number,title,
 CASE
  -- Explicit anchors for acronym-only bills whose titles are not semantically self-describing.
  WHEN congress=119 AND bill_type='HR' AND bill_number=4776 THEN 'PERMITTING_ENV_REVIEW' -- SPEED Act
  WHEN congress=119 AND bill_type='HR' AND bill_number=2392 THEN 'DIGITAL_ASSETS' -- STABLE Act
  WHEN congress=119 AND bill_type='HR' AND bill_number=1566 THEN 'AUTO_REPAIR_RIGHT_TO_REPAIR' -- REPAIR Act
  WHEN congress=119 AND bill_type='S' AND bill_number=1379 THEN 'AUTO_REPAIR_RIGHT_TO_REPAIR' -- REPAIR Act
  WHEN title ~* '(permit|NEPA|environmental review|deployment review|regulatory clarity)' THEN 'PERMITTING_ENV_REVIEW'
  WHEN title ~* '(forest|wildfire|sequoia)' THEN 'FORESTS_WILDFIRE'
  WHEN title ~* '(kids online|online privacy|children and teens|parents over platforms|COPPA|TAKE IT DOWN|App Store Accountability|NO FAKES)' THEN 'ONLINE_SAFETY_PRIVACY'
  WHEN title ~* '(artificial intelligence|(^|[^A-Za-z])AI([^A-Za-z]|$))' THEN 'AI_POLICY'
  WHEN title ~* '(health care (cost|premium|affordability)|PBM|pharmacy benefit|copay|Patients Deserve Price Tags)' THEN 'HEALTHCARE_COSTS_PBM'
  WHEN title ~* '(Medicare|Seniors.*Access to Care|CONNECT for Health|Telehealth|Medically Unnecessary Delays|Safe Step|Dialysis Patients|Resident Physician Shortage|Maternal Deaths|Kids.*Access to Care)' THEN 'HEALTHCARE_ACCESS_DELIVERY'
  WHEN title ~* '(Affordable Prescriptions|Affordable Generics|Biosimilars|pharmacist services)' THEN 'DRUG_ACCESS_COMPETITION'
  WHEN title ~* '(national defense authorization|NDAA)' THEN 'DEFENSE_AUTHORIZATION'
  WHEN title ~* 'department of defense appropriations' THEN 'DEFENSE_APPROPRIATIONS'
  WHEN title ~* '(appropriations act|continuing appropriations)' THEN 'APPROPRIATIONS_OTHER'
  WHEN title ~* '(digital asset|GENIUS Act|stablecoin|crypto)' THEN 'DIGITAL_ASSETS'
  WHEN title ~* '(FDA|orphan cures|give kids a chance|drug|pharmaceutical)' THEN 'DRUG_BIOMEDICAL_POLICY'
  WHEN title ~* '(Main Street Tax|Death Tax|R&D Competitiveness|tax certainty)' THEN 'TAX_BUSINESS_INNOVATION'
  WHEN title ~* '(Cybersecurity|cyber security|cyber)' THEN 'CYBERSECURITY'
  WHEN title ~* '(Patent Eligibility|PREVAIL Act)' THEN 'IP_PATENT_POLICY'
  WHEN title ~* '(Protecting the Right to Organize|Labor Contracts|Protect America.s Workforce|Save Local Business)' THEN 'LABOR_EMPLOYMENT'
  WHEN title ~* '(GRID Power|Reliable Power|Energy Choice|Cross-border Energy Infrastructure|PIPES Act|PIPELINE Safety)' THEN 'ENERGY_GRID_INFRASTRUCTURE'
  WHEN title ~* '(SHIPS for America|Coast Guard Authorization)' THEN 'MARITIME_SHIPPING'
  WHEN title ~* 'Combating Organized Retail Crime' THEN 'RETAIL_CRIME'
  WHEN title ~* '(PFAS|ESA Amendments)' THEN 'ENVIRONMENTAL_REGULATION'
  WHEN title ~* '(recycling infrastructure|recycling technology)' THEN 'RECYCLING_CIRCULAR_ECONOMY'
  WHEN title ~* 'housing' THEN 'HOUSING'
  WHEN title ~* '(aviation|FAA)' THEN 'AVIATION'
  ELSE 'UNKNOWN_OTHER' END canonical_subissue,
 'bill_title_regex_v1'::text classification_method
 FROM policy.bill
), eligible AS (
 SELECT DISTINCT f.filing_type,f.filing_uuid,f.client_id,bm.activity_id,bm.congress,bm.bill_type,bm.bill_number,
 bm.resolution_confidence,m.canonical_subissue,m.classification_method
 FROM lda.filing f JOIN lda.activity a USING(filing_uuid) JOIN policy.bill_mention bm USING(activity_id)
 JOIN bill_map m USING(congress,bill_type,bill_number) CROSS JOIN params p
 WHERE f.filing_year=p.filing_year AND f.filing_type IN('Q1','Q2','Q3','Q4')
 AND bm.resolution_status IN('candidate','verified') AND coalesce(bm.resolution_confidence,0)>=p.min_confidence
), classified AS (SELECT * FROM eligible WHERE canonical_subissue<>'UNKNOWN_OTHER'),
filing_subissue AS (
 SELECT DISTINCT filing_type,filing_uuid,client_id,canonical_subissue FROM classified
), n AS (SELECT filing_type,filing_uuid,count(*)::numeric n FROM filing_subissue GROUP BY 1,2),
allocated AS (
 SELECT fs.*,coalesce(f.income,f.expenses,0)::numeric/nullif(n.n,0) associated_amount
 FROM filing_subissue fs JOIN n USING(filing_type,filing_uuid) JOIN lda.filing f USING(filing_uuid)
), breadth AS (
 SELECT filing_type,canonical_subissue,count(distinct activity_id) activities,
 count(distinct (bill_type,bill_number)) bills
 FROM classified GROUP BY 1,2
), raw AS (
 SELECT a.filing_type,a.canonical_subissue,count(distinct a.filing_uuid) filings,
 count(distinct a.client_id) clients,max(b.activities) activities,max(b.bills) bills,
 sum(a.associated_amount) associated_amount
 FROM allocated a JOIN breadth b USING(filing_type,canonical_subissue) GROUP BY 1,2
), scored AS (
 SELECT *,round((100*(.40*percent_rank()over(partition by filing_type order by associated_amount)
 +.25*percent_rank()over(partition by filing_type order by clients)
 +.20*percent_rank()over(partition by filing_type order by activities)
 +.15*percent_rank()over(partition by filing_type order by bills)))::numeric,1) policy_pressure
 FROM raw
), lagged AS (
 SELECT *,lag(filing_type)over(partition by canonical_subissue order by filing_type) prior_period,
 lag(policy_pressure)over(partition by canonical_subissue order by filing_type) prior_pressure,
 lag(clients)over(partition by canonical_subissue order by filing_type) prior_clients,
 lag(associated_amount)over(partition by canonical_subissue order by filing_type) prior_amount
 FROM scored
)
SELECT filing_type,canonical_subissue,filings,clients,activities,bills,round(associated_amount,2) associated_amount,
 policy_pressure,prior_pressure,round((policy_pressure-prior_pressure)::numeric,1) pressure_change,
 prior_clients,(clients-prior_clients) client_change,
 CASE WHEN prior_amount>0 THEN round((100*(associated_amount-prior_amount)/prior_amount)::numeric,1) END associated_amount_qoq_pct
FROM lagged ORDER BY filing_type DESC,policy_pressure DESC;
