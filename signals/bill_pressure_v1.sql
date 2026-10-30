-- Bill Pressure v1: strong-evidence bill-level pressure and quarter-over-quarter movement.
-- Uses financial quarter, never dt_posted. Filing dollars are allocated across DISTINCT
-- qualifying bills mentioned in that filing, conserving the eligible filing-dollar universe.
-- Associated amounts are analytical exposure, NOT direct spending on a bill.
WITH params AS (SELECT 2025::int filing_year,.850::numeric min_confidence),
eligible AS (
 SELECT DISTINCT f.filing_type,f.filing_uuid,f.client_id,bm.activity_id,bm.congress,bm.bill_type,bm.bill_number
 FROM lda.filing f JOIN lda.activity a USING(filing_uuid) JOIN policy.bill_mention bm USING(activity_id)
 CROSS JOIN params p
 WHERE f.filing_year=p.filing_year AND f.filing_type IN('Q1','Q2','Q3','Q4')
 AND bm.resolution_status IN('candidate','verified') AND coalesce(bm.resolution_confidence,0)>=p.min_confidence
), filing_bill AS (
 SELECT DISTINCT filing_type,filing_uuid,client_id,congress,bill_type,bill_number FROM eligible
), filing_bill_count AS (
 SELECT filing_type,filing_uuid,count(*)::numeric bill_count FROM filing_bill GROUP BY 1,2
), allocated AS (
 SELECT fb.*,coalesce(f.income,f.expenses,0)::numeric/nullif(n.bill_count,0) associated_amount
 FROM filing_bill fb JOIN filing_bill_count n USING(filing_type,filing_uuid) JOIN lda.filing f USING(filing_uuid)
), activity_breadth AS (
 SELECT filing_type,congress,bill_type,bill_number,count(distinct activity_id) activities
 FROM eligible GROUP BY 1,2,3,4
), raw AS (
 SELECT a.filing_type,a.congress,a.bill_type,a.bill_number,max(b.title) title,
 count(distinct a.filing_uuid) filings,count(distinct a.client_id) clients,
 max(ab.activities) activities,sum(a.associated_amount) associated_amount
 FROM allocated a JOIN policy.bill b USING(congress,bill_type,bill_number)
 JOIN activity_breadth ab USING(filing_type,congress,bill_type,bill_number)
 GROUP BY 1,2,3,4
), scored AS (
 SELECT *,round((100*(.40*percent_rank()over(partition by filing_type order by associated_amount)
 +.30*percent_rank()over(partition by filing_type order by clients)
 +.20*percent_rank()over(partition by filing_type order by activities)
 +.10*percent_rank()over(partition by filing_type order by filings)))::numeric,1) bill_pressure
 FROM raw
), lagged AS (
 SELECT *,lag(filing_type)over(partition by congress,bill_type,bill_number order by filing_type) prior_period,
 lag(bill_pressure)over(partition by congress,bill_type,bill_number order by filing_type) prior_pressure,
 lag(clients)over(partition by congress,bill_type,bill_number order by filing_type) prior_clients,
 lag(associated_amount)over(partition by congress,bill_type,bill_number order by filing_type) prior_amount
 FROM scored
)
SELECT filing_type,congress,bill_type,bill_number,title,filings,clients,activities,
 round(associated_amount,2) associated_amount,bill_pressure,
 prior_pressure,round((bill_pressure-prior_pressure)::numeric,1) pressure_change,
 prior_clients,(clients-prior_clients) client_change,
 CASE WHEN prior_amount>0 THEN round((100*(associated_amount-prior_amount)/prior_amount)::numeric,1) END associated_amount_qoq_pct
FROM lagged ORDER BY filing_type DESC,bill_pressure DESC,associated_amount DESC;
