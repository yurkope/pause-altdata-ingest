-- Committee Pressure acceleration v1.
-- Quarter-to-quarter only; never uses dt_posted for economic timing.
-- Strong evidence is confidence >= .85. Associated dollars are not direct committee spend.
WITH params AS (SELECT .850::numeric min_confidence),
ff AS (
 SELECT f.*,make_date(f.filing_year,CASE f.filing_type WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END,1) quarter_start
 FROM lda.filing f WHERE f.filing_type IN ('Q1','Q2','Q3','Q4')
), e AS (
 SELECT DISTINCT ff.quarter_start,ff.filing_uuid,bm.activity_id,bc.committee_code,bm.congress,bm.bill_type,bm.bill_number
 FROM ff JOIN lda.activity a USING(filing_uuid) JOIN policy.bill_mention bm USING(activity_id)
 JOIN policy.bill_committee bc USING(congress,bill_type,bill_number) CROSS JOIN params p
 WHERE bm.resolution_status IN ('candidate','verified') AND coalesce(bm.resolution_confidence,0)>=p.min_confidence
 AND coalesce((bc.activity_json->>'pressure_eligible')::boolean,false)
), fc AS (SELECT DISTINCT quarter_start,filing_uuid,committee_code FROM e),
n AS (SELECT quarter_start,filing_uuid,count(*)::numeric n FROM fc GROUP BY 1,2),
al AS (
 SELECT fc.quarter_start,fc.committee_code,fc.filing_uuid,coalesce(ff.income,ff.expenses,0)::numeric/n.n amt
 FROM fc JOIN n USING(quarter_start,filing_uuid) JOIN ff USING(quarter_start,filing_uuid)
), cb AS (
 SELECT fc.quarter_start,fc.committee_code,count(distinct ff.client_id) clients FROM fc JOIN ff USING(quarter_start,filing_uuid) GROUP BY 1,2
), bb AS (
 SELECT quarter_start,committee_code,count(distinct(congress,bill_type,bill_number)) bills FROM e GROUP BY 1,2
), lb AS (
 SELECT e.quarter_start,e.committee_code,count(distinct x.lobbyist_id) lobbyists FROM e JOIN lda.activity_lobbyist x USING(activity_id) GROUP BY 1,2
), raw AS (
 SELECT al.quarter_start,c.committee_code,c.chamber,c.name committee_name,count(distinct al.filing_uuid) filings,
 cb.clients,lb.lobbyists,bb.bills,sum(al.amt) amt
 FROM al JOIN policy.committee c USING(committee_code) JOIN cb USING(quarter_start,committee_code)
 JOIN lb USING(quarter_start,committee_code) JOIN bb USING(quarter_start,committee_code)
 GROUP BY 1,2,3,4,6,7,8
), r AS (
 SELECT *,percent_rank()over(partition by quarter_start order by amt) ap,
 percent_rank()over(partition by quarter_start order by clients) cp,
 percent_rank()over(partition by quarter_start order by lobbyists) lp,
 percent_rank()over(partition by quarter_start order by bills) bp FROM raw
), s AS (
 SELECT *,round((100*(.45*ap+.25*cp+.20*lp+.10*bp))::numeric,1) pressure_score FROM r
), l AS (
 SELECT *,lag(quarter_start)over(partition by committee_code order by quarter_start) prior_quarter,
 lag(pressure_score)over(partition by committee_code order by quarter_start) prior_score,
 lag(amt)over(partition by committee_code order by quarter_start) prior_amt FROM s
)
SELECT quarter_start,committee_code,chamber,committee_name,filings,clients,lobbyists,bills,
 round(amt,2) associated_allocated_amount,pressure_score,
 CASE WHEN prior_quarter=quarter_start-interval '3 months' THEN prior_score END prior_pressure_score,
 CASE WHEN prior_quarter=quarter_start-interval '3 months' THEN round((pressure_score-prior_score)::numeric,1) END pressure_acceleration,
 CASE WHEN prior_quarter=quarter_start-interval '3 months' AND prior_amt>0 THEN round((100*(amt-prior_amt)/prior_amt)::numeric,1) END associated_amount_qoq_pct
FROM l ORDER BY quarter_start DESC,pressure_score DESC,amt DESC;
