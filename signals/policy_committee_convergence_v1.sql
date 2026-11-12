-- Policy -> Bill -> Committee convergence v1
-- One row per quarter x substantive policy x pressure-eligible committee.
-- Strong bill evidence only: candidate/verified with resolution confidence >= .85.
-- Joins Policy Pressure v2 to Committee Pressure and exposes intersection breadth,
-- committee concentration, QoQ changes, and the dominant legislative anchor.
-- No composite convergence score is imposed in v1; component metrics stay auditable.
-- Committee associated amounts remain filing-level analytical allocations, not direct spend.

WITH policy_pressure AS (
    SELECT *,
        make_date(2025, CASE filing_type WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END, 1) quarter_start
    FROM policy.policy_pressure_v2
),
committee_evidence AS (
    SELECT DISTINCT
        make_date(f.filing_year, CASE f.filing_type WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4 WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10 END, 1) quarter_start,
        f.filing_type,f.filing_uuid,f.client_id,bm.activity_id,bc.committee_code,
        bm.congress,bm.bill_type,bm.bill_number
    FROM policy.bill_mention bm
    JOIN policy.bill_committee bc USING (congress,bill_type,bill_number)
    JOIN lda.activity a USING (activity_id)
    JOIN lda.filing f USING (filing_uuid)
    WHERE f.filing_year=2025
      AND f.filing_type IN ('Q1','Q2','Q3','Q4')
      AND bm.resolution_status IN ('candidate','verified')
      AND COALESCE(bm.resolution_confidence,0)>=.85
      AND COALESCE((bc.activity_json->>'pressure_eligible')::boolean,false)
),
policy_committee AS (
    SELECT DISTINCT ce.*,r.policy_code
    FROM committee_evidence ce
    JOIN policy.activity_policy_resolved_v2 r ON r.activity_id=ce.activity_id
    JOIN policy.canonical_policy cp ON cp.policy_code=r.policy_code
    WHERE cp.allocation_role='substantive'
),
committee_totals AS (
    SELECT quarter_start,committee_code,
        COUNT(DISTINCT activity_id) committee_activities,
        COUNT(DISTINCT filing_uuid) committee_filings,
        COUNT(DISTINCT client_id) committee_clients
    FROM committee_evidence GROUP BY 1,2
),
intersection AS (
    SELECT quarter_start,filing_type,policy_code,committee_code,
        COUNT(DISTINCT activity_id) linked_activities,
        COUNT(DISTINCT filing_uuid) linked_filings,
        COUNT(DISTINCT client_id) linked_clients
    FROM policy_committee GROUP BY 1,2,3,4
),
bill_counts AS (
    SELECT quarter_start,policy_code,committee_code,congress,bill_type,bill_number,
        COUNT(DISTINCT activity_id) activities,
        COUNT(DISTINCT filing_uuid) filings,
        COUNT(DISTINCT client_id) clients
    FROM policy_committee GROUP BY 1,2,3,4,5,6
),
dominant_bill AS (
    SELECT * FROM (
        SELECT bill_counts.*,
            ROW_NUMBER() OVER (
                PARTITION BY quarter_start,policy_code,committee_code
                ORDER BY activities DESC,filings DESC,clients DESC,congress DESC,bill_type,bill_number
            ) rn
        FROM bill_counts
    ) x WHERE rn=1
),
filing_committee AS (
    SELECT DISTINCT quarter_start,filing_uuid,committee_code FROM committee_evidence
),
filing_n AS (
    SELECT quarter_start,filing_uuid,COUNT(*)::numeric n FROM filing_committee GROUP BY 1,2
),
committee_alloc AS (
    SELECT fc.quarter_start,fc.committee_code,fc.filing_uuid,
        COALESCE(f.income,f.expenses,0)::numeric/fn.n amount
    FROM filing_committee fc
    JOIN filing_n fn ON fn.quarter_start=fc.quarter_start AND fn.filing_uuid=fc.filing_uuid
    JOIN lda.filing f ON f.filing_uuid=fc.filing_uuid
),
client_breadth AS (
    SELECT fc.quarter_start,fc.committee_code,COUNT(DISTINCT f.client_id) clients
    FROM filing_committee fc JOIN lda.filing f ON f.filing_uuid=fc.filing_uuid GROUP BY 1,2
),
bill_breadth AS (
    SELECT quarter_start,committee_code,COUNT(DISTINCT (congress,bill_type,bill_number)) bills
    FROM committee_evidence GROUP BY 1,2
),
lobbyist_breadth AS (
    SELECT ce.quarter_start,ce.committee_code,COUNT(DISTINCT al.lobbyist_id) lobbyists
    FROM committee_evidence ce JOIN lda.activity_lobbyist al ON al.activity_id=ce.activity_id GROUP BY 1,2
),
committee_raw AS (
    SELECT ca.quarter_start,ca.committee_code,SUM(ca.amount) amount,
        cb.clients,lb.lobbyists,bb.bills
    FROM committee_alloc ca
    JOIN client_breadth cb ON cb.quarter_start=ca.quarter_start AND cb.committee_code=ca.committee_code
    JOIN lobbyist_breadth lb ON lb.quarter_start=ca.quarter_start AND lb.committee_code=ca.committee_code
    JOIN bill_breadth bb ON bb.quarter_start=ca.quarter_start AND bb.committee_code=ca.committee_code
    GROUP BY 1,2,4,5,6
),
committee_scored AS (
    SELECT *,
        ROUND((100*(.45*PERCENT_RANK() OVER(PARTITION BY quarter_start ORDER BY amount)
                   +.25*PERCENT_RANK() OVER(PARTITION BY quarter_start ORDER BY clients)
                   +.20*PERCENT_RANK() OVER(PARTITION BY quarter_start ORDER BY lobbyists)
                   +.10*PERCENT_RANK() OVER(PARTITION BY quarter_start ORDER BY bills)))::numeric,1) committee_pressure
    FROM committee_raw
),
committee_lagged AS (
    SELECT *,
        LAG(quarter_start) OVER(PARTITION BY committee_code ORDER BY quarter_start) prior_quarter,
        LAG(committee_pressure) OVER(PARTITION BY committee_code ORDER BY quarter_start) prior_pressure
    FROM committee_scored
)
SELECT i.quarter_start,i.filing_type,i.policy_code,
    pp.pressure_score policy_pressure,pp.score_change policy_pressure_change,
    i.committee_code,c.name committee_name,c.chamber,
    cs.committee_pressure,
    CASE WHEN cs.prior_quarter=i.quarter_start-interval '3 months'
         THEN ROUND(cs.committee_pressure-cs.prior_pressure,1) END committee_pressure_change,
    i.linked_activities,i.linked_filings,i.linked_clients,
    ct.committee_activities,ct.committee_filings,ct.committee_clients,
    ROUND(100.0*i.linked_activities/NULLIF(ct.committee_activities,0),1) committee_activity_share_pct,
    ROUND(100.0*i.linked_filings/NULLIF(ct.committee_filings,0),1) committee_filing_share_pct,
    ROUND(100.0*i.linked_clients/NULLIF(ct.committee_clients,0),1) committee_client_share_pct,
    db.congress dominant_bill_congress,db.bill_type dominant_bill_type,db.bill_number dominant_bill_number,
    b.title dominant_bill_title,db.activities dominant_bill_activities,
    db.filings dominant_bill_filings,db.clients dominant_bill_clients
FROM intersection i
JOIN policy_pressure pp ON pp.quarter_start=i.quarter_start AND pp.policy_code=i.policy_code
JOIN committee_totals ct ON ct.quarter_start=i.quarter_start AND ct.committee_code=i.committee_code
JOIN committee_lagged cs ON cs.quarter_start=i.quarter_start AND cs.committee_code=i.committee_code
JOIN policy.committee c ON c.committee_code=i.committee_code
LEFT JOIN dominant_bill db ON db.quarter_start=i.quarter_start AND db.policy_code=i.policy_code AND db.committee_code=i.committee_code
LEFT JOIN policy.bill b ON b.congress=db.congress AND b.bill_type=db.bill_type AND b.bill_number=db.bill_number;
