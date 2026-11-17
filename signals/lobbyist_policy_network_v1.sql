-- Lobbyist-policy network v1 (read-only)
-- Links registered lobbyists to context policy evidence, clients, strong bill mentions,
-- and bill committees. This is an association network, NOT proof that a lobbyist
-- contacted a specific member of Congress.
-- Filing amounts must never be attributed to an individual lobbyist.

WITH base AS (
 SELECT DISTINCT
   e.policy_code,e.activity_id,al.lobbyist_id,f.client_id,f.filing_uuid,
   f.filing_year,f.filing_type
 FROM policy.activity_policy_evidence e
 JOIN lda.activity a ON a.activity_id=e.activity_id
 JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
 JOIN lda.activity_lobbyist al ON al.activity_id=e.activity_id
 WHERE e.classifier_version='permitting-child-v1'
),
lobbyist_policy AS (
 SELECT policy_code,lobbyist_id,filing_type,
   count(DISTINCT activity_id) activities,
   count(DISTINCT client_id) clients,
   count(DISTINCT filing_uuid) filings
 FROM base GROUP BY 1,2,3
),
bill_paths AS (
 SELECT DISTINCT
   b.policy_code,b.lobbyist_id,b.client_id,b.activity_id,
   bm.congress,bm.bill_type,bm.bill_number
 FROM base b
 JOIN policy.bill_mention bm ON bm.activity_id=b.activity_id
 WHERE bm.resolution_confidence>=0.85
),
committee_paths AS (
 SELECT DISTINCT
   bp.policy_code,bp.lobbyist_id,bp.client_id,bp.activity_id,
   bp.congress,bp.bill_type,bp.bill_number,bc.committee_code
 FROM bill_paths bp
 JOIN policy.bill_committee bc
 ON bc.congress=bp.congress
 AND bc.bill_type=bp.bill_type
 AND bc.bill_number=bp.bill_number
),
q4 AS (
 SELECT
   lp.policy_code,lp.lobbyist_id,lp.activities,lp.clients,lp.filings,
   count(DISTINCT (bp.congress,bp.bill_type,bp.bill_number)) strong_bills,
   count(DISTINCT cp.committee_code) committees
 FROM lobbyist_policy lp
 LEFT JOIN bill_paths bp
   ON bp.policy_code=lp.policy_code AND bp.lobbyist_id=lp.lobbyist_id
 LEFT JOIN committee_paths cp
   ON cp.policy_code=lp.policy_code AND cp.lobbyist_id=lp.lobbyist_id
 WHERE lp.filing_type='Q4'
 GROUP BY 1,2,3,4,5
)
SELECT q.policy_code,q.lobbyist_id,
 concat_ws(' ',l.first_name,l.middle_name,l.last_name,l.suffix) lobbyist,
 q.activities,q.clients,q.filings,q.strong_bills,q.committees,
 percent_rank() OVER(PARTITION BY q.policy_code ORDER BY q.clients) client_breadth_pct,
 percent_rank() OVER(PARTITION BY q.policy_code ORDER BY q.activities) activity_intensity_pct
FROM q4 q
JOIN lda.lobbyist l ON l.lobbyist_id=q.lobbyist_id
ORDER BY q.policy_code,q.clients DESC,q.activities DESC,q.strong_bills DESC;
