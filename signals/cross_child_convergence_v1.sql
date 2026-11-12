-- Cross-child convergence detector v1 (read-only prototype)
-- Detects coordinated movement among context children of a canonical parent.
-- Multi-label child evidence is not additive financial exposure.

WITH cq AS (
 SELECT cp.parent_policy_code,e.policy_code child_policy_code,f.filing_type quarter,
        count(DISTINCT e.activity_id)::numeric activities,
        count(DISTINCT f.client_id)::numeric clients
 FROM policy.activity_policy_evidence e
 JOIN policy.canonical_policy cp ON cp.policy_code=e.policy_code
 JOIN lda.activity a ON a.activity_id=e.activity_id
 JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
 WHERE cp.parent_policy_code IS NOT NULL AND cp.allocation_role='context'
   AND f.filing_year=2025
 GROUP BY 1,2,3
), movement AS (
 SELECT parent_policy_code,child_policy_code,
        max(activities) FILTER(WHERE quarter='Q3') q3_activities,
        max(activities) FILTER(WHERE quarter='Q4') q4_activities,
        max(clients) FILTER(WHERE quarter='Q3') q3_clients,
        max(clients) FILTER(WHERE quarter='Q4') q4_clients
 FROM cq GROUP BY 1,2
), accel AS (
 SELECT *,CASE WHEN q4_activities>q3_activities AND q4_clients>q3_clients THEN 1 ELSE 0 END breadth_accelerating
 FROM movement
), bp AS (
 SELECT DISTINCT cp.parent_policy_code,e.policy_code child_policy_code,
        bm.congress,bm.bill_type,bm.bill_number
 FROM policy.activity_policy_evidence e
 JOIN policy.canonical_policy cp ON cp.policy_code=e.policy_code
 JOIN lda.activity a ON a.activity_id=e.activity_id
 JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
 JOIN policy.bill_mention bm ON bm.activity_id=e.activity_id
 WHERE cp.parent_policy_code IS NOT NULL AND cp.allocation_role='context'
   AND f.filing_year=2025 AND f.filing_type='Q4'
   AND bm.resolution_confidence>=0.85
), shared_bills AS (
 SELECT parent_policy_code,congress,bill_type,bill_number,count(DISTINCT child_policy_code) child_count
 FROM bp GROUP BY 1,2,3,4 HAVING count(DISTINCT child_policy_code)>=3
), cp AS (
 SELECT DISTINCT bp.parent_policy_code,bp.child_policy_code,bc.committee_code
 FROM bp JOIN policy.bill_committee bc
 ON bc.congress=bp.congress AND bc.bill_type=bp.bill_type AND bc.bill_number=bp.bill_number
), shared_committees AS (
 SELECT parent_policy_code,committee_code,count(DISTINCT child_policy_code) child_count
 FROM cp GROUP BY 1,2 HAVING count(DISTINCT child_policy_code)>=3
), ps AS (
 SELECT parent_policy_code,count(*) child_count,sum(breadth_accelerating) accelerating_children,
        round(sum(breadth_accelerating)::numeric/nullif(count(*),0),3) accelerating_child_share
 FROM accel GROUP BY 1
)
SELECT ps.*,
 (SELECT count(*) FROM shared_bills s WHERE s.parent_policy_code=ps.parent_policy_code) shared_strong_bills,
 (SELECT count(*) FROM shared_committees s WHERE s.parent_policy_code=ps.parent_policy_code) shared_committees,
 CASE WHEN accelerating_child_share>=0.60
       AND (SELECT count(*) FROM shared_bills s WHERE s.parent_policy_code=ps.parent_policy_code)>=2
       AND (SELECT count(*) FROM shared_committees s WHERE s.parent_policy_code=ps.parent_policy_code)>=2
      THEN 'CROSS_CHILD_CONVERGENCE' ELSE 'NO_ALERT' END campaign_state
FROM ps ORDER BY accelerating_child_share DESC,parent_policy_code;
