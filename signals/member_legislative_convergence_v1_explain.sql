-- Member Legislative Convergence v1: explainability/decomposition layer
-- Read-only. Shows the bills, clients, and registered lobbyists that create a
-- member's legislative-pathway convergence.
--
-- IMPORTANT: every member relationship here is sponsor/cosponsor. The LDA data
-- does not identify named congressional offices, so these paths do NOT establish
-- direct contact between a lobbyist/client and a member.
-- No filing-level dollars are attributed to members or lobbyists.
--
-- Set params to the period/member of interest. NULL bioguide_id returns all members.
WITH params AS (
    SELECT
        2025::int AS filing_year,
        'fourth_quarter'::text AS quarter,
        119::int AS congress,
        NULL::text AS bioguide_id
),
child_evidence AS (
    SELECT DISTINCT e.activity_id,e.policy_code
    FROM policy.activity_policy_evidence e
    WHERE e.classifier_version='permitting-child-v1'
),
paths AS (
    SELECT DISTINCT
        f.filing_year,
        f.filing_period AS quarter,
        ce.policy_code,
        mb.bioguide_id,
        mb.relationship_type,
        bm.congress,bm.bill_type,bm.bill_number,
        a.activity_id,
        f.client_id,
        al.lobbyist_id
    FROM params z
    JOIN child_evidence ce ON true
    JOIN lda.activity a ON a.activity_id=ce.activity_id
    JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
    JOIN policy.bill_mention bm
      ON bm.activity_id=a.activity_id
     AND bm.resolution_confidence>=0.85
    JOIN policy.bill_member mb
      ON mb.congress=bm.congress
     AND mb.bill_type=bm.bill_type
     AND mb.bill_number=bm.bill_number
     AND NOT mb.is_withdrawn
    LEFT JOIN lda.activity_lobbyist al ON al.activity_id=a.activity_id
    WHERE f.filing_year=z.filing_year
      AND f.filing_period=z.quarter
      AND bm.congress=z.congress
      AND (z.bioguide_id IS NULL OR mb.bioguide_id=z.bioguide_id)
),
member_totals AS (
    SELECT
        bioguide_id,
        count(DISTINCT activity_id) AS member_connected_activities
    FROM paths
    GROUP BY 1
),
bill_decomposition AS (
    SELECT
        p.bioguide_id,
        p.bill_type,
        p.bill_number,
        count(DISTINCT p.policy_code) AS permitting_children,
        count(DISTINCT p.activity_id) AS connected_activities,
        count(DISTINCT p.client_id) AS connected_clients,
        count(DISTINCT p.lobbyist_id) AS connected_lobbyists,
        bool_or(p.relationship_type='SPONSOR') AS is_sponsor
    FROM paths p
    GROUP BY 1,2,3
),
ranked_bills AS (
    SELECT
        b.*,
        t.member_connected_activities,
        round(
          100.0*b.connected_activities/nullif(t.member_connected_activities,0),1
        ) AS member_activity_share_pct,
        row_number() OVER (
          PARTITION BY b.bioguide_id
          ORDER BY b.connected_activities DESC,b.connected_clients DESC,
                   b.bill_type,b.bill_number
        ) AS bill_rank
    FROM bill_decomposition b
    JOIN member_totals t USING(bioguide_id)
),
client_decomposition AS (
    SELECT
        p.bioguide_id,
        p.client_id,
        count(DISTINCT p.policy_code) AS permitting_children,
        count(DISTINCT (p.congress,p.bill_type,p.bill_number)) AS strong_bills,
        count(DISTINCT p.activity_id) AS connected_activities,
        count(DISTINCT p.lobbyist_id) AS connected_lobbyists
    FROM paths p
    WHERE p.client_id IS NOT NULL
    GROUP BY 1,2
),
ranked_clients AS (
    SELECT
        c.*,
        row_number() OVER (
          PARTITION BY c.bioguide_id
          ORDER BY c.connected_activities DESC,c.strong_bills DESC,c.client_id
        ) AS client_rank
    FROM client_decomposition c
),
lobbyist_decomposition AS (
    SELECT
        p.bioguide_id,
        p.lobbyist_id,
        count(DISTINCT p.policy_code) AS permitting_children,
        count(DISTINCT (p.congress,p.bill_type,p.bill_number)) AS strong_bills,
        count(DISTINCT p.activity_id) AS connected_activities,
        count(DISTINCT p.client_id) AS connected_clients
    FROM paths p
    WHERE p.lobbyist_id IS NOT NULL
    GROUP BY 1,2
),
ranked_lobbyists AS (
    SELECT
        l.*,
        row_number() OVER (
          PARTITION BY l.bioguide_id
          ORDER BY l.connected_activities DESC,l.connected_clients DESC,
                   l.strong_bills DESC,l.lobbyist_id
        ) AS lobbyist_rank
    FROM lobbyist_decomposition l
)
SELECT
    'BILL'::text AS component_type,
    m.bioguide_id,
    m.direct_order_name AS member_name,
    rb.bill_rank AS component_rank,
    rb.bill_type || ' ' || rb.bill_number::text AS component_name,
    rb.permitting_children,
    rb.connected_activities,
    rb.connected_clients,
    rb.connected_lobbyists,
    rb.member_activity_share_pct,
    CASE WHEN rb.is_sponsor THEN 'SPONSOR' ELSE 'COSPONSOR' END AS member_relationship
FROM ranked_bills rb
JOIN policy.member m USING(bioguide_id)
WHERE rb.bill_rank<=10

UNION ALL

SELECT
    'CLIENT',
    m.bioguide_id,
    m.direct_order_name,
    rc.client_rank,
    c.name,
    rc.permitting_children,
    rc.connected_activities,
    rc.strong_bills,
    rc.connected_lobbyists,
    NULL::numeric,
    'ANALYTICAL_PATH'
FROM ranked_clients rc
JOIN policy.member m USING(bioguide_id)
JOIN lda.client c ON c.client_id=rc.client_id
WHERE rc.client_rank<=10

UNION ALL

SELECT
    'LOBBYIST',
    m.bioguide_id,
    m.direct_order_name,
    rl.lobbyist_rank,
    trim(concat_ws(' ',l.first_name,l.middle_name,l.last_name,l.suffix)) AS component_name,
    rl.permitting_children,
    rl.connected_activities,
    rl.connected_clients,
    rl.strong_bills,
    NULL::numeric,
    'ANALYTICAL_PATH'
FROM ranked_lobbyists rl
JOIN policy.member m USING(bioguide_id)
JOIN lda.lobbyist l ON l.lobbyist_id=rl.lobbyist_id
WHERE rl.lobbyist_rank<=10

ORDER BY member_name,component_type,component_rank;
