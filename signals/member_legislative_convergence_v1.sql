-- Member Legislative Convergence v1
-- Read-only model. Grain: filing_year x quarter x permitting child x member.
--
-- Semantics:
--   Connects LDA activities to named members only through strong (>= 0.85)
--   bill mentions and Congress.gov sponsor/cosponsor relationships.
--   This is LEGISLATIVE PATHWAY CONVERGENCE. It is not evidence that a
--   lobbyist/client contacted a member, and it must not be described as such.
--   No filing-level dollars are attributed to members or lobbyists.
--
-- Score (within each policy-quarter cohort):
--   35% strong-bill breadth
--   30% connected-client breadth
--   25% connected-activity breadth
--   10% connected-lobbyist breadth
-- Sponsor/cosponsor counts are descriptive and are not arbitrary score multipliers.

WITH child_evidence AS (
    SELECT DISTINCT
        e.activity_id,
        e.policy_code
    FROM policy.activity_policy_evidence e
    WHERE e.classifier_version = 'permitting-child-v1'
),
member_paths AS (
    SELECT DISTINCT
        f.filing_year,
        f.filing_period AS quarter,
        ce.policy_code,
        mb.bioguide_id,
        mb.relationship_type,
        bm.congress,
        bm.bill_type,
        bm.bill_number,
        a.activity_id,
        f.client_id,
        al.lobbyist_id
    FROM child_evidence ce
    JOIN lda.activity a
      ON a.activity_id = ce.activity_id
    JOIN lda.filing f
      ON f.filing_uuid = a.filing_uuid
    JOIN policy.bill_mention bm
      ON bm.activity_id = a.activity_id
     AND bm.resolution_confidence >= 0.85
    JOIN policy.bill_member mb
      ON mb.congress = bm.congress
     AND mb.bill_type = bm.bill_type
     AND mb.bill_number = bm.bill_number
     AND NOT mb.is_withdrawn
    LEFT JOIN lda.activity_lobbyist al
      ON al.activity_id = a.activity_id
    WHERE bm.congress = 119
),
member_policy_quarter AS (
    SELECT
        filing_year,
        quarter,
        policy_code,
        bioguide_id,
        count(DISTINCT (congress, bill_type, bill_number)) AS strong_bills,
        count(DISTINCT (congress, bill_type, bill_number))
            FILTER (WHERE relationship_type = 'SPONSOR') AS sponsored_bills,
        count(DISTINCT (congress, bill_type, bill_number))
            FILTER (WHERE relationship_type = 'COSPONSOR') AS cosponsored_bills,
        count(DISTINCT activity_id) AS connected_activities,
        count(DISTINCT client_id) AS connected_clients,
        count(DISTINCT lobbyist_id) AS connected_lobbyists
    FROM member_paths
    GROUP BY 1,2,3,4
),
scored AS (
    SELECT
        q.*,
        count(*) OVER (
            PARTITION BY filing_year, quarter, policy_code
        ) AS cohort_size,
        100.0 * (
            0.35 * percent_rank() OVER (
                PARTITION BY filing_year, quarter, policy_code
                ORDER BY strong_bills
            )
          + 0.30 * percent_rank() OVER (
                PARTITION BY filing_year, quarter, policy_code
                ORDER BY connected_clients
            )
          + 0.25 * percent_rank() OVER (
                PARTITION BY filing_year, quarter, policy_code
                ORDER BY connected_activities
            )
          + 0.10 * percent_rank() OVER (
                PARTITION BY filing_year, quarter, policy_code
                ORDER BY connected_lobbyists
            )
        ) AS convergence_score
    FROM member_policy_quarter q
),
ranked AS (
    SELECT
        s.*,
        dense_rank() OVER (
            PARTITION BY filing_year, quarter, policy_code
            ORDER BY convergence_score DESC
        ) AS pathway_rank
    FROM scored s
)
SELECT
    r.filing_year,
    r.quarter,
    r.policy_code,
    r.bioguide_id,
    m.direct_order_name,
    m.party,
    m.state,
    m.district,
    m.chamber,
    r.strong_bills,
    r.sponsored_bills,
    r.cosponsored_bills,
    r.connected_activities,
    r.connected_clients,
    r.connected_lobbyists,
    r.cohort_size,
    r.pathway_rank,
    round(r.convergence_score::numeric, 1) AS convergence_score,
    CASE
        WHEN r.convergence_score >= 90 THEN 'HIGH_CONVERGENCE'
        WHEN r.convergence_score >= 75 THEN 'ELEVATED'
        WHEN r.convergence_score >= 50 THEN 'CONNECTED'
        ELSE 'BACKGROUND'
    END AS pathway_tier
FROM ranked r
JOIN policy.member m
  ON m.bioguide_id = r.bioguide_id
ORDER BY
    r.filing_year,
    CASE r.quarter
        WHEN 'first_quarter' THEN 1
        WHEN 'second_quarter' THEN 2
        WHEN 'third_quarter' THEN 3
        WHEN 'fourth_quarter' THEN 4
        ELSE 5
    END,
    r.policy_code,
    r.pathway_rank,
    r.bioguide_id;
