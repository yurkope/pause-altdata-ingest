-- Convergence Alert Detector v1
-- Auditable, percentile-calibrated detector over policy -> bill -> committee convergence.
-- Input: policy.policy_committee_convergence_v1.
--
-- Design:
-- * structural_strength is descriptive, not a causal/composite "risk score": mean of
--   within-quarter percentiles for policy pressure, committee pressure, committee
--   activity share, linked activities, and linked clients.
-- * breadth and committee-share gates prevent one-bill / tiny-intersection false alerts.
-- * HIGH_CONVICTION requires simultaneous policy + committee acceleration.
-- * EMERGING requires one accelerating side, the other non-declining, plus meaningful
--   intersection breadth/share.
-- * WATCH is structurally unusual but does not require acceleration.
-- * Q2 has no exact prior quarter because Q1 is not loaded, so it cannot be Emerging/High.
-- * dominant-bill concentration is exposed for explanation but deliberately not used as
--   an alert gate: historical small intersections make it 100% too often.
-- No financial amount is interpreted as direct spend on a bill, policy, committee, or person.

WITH base AS (
    SELECT c.*,
        ROUND((100.0*c.dominant_bill_activities/NULLIF(c.linked_activities,0))::numeric,1)
            AS dominant_bill_concentration_pct,
        PERCENT_RANK() OVER (PARTITION BY c.filing_type ORDER BY c.policy_pressure) AS policy_pressure_pct,
        PERCENT_RANK() OVER (PARTITION BY c.filing_type ORDER BY c.committee_pressure) AS committee_pressure_pct,
        PERCENT_RANK() OVER (PARTITION BY c.filing_type ORDER BY c.committee_activity_share_pct) AS committee_share_pct,
        PERCENT_RANK() OVER (PARTITION BY c.filing_type ORDER BY c.linked_activities) AS linked_activities_pct,
        PERCENT_RANK() OVER (PARTITION BY c.filing_type ORDER BY c.linked_clients) AS linked_clients_pct
    FROM policy.policy_committee_convergence_v1 c
),
scored AS (
    SELECT b.*,
        ROUND(((
            policy_pressure_pct
          + committee_pressure_pct
          + committee_share_pct
          + linked_activities_pct
          + linked_clients_pct
        )/5.0)::numeric,3) AS structural_strength
    FROM base b
),
classified AS (
    SELECT s.*,
        CASE
            WHEN linked_activities >= 20
             AND linked_clients >= 15
             AND committee_activity_share_pct >= 15
             AND structural_strength >= .80
             AND COALESCE(policy_pressure_change,0) >= 5
             AND COALESCE(committee_pressure_change,0) >= 5
                THEN 'HIGH_CONVICTION'
            WHEN linked_activities >= 15
             AND linked_clients >= 12
             AND committee_activity_share_pct >= 10
             AND structural_strength >= .75
             AND (
                    (COALESCE(policy_pressure_change,0) >= 5
                     AND COALESCE(committee_pressure_change,0) >= 0)
                 OR (COALESCE(committee_pressure_change,0) >= 8
                     AND COALESCE(policy_pressure_change,0) >= 0)
                 )
                THEN 'EMERGING'
            WHEN linked_activities >= 10
             AND linked_clients >= 8
             AND committee_activity_share_pct >= 5
             AND structural_strength >= .70
                THEN 'WATCH'
            ELSE 'NORMAL'
        END AS alert_level
    FROM scored s
)
SELECT c.*,
    CASE c.alert_level
        WHEN 'HIGH_CONVICTION' THEN 4
        WHEN 'EMERGING' THEN 3
        WHEN 'WATCH' THEN 2
        ELSE 1
    END AS alert_rank,
    concat_ws('; ',
        CASE WHEN c.structural_strength >= .80 THEN 'high structural convergence'
             WHEN c.structural_strength >= .75 THEN 'elevated structural convergence'
             WHEN c.structural_strength >= .70 THEN 'above-normal structural convergence' END,
        CASE WHEN c.policy_pressure_change >= 5
             THEN format('policy pressure accelerating %+s',c.policy_pressure_change) END,
        CASE WHEN c.committee_pressure_change >= 5
             THEN format('committee pressure accelerating %+s',c.committee_pressure_change) END,
        CASE WHEN c.committee_activity_share_pct >= 50
             THEN format('%s%% of committee strong activity linked to policy',c.committee_activity_share_pct)
             WHEN c.committee_activity_share_pct >= 15
             THEN format('%s%% committee activity share',c.committee_activity_share_pct) END,
        CASE WHEN c.linked_clients >= 50
             THEN format('%s linked clients',c.linked_clients) END,
        CASE WHEN c.dominant_bill_concentration_pct >= 75 AND c.linked_activities >= 20
             THEN format('dominant bill anchors %s%% of linked activity',c.dominant_bill_concentration_pct) END
    ) AS alert_reason
FROM classified c;
