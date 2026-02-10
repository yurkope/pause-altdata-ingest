-- Treasury signal (MVP)
-- Anchor = normalized client_name
-- Windowing = rolling 3-day windows (keep all)
-- Scoring = 25 * agencies_hit_count (25 is "good"; gate at 25)

WITH
params AS (
  SELECT
    'semis'::text     AS policy_domain,          -- keep using your existing domain for now
    'TREASURY'::text  AS agency_key,
    25::numeric       AS points_per_agency_hit,
    25::numeric       AS gate_min
),

-- 1) explode activity government_entities into one row per entity mention
activity_entities AS (
  SELECT
    a.filing_uuid,
    a.activity_idx,
    (ge ->> 'name')::text AS entity_name
  FROM stg_lda_activities a
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(a.government_entities_json::jsonb, '[]'::jsonb)) ge
),

-- 2) match to your whitelist phrases (Treasury only, per your decision)
treasury_hits AS (
  SELECT
    ae.filing_uuid,
    ae.activity_idx,
    w.match_phrase
  FROM activity_entities ae
  JOIN s3_agency_whitelist w
    ON lower(ae.entity_name) = lower(w.match_phrase)
  JOIN params p
    ON w.policy_domain = p.policy_domain
   AND w.agency_key     = p.agency_key
   AND w.enabled_flag   = true
),

-- 3) bring in filing attributes + anchor definition
filings AS (
  SELECT
    f.filing_uuid,
    f.filing_year,
    f.filing_period,
    f.dt_posted,
    f.registrant_name,
    f.client_name,
    -- anchor = normalized client entity (client_name)
    lower(trim(f.client_name)) AS anchor_key
  FROM stg_lda_filings f
  WHERE f.client_name IS NOT NULL
),

-- 4) build 3-day window ids for each filing date (rolling window anchored on each filing date)
filings_with_windows AS (
  SELECT
    fl.*,
    date_trunc('day', fl.dt_posted)::date AS asof_date,
    (date_trunc('day', fl.dt_posted)::date - 2) AS window_start,
    (date_trunc('day', fl.dt_posted)::date)     AS window_end,
    ('D3_' || to_char((date_trunc('day', fl.dt_posted)::date - 2), 'YYYYMMDD')) AS window_id
  FROM filings fl
),

-- 5) aggregate Treasury hits inside each anchor + rolling 3-day window
window_scores AS (
  SELECT
    fw.anchor_key,
    fw.window_id,
    fw.window_start,
    fw.window_end,

    COUNT(DISTINCT th.match_phrase) AS agencies_hit_count,
    COUNT(*)                        AS total_entity_hits
  FROM filings_with_windows fw
  LEFT JOIN treasury_hits th
    ON th.filing_uuid = fw.filing_uuid
  GROUP BY 1,2,3,4
)

SELECT
  ws.anchor_key                                    AS anchor_id,
  ws.window_id,
  'S3a_treasury_intensity'::text                   AS signal_type,

  (ws.agencies_hit_count * p.points_per_agency_hit) AS s3a_intensity,
  p.gate_min,
  ((ws.agencies_hit_count * p.points_per_agency_hit) >= p.gate_min) AS passes_gate,

  ws.agencies_hit_count,
  ws.total_entity_hits,

  ws.window_start,
  ws.window_end
FROM window_scores ws
CROSS JOIN params p
ORDER BY ws.anchor_key, ws.window_start;