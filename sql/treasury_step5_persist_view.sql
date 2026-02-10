-- Persist Treasury signal as a VIEW (MVP)
-- View name: signal.v_treasury_s3a

CREATE SCHEMA IF NOT EXISTS signal;

CREATE OR REPLACE VIEW signal.s3a_treasury_intensity_v1 AS
WITH
params AS (
  SELECT
    'semis'::text     AS policy_domain,
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
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(a.government_entities_json, '[]'::jsonb)) ge
),

-- 2) match to whitelist phrases (Treasury)
treasury_hits AS (
  SELECT
    ae.filing_uuid,
    ae.activity_idx,
    w.agency_key,
    w.match_phrase
  FROM activity_entities ae
  JOIN s3_agency_whitelist w
    ON lower(ae.entity_name) = lower(w.match_phrase)
  JOIN params p
    ON w.policy_domain = p.policy_domain
   AND w.agency_key    = p.agency_key
   AND w.enabled_flag  = true
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
    -- Anchor = client_name (per your decision)
    lower(trim(f.client_name)) AS anchor_id
  FROM stg_lda_filings f
),

-- 4) build 3-day rolling windows per anchor
-- NOTE: this assumes dt_posted is timestamp/date; adjust cast if yours differs
windows AS (
  SELECT DISTINCT
    fl.anchor_id,
    (date(fl.dt_posted))::date AS anchor_date
  FROM filings fl
),

rolling_windows AS (
  SELECT
    w.anchor_id,
    ('D3_' || to_char(w.anchor_date, 'YYYYMMDD')) AS window_id,
    w.anchor_date::date AS window_start,
    (w.anchor_date + 2)::date AS window_end
  FROM windows w
),

-- 5) count hits within each 3-day window
hits_by_window AS (
  SELECT
    rw.anchor_id,
    rw.window_id,
    rw.window_start,
    rw.window_end,
    COUNT(*)::int AS total_entity_hits,
    COUNT(DISTINCT th.agency_key)::int AS agencies_hit_count
  FROM rolling_windows rw
  JOIN filings fl
    ON fl.anchor_id = rw.anchor_id
   AND date(fl.dt_posted) BETWEEN rw.window_start AND rw.window_end
  LEFT JOIN treasury_hits th
    ON th.filing_uuid = fl.filing_uuid
  GROUP BY
    rw.anchor_id, rw.window_id, rw.window_start, rw.window_end
),

-- 6) score + gate
scored AS (
  SELECT
    h.anchor_id,
    h.window_id,
    'S3a_treasury_intensity'::text AS signal_type,
    (h.agencies_hit_count * p.points_per_agency_hit)::numeric AS s3a_intensity,
    p.gate_min,
    ((h.agencies_hit_count * p.points_per_agency_hit) >= p.gate_min) AS passes_gate,
    h.agencies_hit_count,
    h.total_entity_hits,
    h.window_start,
    h.window_end
  FROM hits_by_window h
  CROSS JOIN params p
)

SELECT *
FROM scored;