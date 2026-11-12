-- Bill Provision v1: conservative LDA evidence extraction
-- Read-only. Grain: activity x resolved bill.
--
-- Purpose:
--   Go one level below bill identity without forcing every bill mention into a
--   provision. Exact/named provision evidence must be local to the bill mention.
--
-- Evidence levels / investigation buckets:
--   BILL_SECTION_CANDIDATE       section/sec. reference found in a bounded bill-local window;
--                                requires official bill-text validation
--   EXTERNAL_STATUTE_CANDIDATE   section reference appears with named statute/code language;
--                                preserve as regulatory/statutory context, not bill provision
--   ALL_PROVISIONS               LDA explicitly says all/entire provisions/titles/bill
--   BILL_ONLY_UNRESOLVED         strong bill mention but no safe local provision reference
--
-- A failed bill-section validation is NEVER discarded. It remains an observation and
-- should later be classified as external statute, neighboring bill, version mismatch,
-- normalization/structure mismatch, named provision/topic, or unresolved.
--
-- IMPORTANT:
--   EXACT_SECTION_CANDIDATE is a candidate until validated against the official
--   Congress.gov text structure for that bill/version. A section number elsewhere
--   in the same LDA activity is NOT sufficient.
--   No filing-level dollars are attributed to provisions, members, or lobbyists.

WITH strong_bill_activity AS (
    SELECT DISTINCT
        bm.activity_id,
        bm.congress,
        upper(bm.bill_type) AS bill_type,
        bm.bill_number,
        bm.resolution_confidence,
        a.description
    FROM policy.bill_mention bm
    JOIN lda.activity a
      ON a.activity_id=bm.activity_id
    WHERE bm.resolution_confidence>=0.85
      AND bm.congress=119
),
bill_token_position AS (
    SELECT
        s.*,
        CASE
          WHEN bill_type='HR' THEN greatest(
              strpos(lower(description),'h.r. '||bill_number::text),
              strpos(lower(description),'hr '||bill_number::text),
              strpos(lower(description),'h.r.'||bill_number::text),
              strpos(lower(description),'hr'||bill_number::text)
          )
          WHEN bill_type='S' THEN greatest(
              strpos(lower(description),'s. '||bill_number::text),
              strpos(lower(description),'s '||bill_number::text),
              strpos(lower(description),'s.'||bill_number::text)
          )
          ELSE 0
        END AS bill_pos
    FROM strong_bill_activity s
),
bill_local AS (
    SELECT
        p.*,
        substring(
          description
          FROM greatest(1,bill_pos-40)
          FOR 360
        ) AS local_text
    FROM bill_token_position p
    WHERE bill_pos>0
),
classified AS (
    SELECT
        b.*,
        substring(
          local_text
          FROM '(?i)(?:section|sec\.?)[[:space:]]*[0-9]+[a-zA-Z0-9().-]*'
        ) AS local_section_ref,
        local_text ~* '\m(all|entire)[[:space:]]+(titles|provisions|bill)\M'
          AS explicit_all_provisions
    FROM bill_local b
)
SELECT
    activity_id,
    congress,
    bill_type,
    bill_number,
    resolution_confidence,
    CASE
      WHEN local_section_ref IS NOT NULL
       AND local_text ~* '\\m(clean air act|clean water act|internal revenue code|tax code|national environmental policy act|nepa|endangered species act|public law|u\\.s\\.c\\.|united states code)\\M'
        THEN 'EXTERNAL_STATUTE_CANDIDATE'
      WHEN local_section_ref IS NOT NULL THEN 'BILL_SECTION_CANDIDATE'
      WHEN explicit_all_provisions THEN 'ALL_PROVISIONS'
      ELSE 'BILL_ONLY_UNRESOLVED'
    END AS provision_evidence_level,
    local_section_ref AS provision_reference,
    local_text AS evidence_excerpt
FROM classified
ORDER BY congress,bill_type,bill_number,activity_id;
