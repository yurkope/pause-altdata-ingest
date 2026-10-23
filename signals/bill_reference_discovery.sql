-- Read-only 2025 Q4 LDA bill-reference discovery.
-- Bill numbers are NOT globally unique. Do not assign a Congress number
-- without contextual evidence or external Congress.gov validation.
-- A filing-year 2025 is not sufficient to infer the bill's Congress.
-- Mentions can contain stale bill references or OCR/formatting errors.
WITH extracted AS (
 SELECT
   a.activity_id,
   a.filing_uuid,
   a.general_issue_code AS lda_issue_code,
   a.description,
   regexp_matches(
     a.description,
     '\\m(H\\.?\\s*R\\.?|S\\.?|H\\.?\\s*J\\.?\\s*RES\\.?|S\\.?\\s*J\\.?\\s*RES\\.?)\\s*[-#:]?\\s*([0-9]{1,6})\\M',
     'gi'
   ) AS match
 FROM lda.activity a
 JOIN lda.filing f USING (filing_uuid)
 WHERE f.filing_year = 2025
   AND f.filing_type = 'Q4'
   AND a.description IS NOT NULL
),
mentions AS (
 SELECT
   activity_id,
   filing_uuid,
   lda_issue_code,
   UPPER(regexp_replace(match[1], '[^A-Za-z]', '', 'g')) AS bill_type,
   match[2]::integer AS bill_number,
   description
 FROM extracted
)
SELECT
  bill_type, bill_number,
  COUNT(DISTINCT activity_id) AS mentioning_activities,
  COUNT(DISTINCT filing_uuid) AS mentioning_filings,
  COUNT(DISTINCT lda_issue_code) AS parent_issue_codes
FROM mentions
GROUP BY bill_type, bill_number
ORDER BY mentioning_activities DESC, bill_type, bill_number
LIMIT 100;

-- Next-stage entity graph (not yet implemented):
-- lda.activity -> policy.bill_mention (evidence, context, resolution status)
-- -> policy.bill (congress, type, number, official metadata)
-- -> policy.bill_committee (system_code, referral/action, date)
-- -> policy.committee -> policy.committee_membership (bioguide_id, dates)
-- LDA government-entity contact and bill committee jurisdiction must remain
-- separate edges. Neither alone proves an individual was contacted.
-- Financial figures are filing-level; do not imply bill/committee/person
-- specific amounts were disclosed.
