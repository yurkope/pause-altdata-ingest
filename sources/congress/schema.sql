-- Proposed additive bill-to-committee graph. Do not apply without migration approval.
CREATE SCHEMA IF NOT EXISTS policy;
CREATE TABLE IF NOT EXISTS policy.bill (
  congress integer NOT NULL,
  bill_type text NOT NULL,
  bill_number integer NOT NULL,
  title text,
  introduced_date date,
  congress_url text,
  verified_at timestamptz NOT NULL DEFAULT now(),
  source_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(congress,bill_type,bill_number)
);
CREATE TABLE IF NOT EXISTS policy.bill_mention (
  activity_id bigint NOT NULL REFERENCES lda.activity(activity_id),
  congress integer NOT NULL,
  bill_type text NOT NULL,
  bill_number integer NOT NULL,
  evidence text NOT NULL,
  evidence_context text,
  resolution_method text,
  resolution_confidence numeric(4,3),
  resolution_status text NOT NULL CHECK (resolution_status IN ('candidate','verified','ambiguous','rejected')),
  PRIMARY KEY(activity_id,congress,bill_type,bill_number),
  FOREIGN KEY(congress,bill_type,bill_number)
    REFERENCES policy.bill(congress,bill_type,bill_number)
);
CREATE TABLE IF NOT EXISTS policy.committee (
  committee_code text PRIMARY KEY,
  name text NOT NULL,
  chamber text,
  source_json jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS policy.bill_committee (
  congress integer NOT NULL,
  bill_type text NOT NULL,
  bill_number integer NOT NULL,
  committee_code text NOT NULL REFERENCES policy.committee(committee_code),
  activity_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  PRIMARY KEY(congress,bill_type,bill_number,committee_code),
  FOREIGN KEY(congress,bill_type,bill_number)
    REFERENCES policy.bill(congress,bill_type,bill_number)
);
CREATE INDEX IF NOT EXISTS ix_policy_bill_mention_bill
  ON policy.bill_mention(congress,bill_type,bill_number);
CREATE INDEX IF NOT EXISTS ix_policy_bill_committee_committee
  ON policy.bill_committee(committee_code);
-- Crucial: committee referral is jurisdiction, not disclosed direct contact.
-- Do not interpret filing amount as bill- or committee-specific spend.
