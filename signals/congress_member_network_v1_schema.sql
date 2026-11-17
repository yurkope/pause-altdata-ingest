-- Congressperson network schema v1 -- PROPOSAL ONLY, DO NOT APPLY DIRECTLY
-- Stable identity: Congress.gov Bioguide ID.
-- bill_member.relationship_type is legislative evidence, not evidence of lobbying contact.
-- Committee membership intentionally excluded from v1 until independently sourced/audited.

CREATE TABLE IF NOT EXISTS policy.member (
    bioguide_id text PRIMARY KEY,
    direct_order_name text NOT NULL,
    first_name text,
    middle_name text,
    last_name text,
    suffix_name text,
    party text,
    state text,
    district integer,
    chamber text,
    source_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    verified_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS policy.bill_member (
    congress integer NOT NULL,
    bill_type text NOT NULL,
    bill_number integer NOT NULL,
    bioguide_id text NOT NULL REFERENCES policy.member(bioguide_id),
    relationship_type text NOT NULL CHECK (relationship_type IN ('SPONSOR','COSPONSOR')),
    sponsorship_date date,
    is_withdrawn boolean NOT NULL DEFAULT false,
    source_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    verified_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (congress,bill_type,bill_number,bioguide_id,relationship_type),
    FOREIGN KEY (congress,bill_type,bill_number)
      REFERENCES policy.bill(congress,bill_type,bill_number)
);

CREATE INDEX IF NOT EXISTS bill_member_member_idx
  ON policy.bill_member(bioguide_id,congress);
CREATE INDEX IF NOT EXISTS bill_member_bill_idx
  ON policy.bill_member(congress,bill_type,bill_number,relationship_type);

COMMENT ON TABLE policy.bill_member IS
'Congress.gov sponsor/cosponsor relationships. Legislative relationship only; does not establish direct lobbying contact.';

COMMENT ON COLUMN policy.bill_member.relationship_type IS
'SPONSOR or COSPONSOR from official Congress.gov bill data; never interpret as evidence a lobbyist contacted this member.';
