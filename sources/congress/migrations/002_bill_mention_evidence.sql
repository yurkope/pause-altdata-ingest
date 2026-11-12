-- Add auditable mention-level Congress resolution evidence.
ALTER TABLE policy.bill_mention
  ADD COLUMN IF NOT EXISTS evidence_context text,
  ADD COLUMN IF NOT EXISTS resolution_method text,
  ADD COLUMN IF NOT EXISTS resolution_confidence numeric(4,3);

ALTER TABLE policy.bill_mention
  DROP CONSTRAINT IF EXISTS bill_mention_resolution_confidence_check;
ALTER TABLE policy.bill_mention
  ADD CONSTRAINT bill_mention_resolution_confidence_check
  CHECK (resolution_confidence IS NULL OR
         (resolution_confidence >= 0 AND resolution_confidence <= 1));

CREATE INDEX IF NOT EXISTS ix_policy_bill_mention_resolution
  ON policy.bill_mention(resolution_status,resolution_method);
