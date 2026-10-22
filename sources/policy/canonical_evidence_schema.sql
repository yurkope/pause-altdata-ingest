-- Canonical policy evidence model.
CREATE TABLE IF NOT EXISTS policy.canonical_policy (
 policy_code text PRIMARY KEY,
 policy_name text NOT NULL,
 policy_level text NOT NULL DEFAULT 'subissue' CHECK (policy_level IN ('issue','subissue','topic','program','statute')),
 parent_policy_code text REFERENCES policy.canonical_policy(policy_code),
 description text,
 is_active boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS policy.activity_policy_evidence (
 activity_id bigint NOT NULL REFERENCES lda.activity(activity_id) ON DELETE CASCADE,
 policy_code text NOT NULL REFERENCES policy.canonical_policy(policy_code),
 evidence_method text NOT NULL CHECK (evidence_method IN ('bill_anchor','activity_regex','corpus_anchor','semantic')),
 confidence numeric(4,3) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
 evidence_text text NOT NULL,
 classifier_version text NOT NULL,
 source_congress integer,
 source_bill_type text,
 source_bill_number integer,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (activity_id,policy_code,evidence_method,classifier_version,evidence_text)
);
CREATE INDEX IF NOT EXISTS activity_policy_evidence_policy_idx ON policy.activity_policy_evidence(policy_code,confidence);
CREATE INDEX IF NOT EXISTS activity_policy_evidence_activity_idx ON policy.activity_policy_evidence(activity_id);
CREATE INDEX IF NOT EXISTS activity_policy_evidence_method_idx ON policy.activity_policy_evidence(evidence_method,classifier_version);
