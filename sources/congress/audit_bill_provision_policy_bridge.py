#!/usr/bin/env python3
"""Read-only audit of validated bill provisions against the live policy-v2 model.

Reads bill_provision_policy_bridge.csv locally and queries Neon/Postgres using
DATABASE_URL. Makes no database writes and creates no database objects.
"""
import argparse,csv,os
from collections import Counter
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

VALID={
    "VALIDATED_BILL_PROVISION_CURRENT",
    "VALIDATED_BILL_PROVISION_HISTORICAL",
}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",default="bill_provision_policy_bridge.csv")
    args=ap.parse_args()
    load_dotenv()
    url=os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL not found in environment/.env")

    with open(args.input,encoding="utf-8",newline="") as fh:
        rows=[r for r in csv.DictReader(fh)
              if r.get("official_validation_status") in VALID]

    values=[(
        int(r["activity_id"]),int(r["validation_congress"]),
        r["owner_bill_type"].upper(),int(r["owner_bill_number"]),
        r["normalized_section"],r["official_validation_status"],
        r.get("official_section_heading") or ""
    ) for r in rows]

    sql="""
    WITH incoming(activity_id,congress,bill_type,bill_number,section_ref,validation_status,heading) AS (
      VALUES %s
    ),
    bill_map AS (
      SELECT DISTINCT source_congress AS congress,source_bill_type AS bill_type,
             source_bill_number AS bill_number,policy_code
      FROM policy.activity_policy_evidence
      WHERE classifier_version='bill-policy-v2'
        AND evidence_method='bill_anchor'
    ),
    candidates AS (
      SELECT i.*,m.policy_code
      FROM incoming i
      LEFT JOIN bill_map m
        ON m.congress=i.congress AND m.bill_type=i.bill_type
       AND m.bill_number=i.bill_number
    ),
    measured AS (
      SELECT c.*,
        EXISTS (
          SELECT 1 FROM policy.activity_policy_resolved_v2 r
          WHERE r.activity_id=c.activity_id AND r.policy_code=c.policy_code
        ) AS already_resolved,
        EXISTS (
          SELECT 1 FROM policy.activity_policy_evidence e
          WHERE e.activity_id=c.activity_id AND e.policy_code=c.policy_code
        ) AS already_has_any_evidence,
        COALESCE((
          SELECT r.method_count FROM policy.activity_policy_resolved_v2 r
          WHERE r.activity_id=c.activity_id AND r.policy_code=c.policy_code
        ),0) AS current_method_count,
        COALESCE((
          SELECT r.evidence_count FROM policy.activity_policy_resolved_v2 r
          WHERE r.activity_id=c.activity_id AND r.policy_code=c.policy_code
        ),0) AS current_evidence_count,
        COALESCE((
          SELECT r.corroborated FROM policy.activity_policy_resolved_v2 r
          WHERE r.activity_id=c.activity_id AND r.policy_code=c.policy_code
        ),false) AS currently_corroborated
      FROM candidates c
    )
    SELECT * FROM measured
    ORDER BY activity_id,policy_code NULLS LAST,section_ref
    """

    conn=psycopg2.connect(url)
    try:
        conn.set_session(readonly=True,autocommit=True)
        with conn.cursor() as cur:
            execute_values(cur,sql,values,template="(%s,%s,%s,%s,%s,%s,%s)",page_size=500)
            cols=[d.name for d in cur.description]
            result=[dict(zip(cols,x)) for x in cur.fetchall()]
    finally:
        conn.close()

    mapped=[r for r in result if r["policy_code"]]
    unmapped=[r for r in result if not r["policy_code"]]
    existing=[r for r in mapped if r["already_resolved"]]
    novel=[r for r in mapped if not r["already_resolved"]]
    corroboration_gain=[r for r in existing if not r["currently_corroborated"]]

    print("BILL_PROVISION_POLICY_BRIDGE_AUDIT")
    print({"validated_input_rows":len(rows),
           "candidate_policy_rows":len(result),
           "mapped_candidate_rows":len(mapped),
           "unmapped_candidate_rows":len(unmapped)})
    print({"already_resolved_rows":len(existing),
           "new_activity_policy_rows":len(novel),
           "existing_not_yet_corroborated":len(corroboration_gain)})
    print({"distinct_validated_activities":len({r["activity_id"] for r in result}),
           "distinct_mapped_policies":len({r["policy_code"] for r in mapped})})

    print("IMPACT_CLASS")
    impact=Counter()
    for r in result:
        if not r["policy_code"]: impact["NO_CANONICAL_BILL_MAP"]+=1
        elif not r["already_resolved"]: impact["NEW_ACTIVITY_POLICY"]+=1
        elif not r["currently_corroborated"]: impact["CORROBORATION_GAIN"]+=1
        else: impact["ADDITIONAL_EVIDENCE_ONLY"]+=1
    for k,v in impact.most_common():
        print({"impact":k,"count":v})

    print("POLICY_IMPACT")
    pc=Counter(r["policy_code"] for r in mapped)
    for k,v in pc.most_common():
        print({"policy_code":k,"candidate_rows":v})

    print("NEW_ACTIVITY_POLICY_ROWS")
    for r in novel:
        print({k:r[k] for k in (
            "activity_id","congress","bill_type","bill_number","section_ref",
            "policy_code","validation_status")})

    print("NO_CANONICAL_BILL_MAP")
    seen=set()
    for r in unmapped:
        k=(r["congress"],r["bill_type"],r["bill_number"])
        if k in seen: continue
        seen.add(k)
        print({"congress":r["congress"],"bill_type":r["bill_type"],
               "bill_number":r["bill_number"],"activity_id":r["activity_id"],
               "section_ref":r["section_ref"]})

if __name__=="__main__":
    main()
