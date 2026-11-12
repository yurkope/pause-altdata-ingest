#!/usr/bin/env python3
"""Read-only audit for provision-policy refinement. No database writes."""
import argparse,csv,os
from collections import Counter
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

VALID={"VALIDATED_BILL_PROVISION_CURRENT","VALIDATED_BILL_PROVISION_HISTORICAL"}
READY={"DETERMINISTIC","DETERMINISTIC_PARENT"}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",default="bill_provision_policy_bridge.csv")
    ap.add_argument("--manifest",default="signals/bill_provision_policy_v1_manifest.csv")
    args=ap.parse_args()
    load_dotenv()
    url=os.environ.get("DATABASE_URL")
    if not url: raise SystemExit("DATABASE_URL not found")

    with open(args.input,encoding="utf-8",newline="") as f:
        obs=[r for r in csv.DictReader(f) if r["official_validation_status"] in VALID]
    with open(args.manifest,encoding="utf-8",newline="") as f:
        maps=list(csv.DictReader(f))

    mm={(int(x["congress"]),x["bill_type"].upper(),int(x["bill_number"]),x["section_ref"].lower()):x for x in maps}
    rows=[]
    for x in obs:
        key=(int(x["validation_congress"]),x["owner_bill_type"].upper(),int(x["owner_bill_number"]),x["normalized_section"].lower())
        m=mm.get(key)
        rows.append((int(x["activity_id"]),key[0],key[1],key[2],x["normalized_section"],
                     m["policy_code"] if m else None,m["status"] if m else None))

    ready=[x for x in rows if x[6] in READY]
    checked=[]
    if ready:
        sql="""WITH i(activity_id,congress,bill_type,bill_number,section_ref,policy_code,mapping_status) AS (VALUES %s)
        SELECT i.*,
        EXISTS(SELECT 1 FROM policy.canonical_policy p WHERE p.policy_code=i.policy_code) policy_exists,
        EXISTS(SELECT 1 FROM policy.activity_policy_resolved_v2 r WHERE r.activity_id=i.activity_id AND r.policy_code=i.policy_code) already_resolved
        FROM i ORDER BY activity_id,policy_code"""
        conn=psycopg2.connect(url)
        try:
            conn.set_session(readonly=True,autocommit=True)
            with conn.cursor() as cur:
                execute_values(cur,sql,ready,template="(%s,%s,%s,%s,%s,%s,%s)")
                cols=[d.name for d in cur.description]
                checked=[dict(zip(cols,r)) for r in cur.fetchall()]
        finally: conn.close()

    unmapped=[x for x in rows if not x[5]]
    review=[x for x in rows if x[6]=="REVIEW"]
    usable=[x for x in checked if x["policy_exists"]]
    pending=[x for x in checked if not x["policy_exists"]]
    novel=[x for x in usable if not x["already_resolved"]]

    print("PROVISION_POLICY_REFINEMENT_AUDIT")
    print({"validated":len(rows),"mapped":len(rows)-len(unmapped),"unmapped":len(unmapped),
           "review":len(review),"deterministic":len(ready)})
    print({"usable_now":len(usable),"pending_taxonomy":len(pending),
           "new_activity_policy":len(novel),"already_resolved":len(usable)-len(novel)})
    print("Provision refinement is not an independent corroboration method.")
    print("POLICY_IMPACT")
    for k,v in Counter(x["policy_code"] for x in usable).most_common(): print(k,v)
    print("PENDING_TAXONOMY")
    for x in pending: print(x)
    print("UNMAPPED")
    for x in unmapped: print(x)

if __name__=="__main__": main()
