#!/usr/bin/env python3
"""Export a compact, storage-light CSV for the provision->policy bridge.

Reads bill_clause_official_validation.csv and writes only the seven fields needed
by signals/bill_provision_policy_bridge_v1.sql. No database writes.
"""
import argparse,csv,re

OWNER_RE=re.compile(r"^(?P<bill_type>[A-Za-z]+)\s+(?P<bill_number>\d+)$")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",default="bill_clause_official_validation.csv")
    ap.add_argument("--output",default="bill_provision_policy_bridge.csv")
    args=ap.parse_args()

    with open(args.input,encoding="utf-8",newline="") as fh:
        rows=list(csv.DictReader(fh))

    out=[]; skipped=0
    for r in rows:
        owner=(r.get("owner") or "").strip()
        m=OWNER_RE.match(owner)
        congress=(r.get("validation_congress") or "").strip()
        activity=(r.get("activity_id") or "").strip()
        section=(r.get("normalized_section") or "").strip()
        status=(r.get("official_validation_status") or "").strip()
        if not (m and congress and activity and section and status):
            skipped+=1
            continue
        out.append({
            "activity_id":activity,
            "validation_congress":congress,
            "owner_bill_type":m.group("bill_type").upper(),
            "owner_bill_number":m.group("bill_number"),
            "normalized_section":section,
            "official_validation_status":status,
            "official_section_heading":(r.get("official_section_heading") or "").strip(),
        })

    fields=[
        "activity_id","validation_congress","owner_bill_type","owner_bill_number",
        "normalized_section","official_validation_status","official_section_heading",
    ]
    with open(args.output,"w",encoding="utf-8",newline="") as fh:
        w=csv.DictWriter(fh,fieldnames=fields)
        w.writeheader(); w.writerows(out)

    counts={}
    for r in out:
        counts[r["official_validation_status"]]=counts.get(r["official_validation_status"],0)+1
    print({"output":args.output,"rows":len(out),"skipped":skipped})
    print("STATUS")
    for k,v in sorted(counts.items(),key=lambda x:(-x[1],x[0])):
        print({"status":k,"count":v})

if __name__=="__main__":
    main()
