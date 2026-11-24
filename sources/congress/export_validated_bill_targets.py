#!/usr/bin/env python3
"""Emit targeted resolver arguments from validated bill-provision observations.

This closes the feedback loop for low-frequency bills that can be validated
against official text but may fall below the broad resolver's --top cutoff.

No database access. No API calls. No writes except optional text output.
"""
import argparse,csv,re

VALID={"VALIDATED_BILL_PROVISION_CURRENT","VALIDATED_BILL_PROVISION_HISTORICAL"}
OWNER_RE=re.compile(r"^(?P<type>[A-Za-z]+)\s+(?P<number>\d+)$")

def targets(rows):
    found=set()
    for r in rows:
        if r.get("official_validation_status") not in VALID:
            continue
        m=OWNER_RE.match((r.get("owner") or "").strip())
        congress=(r.get("validation_congress") or "").strip()
        if not (m and congress):
            continue
        found.add((int(congress),m.group("type").upper(),int(m.group("number"))))
    return sorted(found)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",default="bill_clause_official_validation.csv")
    ap.add_argument("--congress",type=int,default=119,
                    help="Only emit targets for this Congress; resolver handles one Congress set per run.")
    ap.add_argument("--output",help="Optional output file; otherwise print commands.")
    args=ap.parse_args()
    with open(args.input,encoding="utf-8",newline="") as f:
        rows=list(csv.DictReader(f))
    selected=[x for x in targets(rows) if x[0]==args.congress]
    commands=[f"--bill {typ}{num}" for _,typ,num in selected]
    text=" ".join(commands)
    if args.output:
        with open(args.output,"w",encoding="utf-8") as f: f.write(text+"\n")
    print({"congress":args.congress,"target_bills":len(selected)})
    print(text)

if __name__=="__main__": main()
