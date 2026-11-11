#!/usr/bin/env python3
"""Outcome-blind historical bill-reference resolver using a frozen Congress manifest.

No network calls. Bill identity is accepted only when the existing deterministic
mention resolver returns candidate/verified with confidence >= 0.85. The frozen
manifest supplies official titles captured before historical outcome testing.
"""
import argparse,csv,os,sys
from collections import Counter
import psycopg2
from dotenv import load_dotenv

HERE=os.path.dirname(os.path.abspath(__file__))
ROOT=os.path.abspath(os.path.join(HERE,"..",".."))
sys.path.insert(0,os.path.join(ROOT,"sources","congress"))
from resolve_bills import extract,mention_resolution

def load_manifest(path,congress):
    out={}
    with open(path,newline="",encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if int(r["congress"])!=congress: continue
            key=(r["bill_type"].upper(),int(r["bill_number"]))
            out.setdefault(key,{"title":r["bill_title"],"policies":[]})["policies"].append(r["policy_code"])
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--year",type=int,required=True)
    ap.add_argument("--congress",type=int,required=True)
    ap.add_argument("--manifest",default="signals/bill_policy_118_manifest_v1.csv")
    ap.add_argument("--output",default="historical_bill_policy_evidence.csv")
    args=ap.parse_args();load_dotenv()
    manifest=load_manifest(args.manifest,args.congress)
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn,conn.cursor() as cur:
        cur.execute("""SELECT a.activity_id,a.description FROM lda.activity a
                       JOIN lda.filing f USING(filing_uuid)
                       WHERE f.filing_year=%s AND f.filing_type IN('Q1','Q2','Q3','Q4')
                         AND a.description IS NOT NULL""",(args.year,))
        rows=cur.fetchall()
    stats=Counter();evidence=[]
    for aid,desc in rows:
        for key,matched,start,end in extract(desc):
            if key not in manifest: continue
            stats["manifest_token_mentions"]+=1
            item=manifest[key]
            status,method,confidence,context=mention_resolution(
                desc,start,end,args.congress,item["title"],matched,{})
            stats[f"status_{status}"]+=1
            if status not in ("candidate","verified") or float(confidence)<0.85:
                continue
            stats["accepted_mentions"]+=1
            for policy in item["policies"]:
                evidence.append((aid,policy,float(confidence),args.congress,key[0],key[1],
                                 method,matched,context))
    # deterministic dedup: one activity/policy/bill identity
    dedup={}
    for r in evidence:
        k=(r[0],r[1],r[3],r[4],r[5])
        if k not in dedup or r[2]>dedup[k][2]: dedup[k]=r
    with open(args.output,"w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh);w.writerow(["activity_id","policy_code","confidence","source_congress",
            "source_bill_type","source_bill_number","resolution_method","evidence","evidence_context"])
        w.writerows(sorted(dedup.values(),key=lambda r:(r[0],r[1],r[4],r[5])))
    print({"year":args.year,"manifest_bills":len(manifest),"source_activities":len(rows),
           "evidence_rows":len(dedup),**dict(stats)})

if __name__=="__main__": main()
