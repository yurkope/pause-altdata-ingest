#!/usr/bin/env python3
"""Classify every bill-local section observation without discarding nonmatches.

Read-only. Uses Neon for LDA/bill evidence and Congress.gov for official bill text.
Outputs one CSV row per observation with an investigation bucket.

Buckets:
  SECTION_EXISTS_IN_CURRENT_BILL
  EXTERNAL_STATUTE_OR_CODE
  HISTORICAL_OR_VERSION_CANDIDATE
  STRUCTURE_OR_NORMALIZATION_CANDIDATE
  NAMED_TOPIC_OR_PROVISION
  UNRESOLVED_SECTION_REFERENCE

A bucket is an evidence status, not proof of causality/contact.
"""
import argparse,csv,html,os,re,time
from datetime import date
import requests,psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

BASE="https://api.congress.gov/v3"
SEC_RE=re.compile(r"(?im)^\s*(?:SEC\.|SECTION)\s+([0-9]+[A-Za-z0-9().-]*)[.\s-]+(.{0,240})$")
STATUTE_RE=re.compile(r"(?i)\b(clean air act|clean water act|national environmental policy act|nepa|endangered species act|internal revenue code|tax code|united states code|u\.s\.c\.|public law|p\.l\.)\b")
OTHER_BILL_RE=re.compile(r"(?i)\b(?:H\.?R\.?|S\.?)\s*\d+\b")
TOPIC_RE=re.compile(r"(?i)\b(provisions? related to|provision regarding|provision concerning|all provisions|all titles|title [ivxlcdm]+|subtitle [a-z])\b")

def sess():
    s=requests.Session()
    retry=Retry(total=8,connect=8,read=8,status=8,backoff_factor=1,
      status_forcelist=(429,500,502,503,504),allowed_methods=frozenset(["GET"]),
      respect_retry_after_header=True,raise_on_status=False)
    s.mount("https://",HTTPAdapter(max_retries=retry)); return s

def api(s,key,path):
    r=s.get(BASE+path,params={"api_key":key,"format":"json"},timeout=(10,60))
    r.raise_for_status(); return r.json()

def versions(s,key,congress,btype,bnum):
    j=api(s,key,f"/bill/{congress}/{btype.lower()}/{bnum}/text")
    return j.get("textVersions",[])

def formatted_url(v):
    for f in v.get("formats",[]):
        u=f.get("url","")
        if "Formatted Text" in f.get("type","") or u.endswith((".htm",".html")): return u
    return None

def text_sections(s,url):
    r=s.get(url,timeout=(10,60)); r.raise_for_status()
    txt=html.unescape(re.sub(r"<[^>]+>","\n",r.text))
    return {m.group(1).rstrip(".").lower(): re.sub(r"\\s+"," ",m.group(2)).strip()
            for m in SEC_RE.finditer(txt)}

def norm(ref):
    m=re.search(r"(?i)(?:section|sec\.?)\s*([0-9]+[A-Za-z0-9().-]*)",ref or "")
    return m.group(1).rstrip(".").lower() if m else None

def parse_date(x):
    if not x:return None
    try:return date.fromisoformat(x[:10])
    except:return None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--congress",type=int,default=119)
    ap.add_argument("--output",default="bill_provision_investigation.csv")
    ap.add_argument("--sleep",type=float,default=.05)
    args=ap.parse_args(); load_dotenv()
    key=os.environ["CONGRESS_API_KEY"]; db=os.environ["DATABASE_URL"]
    q="""WITH s AS (
      SELECT DISTINCT bm.activity_id,bm.congress,upper(bm.bill_type) bill_type,bm.bill_number,
        bm.resolution_confidence,a.description,f.filing_year,f.filing_period,
        CASE WHEN upper(bm.bill_type)='HR' THEN greatest(strpos(lower(a.description),'h.r. '||bm.bill_number::text),strpos(lower(a.description),'hr '||bm.bill_number::text))
             WHEN upper(bm.bill_type)='S' THEN greatest(strpos(lower(a.description),'s. '||bm.bill_number::text),strpos(lower(a.description),'s '||bm.bill_number::text))
             ELSE 0 END bill_pos
      FROM policy.bill_mention bm JOIN lda.activity a ON a.activity_id=bm.activity_id
      JOIN lda.filing f ON f.filing_uuid=a.filing_uuid
      WHERE bm.congress=%s AND bm.resolution_confidence>=.85
    ),w AS (
      SELECT *,substring(description FROM greatest(1,bill_pos-40) FOR 360) local_text
      FROM s WHERE bill_pos>0
    )
    SELECT activity_id,congress,bill_type,bill_number,resolution_confidence,
      filing_year,filing_period,local_text,
      substring(local_text FROM '(?i)(?:section|sec\\.?)[[:space:]]*[0-9]+[a-zA-Z0-9().-]*') section_ref
    FROM w WHERE local_text ~* '(section|sec\\.?)[[:space:]]*[0-9]+'
    ORDER BY bill_type,bill_number,activity_id"""
    with psycopg2.connect(db) as conn:
      with conn.cursor() as cur: cur.execute(q,(args.congress,)); rows=cur.fetchall()

    s=sess(); cache={}; out=[]; counts={}; neighbor_diag=[]
    for aid,cong,bt,bn,conf,fy,fp,local,ref in rows:
        k=(cong,bt,bn)
        if k not in cache:
            vv=versions(s,key,*k); parsed=[]
            for v in vv:
                u=formatted_url(v)
                if not u: continue
                try: secs=text_sections(s,u)
                except requests.RequestException: continue
                parsed.append((v.get("type"),v.get("date"),u,secs))
                time.sleep(args.sleep)
            cache[k]=parsed
        nr=norm(ref); vv=cache[k]
        newest=vv[0] if vv else (None,None,None,{})
        newest_ok=bool(nr and nr in newest[3])
        any_matches=[v for v in vv if nr and nr in v[3]]
        external=bool(STATUTE_RE.search(local))
        named_topic=bool(TOPIC_RE.search(local))

        # Attribute a section to a neighboring bill only when that bill mention
        # is closer to the section reference than the resolved target bill.
        section_m=re.search(r"(?i)(?:section|sec\.?)\s*[0-9]+[A-Za-z0-9().-]*",local)
        section_pos=section_m.start() if section_m else None
        if bt=="HR":
            target_pattern=rf"(?i)\bH\.?R\.?\s*{bn}\b"
        elif bt=="S":
            target_pattern=rf"(?i)\bS\.?\s*{bn}\b"
        else:
            target_pattern=rf"(?i)\b{re.escape(bt)}\.?\s*{bn}\b"
        target_positions=[m.start() for m in re.finditer(target_pattern,local)]
        target_dist=min((abs(p-section_pos) for p in target_positions),default=10**9) if section_pos is not None else 10**9

        other_bill_hits=[]
        for hit in OTHER_BILL_RE.finditer(local):
            digits=re.sub(r"\D","",hit.group(0))
            # Same number is not enough by itself; also compare chamber/type.
            normalized_hit=hit.group(0).upper().replace(".","").replace(" ","")
            normalized_target=f"{bt}{bn}".upper().replace(".","").replace(" ","")
            if normalized_hit==normalized_target:
                continue
            dist=abs(hit.start()-section_pos) if section_pos is not None else 10**9
            if dist < target_dist:
                other_bill_hits.append((dist,hit.group(0)))
        other_bill_hits.sort(key=lambda x:x[0])
        other_bills=[x[1] for x in other_bill_hits]

        if newest_ok: bucket="SECTION_EXISTS_IN_CURRENT_BILL"
        elif external: bucket="EXTERNAL_STATUTE_OR_CODE"
        elif other_bills: bucket="NEIGHBORING_BILL_REFERENCE"
        elif any_matches: bucket="HISTORICAL_OR_VERSION_CANDIDATE"
        elif nr and any(nr.split("(")[0] in sec for _,_,_,secs in vv for sec in secs):
            bucket="STRUCTURE_OR_NORMALIZATION_CANDIDATE"
        elif named_topic: bucket="NAMED_TOPIC_OR_PROVISION"
        else: bucket="UNRESOLVED_SECTION_REFERENCE"

        current_heading=newest[3].get(nr) if newest_ok else None
        historical_heading=next((v[3].get(nr) for v in any_matches if v[3].get(nr)),None)
        counts[bucket]=counts.get(bucket,0)+1
        target_distance=None if target_dist==10**9 else target_dist
        other_distance=other_bill_hits[0][0] if other_bill_hits else None
        if bucket=="NEIGHBORING_BILL_REFERENCE":
            neighbor_diag.append({"target_bill":f"{bt} {bn}",
                                  "target_distance":target_distance,
                                  "other_distance":other_distance,
                                  "neighbors":[x[1] for x in other_bill_hits]})
        out.append([aid,cong,bt,bn,float(conf),fy,fp,ref,nr,bucket,
                    newest[0],newest[1],len(any_matches),current_heading,historical_heading,
                    "|".join(other_bills[:5]),target_distance,other_distance,
                    local.replace("\\n"," ")[:360]])
    with open(args.output,"w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh); w.writerow(["activity_id","congress","bill_type","bill_number",
        "resolution_confidence","filing_year","filing_period","section_ref","normalized_ref",
        "investigation_bucket","newest_text_version","newest_text_date","matching_text_versions",
        "current_section_heading","historical_section_heading","other_bill_refs",
        "target_bill_distance","nearest_other_bill_distance","evidence_excerpt"]); w.writerows(out)
    print({"output":args.output,"observations":len(out)})
    print("BUCKETS")
    for k,v in sorted(counts.items(),key=lambda x:-x[1]):
        print({"bucket":k,"count":v,"pct":round(100*v/len(out),1) if out else 0})
    print("BY_TARGET_BILL")
    by={}
    for r in out:
        bill=f"{r[2]} {r[3]}"; bucket=r[9]
        by.setdefault(bill,{})[bucket]=by.setdefault(bill,{}).get(bucket,0)+1
    for bill,buckets in sorted(by.items(),key=lambda kv:-sum(kv[1].values()))[:30]:
        print({"bill":bill,"observations":sum(buckets.values()),"buckets":buckets})
    print("NEIGHBORING_BILL_DIAGNOSTIC")
    target_d=[x["target_distance"] for x in neighbor_diag if x["target_distance"] is not None]
    other_d=[x["other_distance"] for x in neighbor_diag if x["other_distance"] is not None]
    print({"count":len(neighbor_diag),
           "target_distance_median":sorted(target_d)[len(target_d)//2] if target_d else None,
           "other_distance_median":sorted(other_d)[len(other_d)//2] if other_d else None})
    print("TOP_NEIGHBORING_BILL_PAIRS")
    pairs={}
    for x in neighbor_diag:
        for neighbor in x["neighbors"]:
            key=(x["target_bill"],neighbor.upper().replace(".",""))
            pairs[key]=pairs.get(key,0)+1
    for (target,neighbor),n in sorted(pairs.items(),key=lambda kv:(-kv[1],kv[0]))[:40]:
        print({"target_bill":target,"nearer_bill":neighbor,"observations":n})

if __name__=="__main__": main()
