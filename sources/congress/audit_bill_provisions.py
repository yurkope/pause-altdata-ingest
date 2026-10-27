#!/usr/bin/env python3
"""Audit Bill Provision v1 candidates against official Congress.gov bill text.

Default is dry-run/read-only. It fetches Congress.gov's official text metadata,
downloads the newest available formatted text, extracts SEC./SECTION headings,
and checks whether LDA bill-local section candidates actually exist in that bill.

This script does not write to Neon.
"""
import argparse, os, re, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from html.parser import HTMLParser
from dotenv import load_dotenv
import psycopg2

BASE="https://api.congress.gov/v3"
SEC_RE=re.compile(r"(?im)^\s*(?:SEC\.|SECTION)\s+([0-9]+[A-Za-z0-9().-]*)[.\s-]+(.{0,240})$")

def session():
    s=requests.Session()
    retry=Retry(total=8,connect=8,read=8,status=8,backoff_factor=1,
                status_forcelist=(429,500,502,503,504),allowed_methods=frozenset(["GET"]),
                respect_retry_after_header=True,raise_on_status=False)
    s.mount("https://",HTTPAdapter(max_retries=retry))
    return s

def api(s,key,path):
    r=s.get(BASE+path,params={"api_key":key,"format":"json"},timeout=(10,60))
    r.raise_for_status()
    return r.json()

def newest_text_url(s,key,congress,btype,bnum):
    j=api(s,key,f"/bill/{congress}/{btype.lower()}/{bnum}/text")
    versions=j.get("textVersions",[])
    for v in versions:
        fmts=v.get("formats",[])
        for fmt in fmts:
            u=fmt.get("url","")
            if "Formatted Text" in fmt.get("type","") or u.endswith(".htm") or u.endswith(".html"):
                return u,v.get("type"),v.get("date")
    return None,None,None

def normalize_ref(x):
    if not x: return None
    m=re.search(r"(?i)(?:section|sec\.?)\s*([0-9]+[A-Za-z0-9().-]*)",x)
    return m.group(1).rstrip(".").lower() if m else None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--congress",type=int,default=119)
    ap.add_argument("--limit",type=int,default=0)
    args=ap.parse_args()
    load_dotenv()
    key=os.environ["CONGRESS_API_KEY"]; db=os.environ["DATABASE_URL"]
    q="""WITH s AS (
      SELECT DISTINCT bm.activity_id,bm.congress,upper(bm.bill_type) bill_type,bm.bill_number,a.description,
       CASE WHEN upper(bm.bill_type)='HR' THEN greatest(strpos(lower(a.description),'h.r. '||bm.bill_number::text),strpos(lower(a.description),'hr '||bm.bill_number::text))
            WHEN upper(bm.bill_type)='S' THEN greatest(strpos(lower(a.description),'s. '||bm.bill_number::text),strpos(lower(a.description),'s '||bm.bill_number::text))
            ELSE 0 END bill_pos
      FROM policy.bill_mention bm JOIN lda.activity a ON a.activity_id=bm.activity_id
      WHERE bm.congress=%s AND bm.resolution_confidence>=.85
    ), w AS (
      SELECT *,substring(description FROM greatest(1,bill_pos-40) FOR 360) local_text FROM s WHERE bill_pos>0
    )
    SELECT activity_id,congress,bill_type,bill_number,
      substring(local_text FROM '(?i)(?:section|sec\\.?)[[:space:]]*[0-9]+[a-zA-Z0-9().-]*') section_ref
    FROM w
    WHERE local_text ~* '(section|sec\\.?)[[:space:]]*[0-9]+' ORDER BY bill_type,bill_number,activity_id"""
    with psycopg2.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(q,(args.congress,)); rows=cur.fetchall()
    if args.limit: rows=rows[:args.limit]
    s=session(); cache={}; checked=matched=missing_text=0; by_bill={}; validated_rows=[]
    for activity_id,congress,btype,bnum,ref in rows:
        k=(congress,btype,bnum)
        if k not in cache:
            url,vtype,vdate=newest_text_url(s,key,*k)
            if not url:
                cache[k]=(set(),None,None,None); missing_text+=1
            else:
                r=s.get(url,timeout=(10,60)); r.raise_for_status()
                txt=re.sub(r"<[^>]+>", "\\n", r.text)
                sections={m.group(1).rstrip(".").lower() for m in SEC_RE.finditer(txt)}
                cache[k]=(sections,url,vtype,vdate)
            time.sleep(.05)
        sections,url,vtype,vdate=cache[k]
        nr=normalize_ref(ref); ok=nr in sections if nr else False
        checked+=1; matched+=int(ok)
        stats=by_bill.setdefault(f"{btype} {bnum}",{"checked":0,"validated":0})
        stats["checked"]+=1; stats["validated"]+=int(ok)
        if ok:
            validated_rows.append((activity_id,congress,btype,bnum,ref,vtype,vdate))
        print({"activity_id":activity_id,"bill":f"{btype} {bnum}","section_ref":ref,
               "validated":ok,"text_version":vtype,"text_date":vdate})
    print("\\nBY_BILL")
    for bill,stats in sorted(by_bill.items(),key=lambda kv:(-kv[1]["checked"],kv[0])):
        stats["validation_rate"]=round(100*stats["validated"]/stats["checked"],1) if stats["checked"] else 0
        print({"bill":bill,**stats})
    print("\\nSUMMARY")
    print({"candidates_checked":checked,"validated":matched,
           "rejected":checked-matched,
           "validation_rate":round(100*matched/checked,1) if checked else 0,
           "bills_without_formatted_text":missing_text})

if __name__=="__main__": main()
