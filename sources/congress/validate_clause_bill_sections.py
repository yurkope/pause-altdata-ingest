#!/usr/bin/env python3
"""Validate clause-resolved BILL section references against official Congress.gov text.

Read-only. Input is bill_clause_resolution.csv from resolve_reference_clauses.py.
A text match establishes section existence in an official bill version; it does
not by itself prove that the LDA clause intended that section.
"""
import argparse,csv,html,os,re,time
from datetime import datetime
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE="https://api.congress.gov/v3"
SEC_RE=re.compile(r"(?im)^\s*(?:SEC\.|SECTION)\s+([0-9]+[A-Za-z0-9().-]*)[.\s-]+(.{0,240})$")

def session():
    s=requests.Session()
    retry=Retry(total=8,connect=8,read=8,status=8,backoff_factor=1,
        status_forcelist=(429,500,502,503,504),allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,raise_on_status=False)
    s.mount("https://",HTTPAdapter(max_retries=retry)); return s

def api(s,key,path):
    r=s.get(BASE+path,params={"api_key":key,"format":"json"},timeout=(10,60))
    r.raise_for_status(); return r.json()

def parse_date(x):
    if not x:return datetime.min
    for fmt in ("%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d"):
        try:return datetime.strptime(x[:len(fmt)],fmt)
        except:pass
    return datetime.min

def formatted_url(v):
    for f in v.get("formats",[]):
        u=f.get("url","")
        if "Formatted Text" in f.get("type","") or u.endswith((".htm",".html")): return u
    return None

def sections(s,url):
    r=s.get(url,timeout=(10,60)); r.raise_for_status()
    txt=html.unescape(re.sub(r"<[^>]+>","\n",r.text))
    return {m.group(1).rstrip(".").lower():re.sub(r"\s+"," ",m.group(2)).strip()
            for m in SEC_RE.finditer(txt)}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--congress",type=int,default=119)
    ap.add_argument("--input",default="bill_clause_resolution.csv")
    ap.add_argument("--output",default="bill_clause_official_validation.csv")
    ap.add_argument("--sleep",type=float,default=.05)
    args=ap.parse_args(); load_dotenv(); key=os.environ["CONGRESS_API_KEY"]
    with open(args.input,encoding="utf-8") as fh:
        rows=list(csv.DictReader(fh))
    bill_rows=[r for r in rows if r["owner_type"]=="BILL" and r.get("owner")]
    s=session(); cache={}; out=[]
    for r in bill_rows:
        bt,bn=r["owner"].split()
        k=(args.congress,bt,int(bn))
        if k not in cache:
            j=api(s,key,f"/bill/{args.congress}/{bt.lower()}/{bn}/text")
            vv=j.get("textVersions",[])
            vv=sorted(vv,key=lambda v:parse_date(v.get("date")),reverse=True)
            parsed=[]
            for v in vv:
                url=formatted_url(v)
                if not url: continue
                try: secmap=sections(s,url)
                except requests.RequestException: secmap={}
                parsed.append((v.get("type"),v.get("date"),secmap))
                time.sleep(args.sleep)
            cache[k]=parsed
        vv=cache[k]; sec=r["normalized_section"].lower().rstrip(".")
        matches=[v for v in vv if sec in v[2]]
        newest=vv[0] if vv else (None,None,{})
        current=bool(sec in newest[2])
        historical=bool(matches and not current)
        heading=(newest[2].get(sec) if current else (matches[0][2].get(sec) if matches else None))
        if current and r["ownership_basis"]=="DIRECT_BILL":
            status="VALIDATED_BILL_PROVISION_CURRENT"
        elif historical and r["ownership_basis"]=="DIRECT_BILL":
            status="VALIDATED_BILL_PROVISION_HISTORICAL"
        elif current:
            status="SECTION_EXISTS_CURRENT_ATTRIBUTION_WEAK"
        elif historical:
            status="SECTION_EXISTS_HISTORICAL_VERSION"
        else:
            status="REJECTED_BILL_PROVISION_CANDIDATE"
        out.append({**r,"official_validation_status":status,
                    "newest_text_version":newest[0],"newest_text_date":newest[1],
                    "matching_text_versions":"|".join(str(v[0]) for v in matches),
                    "official_section_heading":heading})
    fields=list(out[0].keys()) if out else []
    with open(args.output,"w",newline="",encoding="utf-8") as fh:
        w=csv.DictWriter(fh,fieldnames=fields); w.writeheader(); w.writerows(out)
    counts={}
    for r in out: counts[r["official_validation_status"]]=counts.get(r["official_validation_status"],0)+1
    print({"output":args.output,"bill_owned_observations":len(out),"distinct_bills":len(cache)})
    print("VALIDATION_STATUS")
    for k,v in sorted(counts.items(),key=lambda x:-x[1]):
        print({"status":k,"count":v,"pct":round(100*v/len(out),1) if out else 0})
    failed=[r for r in out if r["official_validation_status"]=="REJECTED_BILL_PROVISION_CANDIDATE"]
    by_basis={}; by_section={}
    for r in failed:
        by_basis[r.get("ownership_basis")]=by_basis.get(r.get("ownership_basis"),0)+1
        by_section[r.get("normalized_section")]=by_section.get(r.get("normalized_section"),0)+1
    validated=[r for r in out if r["official_validation_status"].startswith("VALIDATED_BILL_PROVISION_")]
    print("VALIDATED_PROVISIONS")
    for r in validated:
        print({"activity_id":r.get("activity_id"),"bill":r.get("owner"),
               "section":r.get("normalized_section"),
               "status":r.get("official_validation_status"),
               "text_version":r.get("newest_text_version"),
               "text_date":r.get("newest_text_date"),
               "matching_versions":r.get("matching_text_versions"),
               "heading":r.get("official_section_heading"),
               "clause":(r.get("clause_text") or "")[:500]})
    print("FAILED_BY_OWNERSHIP_BASIS")
    for k,v in sorted(by_basis.items(),key=lambda x:-x[1]):
        print({"basis":k,"count":v,"pct_of_failed":round(100*v/len(failed),1) if failed else 0})
    print("TOP_FAILED_SECTIONS")
    for k,v in sorted(by_section.items(),key=lambda x:(-x[1],str(x[0])))[:40]:
        print({"section":k,"count":v})
    print("FAILED_CONTEXT_SAMPLES")
    shown={}
    for r in failed:
        sec=r.get("normalized_section")
        if shown.get(sec,0)>=2 or by_section.get(sec,0)<2: continue
        print({"section":sec,"owner":r.get("owner"),"basis":r.get("ownership_basis"),
               "clause":(r.get("clause_text") or "")[:500]})
        shown[sec]=shown.get(sec,0)+1
        if sum(shown.values())>=30: break

if __name__=="__main__": main()
