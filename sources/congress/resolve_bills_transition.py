"""Transition-aware LDA bill resolver for quarters spanning two Congresses."""
import argparse, json, os
from collections import Counter, defaultdict
import psycopg2, requests
from dotenv import load_dotenv
from resolve_bills import api, extract, mention_resolution, committee_evidence, title_evidence, mention_window

def choose(description,start,end,matched,bills):
    rows=[]
    for congress,bill in bills.items():
        status,method,confidence,context=mention_resolution(description,start,end,congress,bill.get("title",""),matched,bill)
        overlap=title_evidence(mention_window(description,start,end),bill.get("title",""))
        rows.append((congress,status,method,float(confidence),context,overlap))
    verified=[r for r in rows if r[1]=="verified"]
    if len(verified)==1: return verified[0],"verified"
    if len(verified)>1: return None,"multiple_verified"
    viable=[r for r in rows if r[1]!="rejected"]
    if len(viable)==1: return viable[0],"single_viable"
    if len(viable)>1:
        ranked=sorted(viable,key=lambda r:(r[3],r[5]),reverse=True); a,b=ranked[:2]
        if a[3]>=.65 and a[3]-b[3]>=.20: return a,"title_confidence_margin"
        if a[5]>=.55 and a[5]-b[5]>=.25: return a,"title_overlap_margin"
        return None,"same_number_ambiguous"
    return None,"all_rejected" if rows else "no_bill"

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--year",type=int,default=2025); p.add_argument("--quarter",default="Q1")
    p.add_argument("--congresses",default="118,119"); p.add_argument("--top",type=int,default=1000)
    p.add_argument("--rank-by",choices=["mentions","dollars","hybrid"],default="hybrid")
    p.add_argument("--write",action="store_true"); args=p.parse_args()
    congresses=[int(x) for x in args.congresses.split(",")]
    load_dotenv(); db_url,key=os.getenv("DATABASE_URL"),os.getenv("CONGRESS_API_KEY")
    if not db_url or not key: raise SystemExit("Set DATABASE_URL and CONGRESS_API_KEY in .env")
    conn=psycopg2.connect(db_url); conn.autocommit=False
    with conn.cursor() as cur:
        cur.execute("""SELECT a.activity_id,a.description,a.filing_uuid,COALESCE(f.income,f.expenses,0)
        FROM lda.activity a JOIN lda.filing f USING(filing_uuid)
        WHERE f.filing_year=%s AND f.filing_type=%s AND a.description IS NOT NULL""",(args.year,args.quarter))
        source=cur.fetchall()
    counts=Counter(); evidence=defaultdict(list); amounts={}; filing_tokens=defaultdict(set)
    for aid,desc,fid,amount in source:
        amounts[fid]=float(amount or 0)
        for token,matched,start,end in extract(desc):
            counts[token]+=1; evidence[token].append((aid,matched,desc,start,end)); filing_tokens[fid].add(token)
    dollars=Counter()
    for fid,tokens in filing_tokens.items():
        if tokens:
            share=amounts.get(fid,0)/len(tokens)
            for token in tokens: dollars[token]+=share
    if args.rank_by=="dollars": ranked=sorted(counts,key=lambda x:(dollars[x],counts[x]),reverse=True)
    elif args.rank_by=="hybrid":
        mr={k:i for i,(k,_) in enumerate(counts.most_common(),1)}
        dr={k:i for i,k in enumerate(sorted(counts,key=lambda x:dollars[x],reverse=True),1)}
        ranked=sorted(counts,key=lambda x:(mr[x]+dr[x],-counts[x]))
    else: ranked=[k for k,_ in counts.most_common()]
    session=requests.Session(); decisions=Counter(); assigned=Counter(); resolved=set()
    try:
        for typ,number in ranked[:args.top]:
            bills={}
            for congress in congresses:
                try:
                    b=api(session,key,f"/bill/{congress}/{typ.lower()}/{number}").get("bill",{})
                    if b and int(b.get("number",0))==number: bills[congress]=b
                except Exception: pass
            selected=defaultdict(list)
            for row in evidence[(typ,number)]:
                aid,matched,desc,start,end=row; picked,reason=choose(desc,start,end,matched,bills); decisions[reason]+=1
                if picked:
                    congress,status,method,confidence,context,_=picked
                    selected[congress].append((row,status,method,confidence,context)); assigned[congress]+=1
            print(f"{typ} {number} mentions={len(evidence[(typ,number)])} assigned={dict((k,len(v)) for k,v in selected.items())} unresolved={len(evidence[(typ,number)])-sum(map(len,selected.values()))}")
            for congress,items in selected.items():
                bill=bills[congress]; committees=api(session,key,f"/bill/{congress}/{typ.lower()}/{number}/committees").get("committees",[])
                ev=[committee_evidence(c) for c in committees]
                if len(ev)==1 and ev[0]["evidence_class"]=="other_or_unknown": ev[0]["pressure_eligible"]=True
                resolved.add((congress,typ,number))
                if not args.write: continue
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO policy.bill(congress,bill_type,bill_number,title,congress_url,source_json)
                    VALUES(%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(congress,bill_type,bill_number) DO UPDATE SET
                    title=EXCLUDED.title,congress_url=EXCLUDED.congress_url,source_json=EXCLUDED.source_json,verified_at=now()""",
                    (congress,typ,number,bill.get("title",""),bill.get("url"),json.dumps(bill)))
                    for row,status,method,confidence,context in items:
                        aid,matched,desc,start,end=row
                        cur.execute("""INSERT INTO policy.bill_mention(activity_id,congress,bill_type,bill_number,evidence,resolution_status,evidence_context,resolution_method,resolution_confidence)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(activity_id,congress,bill_type,bill_number) DO UPDATE SET
                        evidence=EXCLUDED.evidence,resolution_status=EXCLUDED.resolution_status,evidence_context=EXCLUDED.evidence_context,resolution_method=EXCLUDED.resolution_method,resolution_confidence=EXCLUDED.resolution_confidence""",
                        (aid,congress,typ,number,matched,status,context,method,confidence))
                    for c,e in zip(committees,ev):
                        code=c.get("systemCode")
                        if not code: continue
                        cur.execute("""INSERT INTO policy.committee(committee_code,name,chamber,source_json) VALUES(%s,%s,%s,%s::jsonb)
                        ON CONFLICT(committee_code) DO UPDATE SET name=EXCLUDED.name,chamber=EXCLUDED.chamber,source_json=EXCLUDED.source_json""",
                        (code,c.get("name",""),c.get("chamber"),json.dumps(c)))
                        cur.execute("""INSERT INTO policy.bill_committee(congress,bill_type,bill_number,committee_code,activity_json)
                        VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT(congress,bill_type,bill_number,committee_code) DO UPDATE SET activity_json=EXCLUDED.activity_json""",
                        (congress,typ,number,code,json.dumps(e)))
                conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        print(f"SUMMARY resolved_bills={len(resolved)} assigned={dict(assigned)} decisions={dict(decisions)}")
        session.close(); conn.close()

if __name__=="__main__": main()
