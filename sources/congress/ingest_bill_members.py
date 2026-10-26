"""Ingest Congress.gov sponsors/cosponsors for bills already resolved in policy.bill.

Default is DRY RUN. --write requires the reviewed policy.member and policy.bill_member
schema to exist. Uses Bioguide ID as stable person identity.

Semantics: sponsor/cosponsor is a legislative relationship only. It does not prove
that any lobbyist or client contacted the member.
"""
import argparse,json,os,time
import psycopg2,requests
from dotenv import load_dotenv

BASE="https://api.congress.gov/v3"

def api(s,key,path,params=None):
    p={"api_key":key,"format":"json","limit":250}
    if params: p.update(params)
    r=s.get(BASE+path,params=p,timeout=30); r.raise_for_status()
    return r.json()

def pages(s,key,path,keyname):
    offset=0
    while True:
        data=api(s,key,path,{"offset":offset})
        rows=data.get(keyname) or []
        for row in rows: yield row
        pag=data.get("pagination") or {}
        nxt=pag.get("next")
        if not nxt or not rows: break
        offset += len(rows)

def member_row(x):
    bid=x.get("bioguideId")
    name=x.get("fullName") or x.get("directOrderName") or ""
    return bid,name,x.get("firstName"),x.get("middleName"),x.get("lastName"),x.get("suffixName"),x.get("party"),x.get("state"),x.get("district")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--congress",type=int,default=119)
    p.add_argument("--write",action="store_true")
    p.add_argument("--sleep",type=float,default=.08)
    args=p.parse_args()
    load_dotenv(); db=os.getenv("DATABASE_URL"); key=os.getenv("CONGRESS_API_KEY")
    if not db or not key: raise SystemExit("Set DATABASE_URL and CONGRESS_API_KEY in .env")
    conn=psycopg2.connect(db); conn.autocommit=False
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT b.congress,b.bill_type,b.bill_number
          FROM policy.bill b JOIN policy.bill_mention bm USING(congress,bill_type,bill_number)
          JOIN policy.activity_policy_evidence e ON e.activity_id=bm.activity_id
          WHERE b.congress=%s AND bm.resolution_confidence>=.85
            AND e.classifier_version='permitting-child-v1'
          ORDER BY 2,3""",(args.congress,))
        bills=cur.fetchall()
    s=requests.Session(); people={}; edges={}
    for congress,typ,num in bills:
        slug=typ.lower()
        detail=api(s,key,f"/bill/{congress}/{slug}/{num}").get("bill",{})
        for x in detail.get("sponsors") or []:
            bid=x.get("bioguideId")
            if bid:
                people[bid]=(member_row(x),x)
                edges[(congress,typ,num,bid,"SPONSOR")]=(None,False,x)
        for x in pages(s,key,f"/bill/{congress}/{slug}/{num}/cosponsors","cosponsors"):
            bid=x.get("bioguideId")
            if not bid: continue
            people[bid]=(member_row(x),x)
            edges[(congress,typ,num,bid,"COSPONSOR")]=(x.get("sponsorshipDate"),bool(x.get("isWithdrawn")),x)
        time.sleep(args.sleep)
    print({"bills":len(bills),"members":len(people),"bill_member_edges":len(edges),
           "sponsors":sum(k[4]=="SPONSOR" for k in edges),
           "cosponsors":sum(k[4]=="COSPONSOR" for k in edges)})
    if not args.write: conn.close(); return
    with conn.cursor() as cur:
        for bid,(row,raw) in people.items():
            _,name,first,middle,last,suffix,party,state,district=row
            chamber="House" if any(k[3]==bid and k[1].upper().startswith("H") for k in edges) else "Senate"
            cur.execute("""INSERT INTO policy.member
              (bioguide_id,direct_order_name,first_name,middle_name,last_name,suffix_name,party,state,district,chamber,source_json)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
              ON CONFLICT(bioguide_id) DO UPDATE SET
              direct_order_name=excluded.direct_order_name,first_name=excluded.first_name,
              middle_name=excluded.middle_name,last_name=excluded.last_name,suffix_name=excluded.suffix_name,
              party=excluded.party,state=excluded.state,district=excluded.district,chamber=excluded.chamber,
              source_json=excluded.source_json,verified_at=now()""",
              (bid,name,first,middle,last,suffix,party,state,district,chamber,json.dumps(raw)))
        for (congress,typ,num,bid,rel),(date,withdrawn,raw) in edges.items():
            cur.execute("""INSERT INTO policy.bill_member
              (congress,bill_type,bill_number,bioguide_id,relationship_type,sponsorship_date,is_withdrawn,source_json)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
              ON CONFLICT(congress,bill_type,bill_number,bioguide_id,relationship_type) DO UPDATE SET
              sponsorship_date=excluded.sponsorship_date,is_withdrawn=excluded.is_withdrawn,
              source_json=excluded.source_json,verified_at=now()""",
              (congress,typ,num,bid,rel,date,withdrawn,json.dumps(raw)))
    conn.commit(); conn.close()

if __name__=="__main__": main()
