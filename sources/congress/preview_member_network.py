"""Preview member-level Congress.gov relationships for policy-network bills.

Uses official Congress.gov API and the existing policy.bill universe.
Default is read-only preview. This intentionally separates:
  sponsor/cosponsor = legislative relationship
  committee member = jurisdictional relationship
Neither relationship proves direct lobbying contact.

Requires DATABASE_URL and CONGRESS_API_KEY in .env.
"""
import argparse, os, requests, psycopg2
from dotenv import load_dotenv

BASE="https://api.congress.gov/v3"

def api(session,key,path,params=None):
    p={"api_key":key,"format":"json","limit":250}
    if params: p.update(params)
    r=session.get(BASE+path,params=p,timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--congress",type=int,default=119)
    p.add_argument("--limit",type=int,default=25,
                   help="Preview this many highest-linked bills")
    args=p.parse_args()
    load_dotenv()
    db=os.getenv("DATABASE_URL"); key=os.getenv("CONGRESS_API_KEY")
    if not db or not key:
        raise SystemExit("Set DATABASE_URL and CONGRESS_API_KEY in .env")

    conn=psycopg2.connect(db)
    with conn.cursor() as cur:
        cur.execute("""
          SELECT bm.congress,bm.bill_type,bm.bill_number,b.title,
                 count(DISTINCT bm.activity_id) activity_mentions
          FROM policy.bill_mention bm
          JOIN policy.bill b USING(congress,bill_type,bill_number)
          JOIN policy.activity_policy_evidence e ON e.activity_id=bm.activity_id
          WHERE bm.congress=%s
            AND bm.resolution_confidence>=0.85
            AND e.classifier_version='permitting-child-v1'
          GROUP BY 1,2,3,4
          ORDER BY activity_mentions DESC
          LIMIT %s
        """,(args.congress,args.limit))
        bills=cur.fetchall()

    s=requests.Session()
    for congress,typ,num,title,mentions in bills:
        slug=typ.lower()
        detail=api(s,key,f"/bill/{congress}/{slug}/{num}").get("bill",{})
        sponsors=detail.get("sponsors") or []
        cos=api(s,key,f"/bill/{congress}/{slug}/{num}/cosponsors").get("cosponsors",[])
        committees=api(s,key,f"/bill/{congress}/{slug}/{num}/committees").get("committees",[])
        print({
          "bill":f"{typ} {num}","title":title,"activity_mentions":mentions,
          "sponsors":[{"bioguideId":x.get("bioguideId"),"name":x.get("fullName")}
                      for x in sponsors],
          "cosponsor_count":len(cos),
          "committee_codes":[x.get("systemCode") for x in committees if x.get("systemCode")]
        })

if __name__=="__main__":
    main()
