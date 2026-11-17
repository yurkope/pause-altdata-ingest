#!/usr/bin/env python3
import argparse,csv,os,re
import psycopg2
from psycopg2.extras import execute_values
PARENTS={"DRUG_BIOMEDICAL_POLICY","HEALTHCARE_ACCESS_DELIVERY","HEALTHCARE_COSTS_PBM","FOREST_WILDFIRE","HOUSING","LABOR_EMPLOYMENT","ONLINE_SAFETY_PRIVACY","ENERGY_GRID_INFRASTRUCTURE","TAX_BUSINESS_INNOVATION","APPROPRIATIONS_OTHER","ENVIRONMENTAL_REGULATION"}
CONTEXT={"AIR_PERMITTING","BROADBAND_PERMITTING","BUDGET_RECONCILIATION_OBBBA","BUY_AMERICA","CONTINUING_APPROPRIATIONS","ENDANGERED_SPECIES_ACT","ENERGY_INFRA_PERMITTING","FIN_SERVICES_APPROPRIATIONS","FIX_OUR_FORESTS","HIGHER_EDUCATION_ACT","HOMELAND_SECURITY_APPROPRIATIONS","INFRASTRUCTURE_IIJA","MINING_PERMITTING","NASA_AUTHORIZATION","NEPA_REVIEW","RENEWABLE_FUEL_STANDARD","ROAD_TO_HOUSING","TAX_CUTS_JOBS_ACT","TRANSMISSION_PERMITTING","VOTING_RIGHTS","WATER_PERMITTING","WORKFORCE_WIOA","WRDA_WATER_RESOURCES","AM_RADIO_VEHICLES"}
PARENT_OF={"DRUG_340B":"DRUG_BIOMEDICAL_POLICY","DRUG_PRICING":"DRUG_BIOMEDICAL_POLICY","RARE_DISEASE":"DRUG_BIOMEDICAL_POLICY","ACA":"HEALTHCARE_ACCESS_DELIVERY","CMS_MEDICARE_PAYMENT":"HEALTHCARE_ACCESS_DELIVERY","MEDICAID":"HEALTHCARE_ACCESS_DELIVERY","PRIOR_AUTH":"HEALTHCARE_ACCESS_DELIVERY","SITE_NEUTRAL":"HEALTHCARE_ACCESS_DELIVERY","TELEHEALTH":"HEALTHCARE_ACCESS_DELIVERY","PBM":"HEALTHCARE_COSTS_PBM","DEFENSE_APPROPRIATIONS":"APPROPRIATIONS_OTHER","ENERGY_WATER_APPROPRIATIONS":"APPROPRIATIONS_OTHER","THUD_APPROPRIATIONS":"APPROPRIATIONS_OTHER","PFAS":"ENVIRONMENTAL_REGULATION"}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--year",type=int,required=True);ap.add_argument("--bill-evidence",required=True);ap.add_argument("--output",required=True);a=ap.parse_args()
 source=open("signals/activity_policy_pressure_v1.sql").read();prefix=source.split("), fl AS",1)[0]+")";prefix=prefix.replace("f.filing_year=2025",f"f.filing_year={a.year}")
 labels=set(re.findall(r"\('([A-Z][A-Z0-9_]+)'\s*,",prefix));br=list(csv.DictReader(open(a.bill_evidence)));labels.update(x["policy_code"] for x in br)
 meta=[(p,"context" if p in CONTEXT else ("parent" if p in PARENTS else "substantive"),PARENT_OF.get(p)) for p in sorted(labels)]
 with psycopg2.connect(os.environ["DATABASE_URL"]) as conn,conn.cursor() as cur:
  cur.execute("CREATE TEMP TABLE be(activity_id bigint,policy_code text)"); 
  if br: execute_values(cur,"INSERT INTO be VALUES %s",[(int(x["activity_id"]),x["policy_code"]) for x in br])
  cur.execute("CREATE TEMP TABLE pm(policy_code text primary key,role text,parent text)");execute_values(cur,"INSERT INTO pm VALUES %s",meta)
  sql=prefix+""",
ev AS (SELECT DISTINCT activity_id,label policy_code FROM labels UNION SELECT DISTINCT activity_id,policy_code FROM be),
e AS (SELECT ev.*,pm.role,pm.parent FROM ev JOIN pm USING(policy_code)),
r AS (SELECT e.* FROM e WHERE role='substantive' OR (role='parent' AND NOT EXISTS(SELECT 1 FROM e ch WHERE ch.activity_id=e.activity_id AND ch.parent=e.policy_code)))
SELECT %s year,substring(f.filing_type from 2 for 1)::int quarter_num,r.policy_code,c.client_id,c.name,count(DISTINCT r.activity_id) activities,count(DISTINCT f.filing_uuid) filings
FROM r JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid) JOIN lda.client c USING(client_id) JOIN pm USING(policy_code)
WHERE f.filing_year=%s AND f.filing_type IN('Q1','Q2','Q3','Q4') AND pm.role='substantive'
GROUP BY 1,2,3,4,5 ORDER BY 2,3,5"""
  cur.execute(sql,(a.year,a.year));rows=cur.fetchall();cols=[x.name for x in cur.description]
 with open(a.output,"w",newline="") as o:w=csv.writer(o);w.writerow(cols);w.writerows(rows)
 print({"year":a.year,"rows":len(rows),"clients":len(set(x[3] for x in rows)),"policies":len(set(x[2] for x in rows))})
if __name__=="__main__":main()
