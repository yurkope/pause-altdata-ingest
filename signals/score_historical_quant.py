#!/usr/bin/env python3
"""Historical Quant scorer: deterministic text + frozen bill evidence, conserved dollars."""
import argparse,csv,os,re
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

PARENTS={"DRUG_BIOMEDICAL_POLICY","HEALTHCARE_ACCESS_DELIVERY","HEALTHCARE_COSTS_PBM",
"FOREST_WILDFIRE","HOUSING","LABOR_EMPLOYMENT","ONLINE_SAFETY_PRIVACY",
"ENERGY_GRID_INFRASTRUCTURE","TAX_BUSINESS_INNOVATION","APPROPRIATIONS_OTHER",
"ENVIRONMENTAL_REGULATION"}
CONTEXT={"AIR_PERMITTING","BROADBAND_PERMITTING","BUDGET_RECONCILIATION_OBBBA","BUY_AMERICA",
"CONTINUING_APPROPRIATIONS","ENDANGERED_SPECIES_ACT","ENERGY_INFRA_PERMITTING",
"FIN_SERVICES_APPROPRIATIONS","FIX_OUR_FORESTS","HIGHER_EDUCATION_ACT",
"HOMELAND_SECURITY_APPROPRIATIONS","INFRASTRUCTURE_IIJA","MINING_PERMITTING",
"NASA_AUTHORIZATION","NEPA_REVIEW","RENEWABLE_FUEL_STANDARD","ROAD_TO_HOUSING",
"TAX_CUTS_JOBS_ACT","TRANSMISSION_PERMITTING","VOTING_RIGHTS","WATER_PERMITTING",
"WORKFORCE_WIOA","WRDA_WATER_RESOURCES","AM_RADIO_VEHICLES"}
PARENT_OF={"DRUG_340B":"DRUG_BIOMEDICAL_POLICY","DRUG_PRICING":"DRUG_BIOMEDICAL_POLICY","RARE_DISEASE":"DRUG_BIOMEDICAL_POLICY",
"ACA":"HEALTHCARE_ACCESS_DELIVERY","CMS_MEDICARE_PAYMENT":"HEALTHCARE_ACCESS_DELIVERY","MEDICAID":"HEALTHCARE_ACCESS_DELIVERY",
"PRIOR_AUTH":"HEALTHCARE_ACCESS_DELIVERY","SITE_NEUTRAL":"HEALTHCARE_ACCESS_DELIVERY","TELEHEALTH":"HEALTHCARE_ACCESS_DELIVERY",
"PBM":"HEALTHCARE_COSTS_PBM","DEFENSE_APPROPRIATIONS":"APPROPRIATIONS_OTHER","ENERGY_WATER_APPROPRIATIONS":"APPROPRIATIONS_OTHER",
"THUD_APPROPRIATIONS":"APPROPRIATIONS_OTHER","PFAS":"ENVIRONMENTAL_REGULATION"}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--year",type=int,required=True)
    ap.add_argument("--bill-evidence",required=True);ap.add_argument("--output",required=True)
    ap.add_argument("--audit-output",required=True);args=ap.parse_args();load_dotenv()
    source=open("signals/activity_policy_pressure_v1.sql",encoding="utf-8").read()
    prefix=source.split("), fl AS",1)[0]+")"
    prefix=prefix.replace("f.filing_year=2025",f"f.filing_year={args.year}")
    labels=set(re.findall(r"\('([A-Z][A-Z0-9_]+)'\s*,",prefix))
    bill_rows=list(csv.DictReader(open(args.bill_evidence,encoding="utf-8")))
    labels.update(r["policy_code"] for r in bill_rows)
    meta=[]
    for p in sorted(labels):
        role="context" if p in CONTEXT else ("parent" if p in PARENTS else "substantive")
        meta.append((p,role,PARENT_OF.get(p)))
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn,conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE hist_bill_evidence(activity_id bigint,policy_code text,confidence numeric,source_congress int,source_bill_type text,source_bill_number int,resolution_method text,evidence text,evidence_context text)")
        if bill_rows:
            execute_values(cur,"INSERT INTO hist_bill_evidence VALUES %s",[(int(r["activity_id"]),r["policy_code"],float(r["confidence"]),int(r["source_congress"]),r["source_bill_type"],int(r["source_bill_number"]),r["resolution_method"],r["evidence"],r["evidence_context"]) for r in bill_rows])
        cur.execute("CREATE TEMP TABLE hist_policy_meta(policy_code text primary key,allocation_role text,parent_policy_code text)")
        execute_values(cur,"INSERT INTO hist_policy_meta VALUES %s",meta)
        sql=prefix+""",
, ev AS (
  SELECT DISTINCT activity_id,label AS policy_code,'activity_regex'::text evidence_method FROM labels
  UNION
  SELECT DISTINCT activity_id,policy_code,'bill_anchor'::text FROM hist_bill_evidence
), agg AS (
  SELECT activity_id,policy_code,count(DISTINCT evidence_method) method_count
  FROM ev GROUP BY 1,2
), enriched AS (
  SELECT a.*,m.allocation_role,m.parent_policy_code
  FROM agg a JOIN hist_policy_meta m USING(policy_code)
), resolved AS (
  SELECT e.*,
    CASE WHEN e.allocation_role='substantive' THEN true
         WHEN e.allocation_role='parent' THEN NOT EXISTS (
           SELECT 1 FROM enriched ch WHERE ch.activity_id=e.activity_id AND ch.parent_policy_code=e.policy_code)
         ELSE false END allocation_eligible
  FROM enriched e
), fp AS (
  SELECT DISTINCT f.filing_type,f.filing_uuid,f.client_id,r.policy_code,r.allocation_role
  FROM resolved r JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid)
  WHERE f.filing_year=%s AND f.filing_type IN('Q1','Q2','Q3','Q4') AND r.allocation_role<>'context'
), leaf AS (
  SELECT fp.* FROM fp
  WHERE allocation_role='substantive' OR (allocation_role='parent' AND NOT EXISTS (
    SELECT 1 FROM fp ch JOIN hist_policy_meta m ON m.policy_code=ch.policy_code
    WHERE ch.filing_uuid=fp.filing_uuid AND m.parent_policy_code=fp.policy_code))
), counted AS (
  SELECT leaf.*,count(*) OVER(PARTITION BY filing_uuid)::numeric label_count FROM leaf
), alloc AS (
  SELECT c.*,coalesce(f.income,f.expenses,0)::numeric/c.label_count associated_allocation
  FROM counted c JOIN lda.filing f USING(filing_uuid)
), financial AS (
  SELECT filing_type,substring(filing_type from 2 for 1)::int quarter_num,policy_code,
         sum(associated_allocation)::numeric allocated_amount,
         count(DISTINCT client_id) clients,count(DISTINCT filing_uuid) filings
  FROM alloc a JOIN hist_policy_meta m USING(policy_code)
  WHERE m.allocation_role='substantive' GROUP BY 1,2,3
), breadth AS (
  SELECT f.filing_type,r.policy_code,count(DISTINCT r.activity_id) activities
  FROM resolved r JOIN hist_policy_meta m USING(policy_code)
  JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid)
  WHERE f.filing_year=%s AND m.allocation_role='substantive'
    AND EXISTS(SELECT 1 FROM alloc x WHERE x.filing_uuid=f.filing_uuid AND x.policy_code=r.policy_code)
  GROUP BY 1,2
), raw AS (
  SELECT financial.*,coalesce(breadth.activities,0) activities FROM financial LEFT JOIN breadth USING(filing_type,policy_code)
), ranked AS (
  SELECT r.*,percent_rank() OVER(PARTITION BY filing_type ORDER BY allocated_amount) amount_pct,
    percent_rank() OVER(PARTITION BY filing_type ORDER BY clients) clients_pct,
    percent_rank() OVER(PARTITION BY filing_type ORDER BY activities) activities_pct,
    percent_rank() OVER(PARTITION BY filing_type ORDER BY filings) filings_pct
  FROM raw r
), scored AS (
  SELECT *,round((100*(.40*amount_pct+.30*clients_pct+.20*activities_pct+.10*filings_pct))::numeric,1) economic_pressure_score
  FROM ranked
), lagged AS (
  SELECT s.*,lag(quarter_num) OVER(PARTITION BY policy_code ORDER BY quarter_num) prior_quarter_num,
    lag(economic_pressure_score) OVER(PARTITION BY policy_code ORDER BY quarter_num) prior_score
  FROM scored s
)
SELECT policy_code,%s year,quarter_num,round(allocated_amount,2),clients,activities,filings,economic_pressure_score,
 CASE WHEN prior_quarter_num=quarter_num-1 THEN round(economic_pressure_score-prior_score,1) END score_change,
 CASE WHEN prior_quarter_num IS NULL OR prior_quarter_num<>quarter_num-1 THEN 'NEW_EMERGING'
      WHEN economic_pressure_score-prior_score>=5 THEN 'ACCELERATING'
      WHEN economic_pressure_score-prior_score<=-5 THEN 'COOLING' ELSE 'PERSISTENT' END signal_state
FROM lagged ORDER BY quarter_num,economic_pressure_score DESC
"""
        cur.execute(sql,(args.year,args.year,args.year))
        scores=cur.fetchall()
        cols=["policy_code","year","quarter_num","allocated_amount","unique_clients","activity_count","filing_count","economic_pressure_score","score_change","signal_state"]
        with open(args.output,"w",newline="",encoding="utf-8") as fh:
            w=csv.writer(fh);w.writerow(cols);w.writerows(scores)
        audit_sql=prefix+""",
, ev AS (
 SELECT DISTINCT activity_id,label policy_code FROM labels
 UNION SELECT DISTINCT activity_id,policy_code FROM hist_bill_evidence
), enriched AS (
 SELECT e.*,m.allocation_role,m.parent_policy_code FROM ev e JOIN hist_policy_meta m USING(policy_code)
), fp AS (
 SELECT DISTINCT f.filing_type,f.filing_uuid,e.policy_code,e.allocation_role
 FROM enriched e JOIN lda.activity a USING(activity_id) JOIN lda.filing f USING(filing_uuid)
 WHERE f.filing_year=%s AND f.filing_type IN('Q1','Q2','Q3','Q4') AND e.allocation_role<>'context'
), leaf AS (
 SELECT fp.* FROM fp WHERE allocation_role='substantive' OR (allocation_role='parent' AND NOT EXISTS(
 SELECT 1 FROM fp ch JOIN hist_policy_meta m ON m.policy_code=ch.policy_code
 WHERE ch.filing_uuid=fp.filing_uuid AND m.parent_policy_code=fp.policy_code))
), counted AS (
 SELECT leaf.*,count(*) OVER(PARTITION BY filing_uuid)::numeric n FROM leaf
), alloc AS (
 SELECT c.*,coalesce(f.income,f.expenses,0)::numeric/n amt FROM counted c JOIN lda.filing f USING(filing_uuid)
), chk AS (
 SELECT a.filing_uuid,max(coalesce(f.income,f.expenses,0)::numeric) disclosed,sum(a.amt) allocated
 FROM alloc a JOIN lda.filing f USING(filing_uuid) GROUP BY 1
)
SELECT (SELECT count(*) FROM labels) regex_evidence_rows,
       (SELECT count(*) FROM hist_bill_evidence) bill_evidence_rows,
       (SELECT count(DISTINCT activity_id||':'||policy_code) FROM ev) activity_policy_pairs,
       (SELECT count(DISTINCT filing_uuid) FROM alloc) allocated_filings,
       (SELECT count(*) FROM chk WHERE abs(disclosed-allocated)>.01) conservation_violations,
       (SELECT coalesce(max(abs(disclosed-allocated)),0) FROM chk) max_abs_diff,
       (SELECT coalesce(sum(allocated),0) FROM chk) total_allocated
"""
        cur.execute(audit_sql,(args.year,));audit=cur.fetchone()
        with open(args.audit_output,"w",newline="",encoding="utf-8") as fh:
            w=csv.writer(fh);w.writerow(["regex_evidence_rows","bill_evidence_rows","activity_policy_pairs","allocated_filings","conservation_violations","max_abs_diff","total_allocated"]);w.writerow(audit)
        print(dict(zip(["regex_evidence_rows","bill_evidence_rows","activity_policy_pairs","allocated_filings","conservation_violations","max_abs_diff","total_allocated"],audit)))
        print({"score_rows":len(scores),"policies":len(set(r[0] for r in scores))})
        if audit[4]!=0 or float(audit[5])>.01: raise SystemExit("Dollar conservation gate failed")

if __name__=="__main__": main()
