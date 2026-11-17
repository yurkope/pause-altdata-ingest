#!/usr/bin/env python3
"""QC a normalized congressional trade extract before external validation."""
import argparse,csv
from collections import Counter
from datetime import date

def main():
 p=argparse.ArgumentParser();p.add_argument("csv");p.add_argument("--year",type=int,default=2025);a=p.parse_args()
 rows=list(csv.DictReader(open(a.csv,encoding="utf-8",newline="")))
 keys=set();dups=0;ch=Counter();types=Counter();tick=0;bio=0;src=0;bad_date=0
 for r in rows:
  k=(r.get("source_url"),r.get("member_name"),r.get("ticker"),r.get("transaction_date"),r.get("transaction_type"),r.get("owner"),r.get("amount_range"))
  if k in keys:dups+=1
  keys.add(k);ch[r.get("chamber","")]+=1;types[r.get("transaction_type","")]+=1
  tick+=bool((r.get("ticker")or"").strip());bio+=bool((r.get("bioguide_id")or"").strip());src+=bool((r.get("source_url")or"").strip())
  try:
   d=date.fromisoformat((r.get("transaction_date")or"")[:10]);bad_date+=d.year!=a.year
  except:bad_date+=1
 n=len(rows) or 1
 print("rows",len(rows));print("chambers",dict(ch));print("transaction_types",dict(types))
 print("duplicate_keys",dups);print("ticker_coverage_pct",round(100*tick/n,2))
 print("bioguide_coverage_pct",round(100*bio/n,2));print("source_url_coverage_pct",round(100*src/n,2))
 print("invalid_or_out_of_year_dates",bad_date)
 if dups or bad_date or src<n: raise SystemExit("QC FAIL")
 print("QC PASS")
if __name__=="__main__":main()
