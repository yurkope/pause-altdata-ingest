#!/usr/bin/env python3
"""Fetch a narrow congressional stock-trade slice from a Datasette JSON API.

Designed for external validation artifacts; no Neon writes.
Example:
 python signals/fetch_congress_stock_trades.py \
   --base-url https://regs.datadawn.org \
   --database openregs --year 2025 --out data/congress_stock_trades_2025.csv

The endpoint/database name can be overridden because serving layouts can change.
"""
from __future__ import annotations
import argparse,csv,json,urllib.parse,urllib.request
from pathlib import Path

COLS=["member_name","bioguide_id","chamber","transaction_date","ticker",
"asset_description","transaction_type","amount_range","owner","source_url","filing_type"]

def get_json(url):
    req=urllib.request.Request(url,headers={"User-Agent":"Quant-research/1.0"})
    with urllib.request.urlopen(req,timeout=60) as r:
        return json.load(r)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--base-url",required=True); p.add_argument("--database",required=True)
    p.add_argument("--year",type=int,default=2025); p.add_argument("--out",required=True)
    p.add_argument("--page-size",type=int,default=1000)
    a=p.parse_args()
    base=a.base_url.rstrip("/")+"/"+urllib.parse.quote(a.database)+".json"
    select="member_name,bioguide_id,chamber,transaction_date,ticker,asset_description,transaction_type,amount_range,owner,source_url"
    rows=[]; offset=0
    while True:
        sql=(f"SELECT {select} FROM stock_trades "
             f"WHERE transaction_date >= '{a.year}-01-01' "
             f"AND transaction_date < '{a.year+1}-01-01' "
             "ORDER BY transaction_date, member_name, ticker, source_url, rowid "
             f"LIMIT {a.page_size} OFFSET {offset}")
        url=base+"?"+urllib.parse.urlencode({"sql":sql,"_shape":"objects"})
        obj=get_json(url); batch=obj.get("rows") or obj.get("results") or []
        if batch and isinstance(batch[0],list):
            cols=obj.get("columns") or COLS[:-1]
            batch=[dict(zip(cols,r)) for r in batch]
        rows.extend(batch)
        if len(batch)<a.page_size: break
        offset+=a.page_size
    dedup=[]; seen=set()
    for r in rows:
        key=(r.get("source_url",""),r.get("member_name",""),r.get("ticker",""),
             r.get("transaction_date",""),r.get("transaction_type",""),r.get("owner",""),
             r.get("amount_range",""))
        if key in seen: continue
        seen.add(key); dedup.append(r)
    print(f"raw rows: {len(rows):,}; duplicate keys removed: {len(rows)-len(dedup):,}")
    rows=dedup
    out=Path(a.out); out.parent.mkdir(parents=True,exist_ok=True)
    with out.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=COLS,extrasaction="ignore");w.writeheader()
        for r in rows:w.writerow({k:r.get(k,"") for k in COLS})
    print(f"wrote {len(rows):,} rows to {out}")

if __name__=="__main__":main()
