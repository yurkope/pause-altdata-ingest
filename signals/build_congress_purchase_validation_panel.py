#!/usr/bin/env python3
"""External validation: Quant policy pressure vs congressional purchases.

Research-only. Does not write Neon and must not tune Quant model weights.
Expected trade input columns:
member_name, bioguide_id, chamber, transaction_date, ticker, asset_description,
transaction_type, amount_range, owner, source_url, filing_type
"""
from __future__ import annotations
import argparse, csv, math, re
from collections import defaultdict
from datetime import date
from pathlib import Path

BUY_RE = re.compile(r"(?i)^purchase")
AMOUNT_RE = re.compile(r"\$?([0-9,]+)")

def amount_lower(s: str) -> int:
    m=AMOUNT_RE.search(s or "")
    return int(m.group(1).replace(",","")) if m else 0

def quarter(d: date) -> int:
    return (d.month-1)//3+1

def read_map(path: Path):
    out=defaultdict(set)
    with path.open(newline="",encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["include_primary"].lower()=="true":
                out[r["ticker"].upper()].add(r["policy_code"])
    return out

def read_trades(path: Path, year: int):
    seen=set(); rows=[]
    with path.open(newline="",encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not BUY_RE.search(r.get("transaction_type","")): continue
            try: d=date.fromisoformat(r["transaction_date"][:10])
            except Exception: continue
            if d.year!=year: continue
            ticker=(r.get("ticker") or "").upper().strip()
            if not ticker: continue
            key=(r.get("source_url",""),r.get("member_name",""),ticker,d.isoformat(),
                 r.get("transaction_type",""),r.get("owner",""),r.get("amount_range",""))
            if key in seen: continue
            seen.add(key)
            rows.append({**r,"ticker":ticker,"date":d,"q":quarter(d),
                         "amount_lower":amount_lower(r.get("amount_range",""))})
    return rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--trades",required=True)
    ap.add_argument("--exposure-map",default="signals/congress_trade_policy_exposure_map_v1.csv")
    ap.add_argument("--year",type=int,default=2025)
    ap.add_argument("--out",default="congress_policy_purchase_panel.csv")
    a=ap.parse_args()
    exposure=read_map(Path(a.exposure_map)); trades=read_trades(Path(a.trades),a.year)
    panel=defaultdict(lambda:{"trades":0,"members":set(),"amount_lower":0})
    for t in trades:
        for p in exposure.get(t["ticker"],()):
            k=(p,t["q"]); panel[k]["trades"]+=1
            panel[k]["members"].add(t.get("bioguide_id") or t.get("member_name"))
            panel[k]["amount_lower"]+=t["amount_lower"]
    with Path(a.out).open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["policy_code","quarter_num","purchase_count",
                                     "unique_members","amount_lower_bound"])
        for (p,q),v in sorted(panel.items()):
            w.writerow([p,q,v["trades"],len(v["members"]),v["amount_lower"]])
    print(f"deduped {a.year} purchases: {len(trades):,}")
    print(f"mapped policy-quarter cells with purchases: {len(panel):,}")

if __name__=="__main__": main()
