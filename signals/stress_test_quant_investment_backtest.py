#!/usr/bin/env python3
import argparse,calendar
from pathlib import Path
import numpy as np,pandas as pd,yfinance as yf
from scipy.stats import ttest_1samp

SECTOR={
"AI_POLICY":"XLK","CYBERSECURITY":"XLK","DIGITAL_ASSETS":"XLF","DEFENSE_APPROPRIATIONS":"XLI",
"FAA_AVIATION":"XLI","BANK_CAPITAL":"XLF","BROADBAND_BEAD":"XLC","CLEAN_ENERGY_TAX_IRA":"XLU",
"CRITICAL_MINERALS_SUPPLY_CHAIN":"XLB","DRUG_PRICING":"XLV","PBM":"XLV",
"AUTONOMOUS_VEHICLES":"XLY","PATENT_POLICY":"XLK","TRADE_TARIFFS":"XLB"}
def qend(y,q): return pd.Timestamp(y,3*q,calendar.monthrange(y,3*q)[1])+pd.Timedelta(days=20)
def load(p,y):
 d=pd.read_csv(p);d["year"]=y;return d[["policy_code","quarter_num","economic_pressure_score","year"]]
def main():
 ap=argparse.ArgumentParser()
 for x in ["q23","q24","q25","map","out"]:ap.add_argument("--"+x,required=True)
 a=ap.parse_args()
 s=pd.concat([load(a.q23,2023),load(a.q24,2024),load(a.q25,2025)])
 m=pd.read_csv(a.map);m=m[m.include_primary.astype(str).str.lower().eq("true")]
 policies=sorted(set(m.policy_code)&set(s.policy_code)&set(SECTOR));s=s[s.policy_code.isin(policies)].sort_values(["policy_code","year","quarter_num"])
 s["delta"]=s.groupby(["policy_code","year"]).economic_pressure_score.diff()
 s=s[s.delta.notna()].copy();s["pct"]=s.groupby(["year","quarter_num"]).delta.rank(pct=True,method="average")
 allrows=s.merge(m[["policy_code","ticker"]],on="policy_code")
 tickers=sorted(set(allrows.ticker)|{"SPY"}|set(SECTOR.values()))
 px=yf.download(tickers,start="2023-04-15",end="2026-08-15",auto_adjust=True,progress=False,group_by="column",threads=True)["Close"]
 def price(t,dt):
  z=px[t].dropna();z=z[z.index>=dt];return float(z.iloc[0]) if len(z) else np.nan
 rec=[]
 for _,r in allrows.iterrows():
  st=qend(int(r.year),int(r.quarter_num));en=st+pd.Timedelta(days=90); sec=SECTOR[r.policy_code]
  vals=[price(t,d) for t,d in [(r.ticker,st),(r.ticker,en),("SPY",st),("SPY",en),(sec,st),(sec,en)]]
  if not np.all(np.isfinite(vals)):continue
  ret=vals[1]/vals[0]-1;spy=vals[3]/vals[2]-1;sret=vals[5]/vals[4]-1
  rec.append([r.year,r.quarter_num,r.policy_code,r.ticker,r.delta,r.pct,ret,ret-spy,ret-sret,sec])
 d=pd.DataFrame(rec,columns=["year","quarter","policy","ticker","delta","pct","ret","spy_excess","sector_excess","sector"])
 d.to_csv(a.out,index=False)
 top=d[d.pct>.75];bot=d[d.pct<=.25]
 def qe(g,col):return g.groupby(["year","quarter"])[col].mean()
 def stat(v):
  t=ttest_1samp(v,0);return float(v.mean()),float(t.pvalue),len(v)
 print("TOP_SPY",stat(qe(top,"spy_excess")));print("TOP_SECTOR",stat(qe(top,"sector_excess")))
 # top-minus-bottom by quarter, ticker equal weight within side
 tq=qe(top,"spy_excess");bq=qe(bot,"spy_excess");ls=(tq-bq).dropna();print("TOP_MINUS_BOTTOM_SPY",stat(ls))
 tq2=qe(top,"sector_excess");bq2=qe(bot,"sector_excess");ls2=(tq2-bq2).dropna();print("TOP_MINUS_BOTTOM_SECTOR",stat(ls2))
 # leave one quarter out
 base=qe(top,"spy_excess")
 print("LOQ")
 for idx in base.index: print(idx,float(base.drop(idx).mean()))
 # leave one policy out: recompute top portfolio excluding tickers of that policy
 print("LOP")
 for p in sorted(top.policy.unique()):
  v=qe(top[top.policy.ne(p)],"spy_excess");print(p,float(v.mean()))
 # policy concentration contribution
 pc=top.groupby("policy").agg(n=("ticker","size"),mean_excess=("spy_excess","mean"))
 print("POLICY_CONCENTRATION");print(pc.sort_values("mean_excess",ascending=False).to_string())
 # placebo: choose same number of policies as observed top each quarter from mapped policy universe, compare ticker EW SPY excess
 rng=np.random.default_rng(20261117);obs=base.mean(); sims=[]
 qs=list(s[["year","quarter_num"]].drop_duplicates().itertuples(index=False,name=None))
 for _ in range(50000):
  vv=[]
  for y,q in qs:
   ss=s[(s.year.eq(y))&(s.quarter_num.eq(q))]; k=int((ss.pct>.75).sum())
   chosen=set(rng.choice(ss.policy_code.unique(),size=k,replace=False))
   g=d[(d.year.eq(y))&(d.quarter.eq(q))&d.policy.isin(chosen)]
   if len(g):vv.append(g.spy_excess.mean())
  sims.append(np.mean(vv))
 p=(np.sum(np.array(sims)>=obs)+1)/(len(sims)+1)
 print("PLACEBO one-sided",obs,p,"median",float(np.median(sims)),"p95",float(np.quantile(sims,.95)))
if __name__=="__main__":main()
