#!/usr/bin/env python3
import argparse,json,re
import numpy as np,pandas as pd,yfinance as yf
from scipy.stats import ttest_1samp
SUF={"inc","incorporated","corp","corporation","co","company","llc","lp","ltd","limited","plc","holdings","holding","group","the"}
def norm(s):
 w=re.findall(r"[a-z0-9]+",str(s).lower())
 while w and w[-1] in SUF:w.pop()
 if w and w[0]=="the":w=w[1:]
 return "".join(w)
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--exposure",action="append",required=True);ap.add_argument("--scores",action="append",required=True);ap.add_argument("--sec",required=True);ap.add_argument("--out",required=True);a=ap.parse_args()
 ex=pd.concat([pd.read_csv(x) for x in a.exposure]); ex=ex.rename(columns={"reconstruction_year":"year"}); sc=pd.concat([pd.read_csv(x) for x in a.scores])
 sec=json.load(open(a.sec)); vals=sec.values() if isinstance(sec,dict) else sec
 names={}
 for x in vals:
  n=norm(x.get("title",""));t=x.get("ticker")
  if n and t:names.setdefault(n,set()).add(t)
 ex["n"]=ex.name.map(norm);ex["ticker"]=ex.n.map(lambda n:next(iter(names[n])) if n in names and len(names[n])==1 else None)
 ex=ex[ex.ticker.notna()].copy()
 sc=sc.sort_values(["policy_code","year","quarter_num"]) if "year" in sc else sc
 if "year" not in sc.columns: raise SystemExit("score files must include year")
 sc["delta"]=sc.groupby(["policy_code","year"]).economic_pressure_score.diff();sc=sc[sc.delta.notna()]
 sc["pct"]=sc.groupby(["year","quarter_num"]).delta.rank(pct=True,method="average")
 top=sc[sc.pct>.75][["year","quarter_num","policy_code","delta"]]
 u=top.merge(ex,on=["year","quarter_num","policy_code"])
 # one ticker once per quarter even if multiple client records/policies
 u=u.sort_values(["year","quarter_num","ticker","policy_code"]).drop_duplicates(["year","quarter_num","ticker"])
 tick=sorted(set(u.ticker)|{"SPY"});px=yf.download(tick,start="2023-04-15",end="2025-08-01",auto_adjust=True,progress=False,group_by="column",threads=True)["Close"]
 def p(t,d):
  z=px[t].dropna();z=z[z.index>=pd.Timestamp(d)];return float(z.iloc[0]) if len(z) else np.nan
 rec=[]
 for _,r in u.iterrows():
  st=pd.Timestamp(int(r.year),int(r.quarter_num)*3,1)+pd.offsets.MonthEnd(0)+pd.Timedelta(days=20);en=st+pd.Timedelta(days=90)
  v=[p(r.ticker,st),p(r.ticker,en),p("SPY",st),p("SPY",en)]
  if np.all(np.isfinite(v)):rec.append([r.year,r.quarter_num,r.policy_code,r.ticker,r["name"],v[1]/v[0]-1,v[3]/v[2]-1,v[1]/v[0]-v[3]/v[2]])
 d=pd.DataFrame(rec,columns=["year","quarter","policy","ticker","client_name","return","spy_return","excess"]);d.to_csv(a.out,index=False)
 q=d.groupby(["year","quarter"]).excess.mean();t=ttest_1samp(q,0)
 print({"matched_client_policy_rows":len(ex),"matched_tickers":ex.ticker.nunique(),"selected_security_observations":len(d),"signal_quarters":len(q),"mean_90d_excess":float(q.mean()),"t_pvalue":float(t.pvalue)})
 print(q.to_string())
if __name__=="__main__":main()
