#!/usr/bin/env python3
import argparse,calendar,csv,math
from pathlib import Path
import numpy as np,pandas as pd,yfinance as yf
from scipy.stats import spearmanr,ttest_1samp

def qend(y,q): return pd.Timestamp(y,3*q,calendar.monthrange(y,3*q)[1])
def load_scores(path,year):
 d=pd.read_csv(path); d["year"]=year
 return d[["policy_code","quarter_num","economic_pressure_score","year"]]
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--q23",required=True);ap.add_argument("--q24",required=True);ap.add_argument("--q25",required=True);ap.add_argument("--map",required=True);ap.add_argument("--out",required=True);a=ap.parse_args()
 scores=pd.concat([load_scores(a.q23,2023),load_scores(a.q24,2024),load_scores(a.q25,2025)],ignore_index=True)
 m=pd.read_csv(a.map);m=m[m.include_primary.astype(str).str.lower().eq("true")].copy()
 policies=sorted(set(m.policy_code)&set(scores.policy_code)); scores=scores[scores.policy_code.isin(policies)].copy()
 scores=scores.sort_values(["policy_code","year","quarter_num"])
 # do not bridge year boundary: q/q delta within year only
 scores["delta"]=scores.groupby(["policy_code","year"]).economic_pressure_score.diff()
 sig=scores[scores.delta.notna()].copy()
 sig["pct"]=sig.groupby(["year","quarter_num"]).delta.rank(pct=True,method="average")
 sig["selected"]=sig.pct.gt(.75)
 rows=sig[sig.selected].merge(m[["policy_code","ticker"]],on="policy_code")
 tickers=sorted(set(rows.ticker)|{"SPY"})
 px=yf.download(tickers,start="2023-03-30",end="2026-08-01",auto_adjust=True,progress=False,group_by="column",threads=True)["Close"]
 if isinstance(px,pd.Series): px=px.to_frame()
 horizons=[30,60,90,180]; out=[]
 for _,r in rows.iterrows():
  start=qend(int(r.year),int(r.quarter_num))
  for h in horizons:
   end=start+pd.Timedelta(days=h)
   def first_on_after(ticker,dt):
    s=px[ticker].dropna(); z=s[s.index>=dt]; return (z.index[0],float(z.iloc[0])) if len(z) else (None,np.nan)
   sd,sp=first_on_after(r.ticker,start); ed,ep=first_on_after(r.ticker,end)
   _,bsp=first_on_after("SPY",start); _,bep=first_on_after("SPY",end)
   if not all(np.isfinite([sp,ep,bsp,bep])): continue
   ret=ep/sp-1; spy=bep/bsp-1
   out.append([r.year,r.quarter_num,r.policy_code,r.ticker,r.delta,h,sd.date(),ed.date(),ret,spy,ret-spy])
 detail=pd.DataFrame(out,columns=["year","quarter_num","policy_code","ticker","pressure_delta","horizon_days","start_date","end_date","return","spy_return","excess_return"])
 detail.to_csv(a.out,index=False)
 summ=[]
 rng=np.random.default_rng(20261117)
 for h,g in detail.groupby("horizon_days"):
  # equal-weight tickers, then average within signal quarter to avoid large policy baskets dominating
  q=g.groupby(["year","quarter_num"]).excess_return.mean()
  t=ttest_1samp(q,0)
  # sign-flip randomization across independent-ish signal quarters
  obs=q.mean(); vals=np.array([np.mean(q.to_numpy()*rng.choice([-1,1],len(q))) for _ in range(100000)])
  pp=(np.sum(np.abs(vals)>=abs(obs))+1)/(len(vals)+1)
  summ.append([h,len(g),len(q),g["return"].mean(),g.spy_return.mean(),g.excess_return.mean(),q.mean(),t.statistic,t.pvalue,pp])
 s=pd.DataFrame(summ,columns=["horizon_days","ticker_observations","signal_quarters","mean_ticker_return","mean_spy_return","mean_ticker_excess","mean_quarter_equal_weight_excess","t_stat","t_pvalue","signflip_pvalue"])
 s.to_csv(Path(a.out).with_name("quant_investment_backtest_summary.csv"),index=False)
 print("mapped policies",len(policies),"primary tickers",m.ticker.nunique(),"signal quarters",sig[["year","quarter_num"]].drop_duplicates().shape[0])
 print("selected policy-signals",rows[["year","quarter_num","policy_code"]].drop_duplicates().shape[0])
 print(s.to_string(index=False))
 print("\n90d quarter portfolios")
 print(detail[detail.horizon_days.eq(90)].groupby(["year","quarter_num"]).agg(n=("ticker","nunique"),ret=("return","mean"),spy=("spy_return","mean"),excess=("excess_return","mean")).to_string())
if __name__=="__main__":main()
