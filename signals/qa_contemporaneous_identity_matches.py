#!/usr/bin/env python3
import argparse,json,re
import pandas as pd
SUF={"inc","incorporated","corp","corporation","co","company","llc","lp","ltd","limited","plc","holdings","holding","group","the"}
def norm(s):
 w=re.findall(r"[a-z0-9]+",str(s).lower())
 while w and w[-1] in SUF:w.pop()
 if w and w[0]=="the":w=w[1:]
 return "".join(w)
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--exposure",action="append",required=True);ap.add_argument("--sec",required=True);ap.add_argument("--out",required=True);a=ap.parse_args()
 ex=pd.concat([pd.read_csv(x) for x in a.exposure],ignore_index=True)
 sec=json.load(open(a.sec)); vals=list(sec.values()) if isinstance(sec,dict) else sec
 sm=pd.DataFrame([{"sec_name":x.get("title"),"ticker":x.get("ticker"),"cik":x.get("cik_str"),"n":norm(x.get("title",""))} for x in vals])
 counts=sm.groupby("n").size(); unique=set(counts[counts.eq(1)].index)
 ex["n"]=ex.name.map(norm); ex["sec_candidates"]=ex.n.map(counts).fillna(0).astype(int)
 m=ex[ex.n.isin(unique)].merge(sm,on="n",how="left",suffixes=("_lda","_sec"))
 # QA flags intentionally conservative
 m["very_short_norm"]=m.n.str.len().lt(5)
 m["ticker_nonstandard"]=~m.ticker.fillna("").str.match(r"^[A-Z]{1,5}([.-][A-Z])?$")
 m["name_exact_normalized"]=m.name.map(norm).eq(m.sec_name.map(norm))
 summary={
 "lda_rows":len(ex),"lda_clients":int(ex.client_id.nunique()),"unique_sec_matches":len(m),
 "matched_clients":int(m.client_id.nunique()),"matched_tickers":int(m.ticker.nunique()),
 "short_norm_rows":int(m.very_short_norm.sum()),"nonstandard_ticker_rows":int(m.ticker_nonstandard.sum()),
 "unmatched_rows":int((ex.sec_candidates.eq(0)).sum()),"ambiguous_sec_rows":int((ex.sec_candidates.gt(1)).sum())}
 print(summary)
 m.sort_values(["very_short_norm","ticker_nonstandard","n"],ascending=[False,False,True]).to_csv(a.out,index=False)
 print("SUSPICIOUS")
 print(m[m.very_short_norm|m.ticker_nonstandard][["name","sec_name","ticker","n"]].drop_duplicates().head(100).to_string(index=False))
if __name__=="__main__":main()
