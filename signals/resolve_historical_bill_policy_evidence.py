#!/usr/bin/env python3
"""Outcome-blind historical bill-reference resolver using a frozen Congress manifest.

No network calls. Bill identity is accepted only when the existing deterministic
mention resolver returns candidate/verified with confidence >= 0.85. The frozen
manifest supplies official titles captured before historical outcome testing.
"""
import argparse,csv,os,sys
from collections import Counter
import psycopg2
from dotenv import load_dotenv

HERE=os.path.dirname(os.path.abspath(__file__))
ROOT=os.path.abspath(os.path.join(HERE,".."))
sys.path.insert(0,os.path.join(ROOT,"sources","congress"))
from resolve_bills import extract,mention_resolution

def load_manifest(path,congress):
    out={}
    with open(path,newline="",encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if int(r["congress"])!=congress: continue
            key=(r["bill_type"].upper(),int(r["bill_number"]))
            out.setdefault(key,{"title":r["bill_title"],"policies":[]})["policies"].append(r["policy_code"])
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--year",type=int,required=True)
    ap.add_argument("--congress",type=int,required=True)