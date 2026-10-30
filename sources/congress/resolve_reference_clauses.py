#!/usr/bin/env python3
"""Clause-level resolver for LDA legislative references.

Read-only. Segments activity descriptions into clauses, then resolves section
references to the nearest bill or explicit statute/code context. It preserves
ambiguity and never treats co-occurrence as proof of lobbying contact.
"""
import argparse,csv,os,re
import psycopg2
from dotenv import load_dotenv
from reference_normalization import SECTION_TOKEN_RE

BILL_RE=re.compile(r"(?i)\b(?P<type>H\.?R\.?|S\.?)\s*(?P<number>\d+)\b")
SECTION_RE=re.compile(r"(?i)\b(?:section|sec\.?)\s*(?P<section>[0-9]+(?:\.[0-9]+)*(?:[A-Za-z]+)?(?:\([A-Za-z0-9]+\))*)")
STATUTE_RE=re.compile(
 r"(?i)\b(clean air act|clean water act|national environmental policy act|nepa|"
 r"endangered species act|internal revenue code|irc|dodd[- ]frank(?: act)?|"
 r"energy independence and security act|eisa|united states code|u\.?s\.?c\.?|"
 r"public law|p\.?l\.?|section 301 tariff|section 232)\b")
CLAUSE_SPLIT_RE=re.compile(r"(?<=[.;])\s+|\s+(?:and|as well as)\s+(?=(?:H\.?R\.?|S\.?)\s*\d+\b)|(?<=:)\s+(?=(?:H\.?R\.?|S\.?)\s*\d+\b)",re.I)

# Contextual authority aliases require both a section identifier and contextual
# language. Section numbers alone are never sufficient to identify an authority.
AUTHORITY_RULES=[
 ("TRADE_ACT_301", {"301"}, re.compile(r"(?i)\b(USTR|trade representative|tariff|trade act|section 301 investigation)\b")),
 ("CDA_230", {"230"}, re.compile(r"(?i)\b(content moderation|communications decency|online platform|interactive computer service|social media)\b")),
 ("SMALL_BUSINESS_DATA_1071", {"1071"}, re.compile(r"(?i)\b(CFPB|small business data|data reporting|business lending|ECOA|equal credit opportunity)\b")),
 ("IRC_174", {"174"}, re.compile(r"(?i)\b(R&D|research|experimental|expensing|deductibility|tax)\b")),
 ("IRC_179D", {"179d"}, re.compile(r"(?i)\b(energy efficient|commercial building|building deduction|179D|tax deduction)\b")),
 ("IRC_48", {"48"}, re.compile(r"(?i)\b(investment tax credit|ITC|energy credit|solar|storage|tax credit)\b")),
 ("IRC_45Y", {"45y"}, re.compile(r"(?i)\b(clean electricity|production tax credit|PTC|tax credit|treasury|IRS)\b")),
 ("PHSA_340B", {"340b"}, re.compile(r"(?i)\b(340B|drug pricing|covered entit|HRSA|pharmaceutical)\w*\b")),
 ("ACA_1557", {"1557"}, re.compile(r"(?i)\b(affordable care act|ACA|nondiscrimination|health program|HHS)\b")),
 ("ACA_2706", {"2706"}, re.compile(r"(?i)\b(affordable care act|ACA|provider nondiscrimination|health care provider)\b")),
 ("FLSA_14C", {"14(c)"}, re.compile(r"(?i)\b(FLSA|fair labor standards|subminimum wage|disabilit)\w*\b")),
 ("FISA_702", {"702"}, re.compile(r"(?i)\b(FISA|foreign intelligence|surveillance|NSA|section 702)\b")),
 ("CUSTOMS_DE_MINIMIS_321", {"321"}, re.compile(r"(?i)\b(de minimis|customs|CBP|imports?|shipments?)\b")),

 ("IRC_179", {"179"}, re.compile(r"(?i)\b(expensing|deduction|tax cuts|tax code|business equipment)\b")),
 ("IRC_199A", {"199a"}, re.compile(r"(?i)\b(pass[- ]through|qualified business|main street tax|deduction|tax)\b")),
 ("IRC_30D", {"30d"}, re.compile(r"(?i)\b(clean vehicle|vehicle credit|treasury|IRS)\b")),
 ("IRC_45V", {"45v"}, re.compile(r"(?i)\b(hydrogen|clean hydrogen|tax credit|treasury|IRS)\b")),
 ("IRC_45Z", {"45z"}, re.compile(r"(?i)\b(clean fuel|fuel production|tax credit|treasury|IRS)\b")),
 ("IRC_163J", {"163(j)"}, re.compile(r"(?i)\b(interest|business interest|deduction|tax|IRC|code)\b")),
 ("IRC_401H", {"401(h)"}, re.compile(r"(?i)\b(VEBA|retiree|medical|pension|surplus|tax)\b")),
 ("IRC_3504", {"3504"}, re.compile(r"(?i)\b(PEO|employment tax|payroll|IRS|code)\b")),
 ("TARIFF_ACT_337", {"337"}, re.compile(r"(?i)\b(ITC|international trade commission|importation|patent|infring)\w*\b")),
 ("TSCA_5", {"5"}, re.compile(r"(?i)\b(TSCA|toxic substances|significant new use|SNUN)\b")),
 ("TSCA_8E", {"8e","8(e)"}, re.compile(r"(?i)\b(TSCA|PFAS|toxic substances|reporting requirements)\b")),
 ("WAR_POWERS_5C", {"5(c)"}, re.compile(r"(?i)\b(war powers|armed forces|hostilities)\b")),
 ("HOUSING_CHOICE_VOUCHER_8", {"8"}, re.compile(r"(?i)\b(housing choice voucher|HCV|HUD|section 8 housing)\b")),
 ("PAMA_216", {"216"}, re.compile(r"(?i)\b(PAMA|protecting access to medicare|clinical laboratory|laboratory services)\b")),
 ("HRSA_PCTE_747", {"747"}, re.compile(r"(?i)\b(primary care training|PCTE|HRSA|health resources and services administration)\b")),
 ("OBBBA_899", {"899"}, re.compile(r"(?i)\b(one big beautiful|OBBBA|119-21|tax code|REIT)\b")),
]

def contextual_authority(section,text):
    s=(section or "").lower().rstrip(".")
    hits=[name for name,sections,rx in AUTHORITY_RULES if s in sections and rx.search(text or "")]
    return hits[0] if len(hits)==1 else None

def norm_bill_type(x):
    x=re.sub(r"[^A-Z]","",x.upper())
    return "HR" if x=="HR" else x

def split_clauses(text):
    parts=[]; start=0
    for m in CLAUSE_SPLIT_RE.finditer(text or ""):
        end=m.start()
        if end>start and (text[start:end].strip()):
            parts.append((start,end,text[start:end].strip()))
        start=m.end()
    if text and text[start:].strip(): parts.append((start,len(text),text[start:].strip()))
    return parts

def resolve_clause(activity_id,clause_index,start,end,text,inherited_bill=None):
    bills=[]
    for m in BILL_RE.finditer(text):
        bills.append({"bill_type":norm_bill_type(m.group("type")),
                      "bill_number":int(m.group("number")),
                      "start":m.start(),"end":m.end(),"raw":m.group(0)})
    statutes=[{"name":m.group(0),"start":m.start(),"end":m.end()} for m in STATUTE_RE.finditer(text)]
    sections=list(SECTION_RE.finditer(text))
    rows=[]
    for sm in sections:
        sec=sm.group("section").rstrip(".").lower(); sp=sm.start()
        bill_rank=sorted([(min(abs(sp-b["start"]),abs(sp-b["end"])),b) for b in bills],key=lambda x:x[0])
        statute_rank=sorted([(min(abs(sp-s["start"]),abs(sp-s["end"])),s) for s in statutes],key=lambda x:x[0])
        nearest_bill=bill_rank[0] if bill_rank else None
        nearest_statute=statute_rank[0] if statute_rank else None
        owner_type="UNRESOLVED"; owner=None; distance=None; confidence="LOW"; ambiguous=False; ownership_basis=None
        candidates=[]
        if nearest_bill: candidates.append(("BILL",nearest_bill[0]))
        if nearest_statute: candidates.append(("STATUTE_OR_CODE",nearest_statute[0]))
        candidates.sort(key=lambda x:x[1])
        if len(candidates)>1 and abs(candidates[0][1]-candidates[1][1])<=20:
            ambiguous=True
        # A recognized section+context authority is stronger evidence than
        # mere proximity to a bill token. Resolve it before nearest-bill logic.
        authority=contextual_authority(sec,text)
        if authority:
            owner_type="STATUTE_OR_CODE"; owner=authority; confidence="MEDIUM"; ownership_basis="CONTEXTUAL_AUTHORITY"
        elif ambiguous:
            owner_type="AMBIGUOUS"; owner=None; distance=candidates[0][1]; confidence="LOW"
        elif nearest_statute and (not nearest_bill or nearest_statute[0] < nearest_bill[0]):
            owner_type="STATUTE_OR_CODE"; owner=nearest_statute[1]["name"]; distance=nearest_statute[0]; confidence="MEDIUM"; ownership_basis="DIRECT_STATUTE"
        elif nearest_bill:
            owner_type="BILL"; owner=f'{nearest_bill[1]["bill_type"]} {nearest_bill[1]["bill_number"]}'; distance=nearest_bill[0]
            confidence="HIGH" if distance<=60 else ("MEDIUM" if distance<=140 else "LOW"); ownership_basis="DIRECT_BILL"
        # Prior validation showed inherited bill ownership is too noisy to
        # support provision attribution. Preserve the inherited bill only as a
        # candidate/context field; do not promote it to BILL ownership.
        if owner_type=="UNRESOLVED" and inherited_bill:
            owner=inherited_bill; confidence="LOW"; ownership_basis="INHERITED_BILL_CANDIDATE"
        rows.append([activity_id,clause_index,start,end,sm.group(0),sec,owner_type,owner,distance,confidence,ambiguous,ownership_basis,text])
    return rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--congress",type=int,default=119)
    ap.add_argument("--output",default="bill_clause_resolution.csv")
    ap.add_argument("--activity-id",type=int)
    args=ap.parse_args(); load_dotenv()
    db=os.environ["DATABASE_URL"]
    q="""SELECT DISTINCT a.activity_id,a.description
         FROM lda.activity a
         JOIN policy.bill_mention bm ON bm.activity_id=a.activity_id
         WHERE bm.congress=%s AND bm.resolution_confidence>=.85
           AND a.description ~* '(section|sec\\.?)[[:space:]]*[0-9]+'
           AND (%s IS NULL OR a.activity_id=%s)
         ORDER BY a.activity_id"""
    with psycopg2.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(q,(args.congress,args.activity_id,args.activity_id)); activities=cur.fetchall()
    out=[]
    for aid,description in activities:
        inherited_bill=None
        for i,(start,end,clause) in enumerate(split_clauses(description),1):
            clause_bills=list(BILL_RE.finditer(clause))
            clause_statutes=list(STATUTE_RE.finditer(clause))
            # A single explicit bill establishes carry-forward context. Multiple
            # bills or an explicit statute reset inheritance to avoid smearing.
            if len(clause_bills)==1 and not clause_statutes:
                m=clause_bills[0]
                inherited_bill=f"{norm_bill_type(m.group('type'))} {int(m.group('number'))}"
            elif len(clause_bills)>1 or clause_statutes:
                inherited_bill=None
            out.extend(resolve_clause(aid,i,start,end,clause,inherited_bill))
    with open(args.output,"w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh)
        w.writerow(["activity_id","clause_index","clause_start","clause_end","section_ref",
                    "normalized_section","owner_type","owner","owner_distance",
                    "resolution_confidence","ambiguous_owner","ownership_basis","clause_text"])
        w.writerows(out)
    counts={}; bases={}
    for r in out:
        counts[r[6]]=counts.get(r[6],0)+1
        bases[r[11]]=bases.get(r[11],0)+1
    print({"output":args.output,"activities":len(activities),"section_observations":len(out)})
    print("OWNER_TYPES")
    for k,v in sorted(counts.items(),key=lambda x:-x[1]):
        print({"owner_type":k,"count":v,"pct":round(100*v/len(out),1) if out else 0})
    unresolved=[r for r in out if r[6]=="UNRESOLVED"]
    section_counts={}
    for r in unresolved: section_counts[r[5]]=section_counts.get(r[5],0)+1
    print("TOP_UNRESOLVED_SECTIONS")
    for sec,n in sorted(section_counts.items(),key=lambda x:(-x[1],str(x[0])))[:40]:
        print({"section":sec,"count":n})
    print("UNRESOLVED_CONTEXT_SAMPLES")
    shown={}
    for r in unresolved:
        sec=r[5]
        if shown.get(sec,0)>=2: continue
        if section_counts.get(sec,0)<3: continue
        print({"section":sec,"clause":r[12][:500]})
        shown[sec]=shown.get(sec,0)+1
        if sum(shown.values())>=30: break
    print("OWNERSHIP_BASIS")
    for k,v in sorted(bases.items(),key=lambda x:-x[1]):
        print({"basis":k or "NONE","count":v,"pct":round(100*v/len(out),1) if out else 0})

if __name__=="__main__": main()
