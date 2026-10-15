"""Resolve frequent LDA bill mentions using official Congress.gov API.

Requires DATABASE_URL and CONGRESS_API_KEY in local .env.
No writes to production unless --write is explicitly supplied; schema must
already have been reviewed and applied. Default mode previews resolutions.
"""
import argparse
import json
import os
import re
import time
from collections import Counter

import psycopg2
import requests
from dotenv import load_dotenv

BASE = "https://api.congress.gov/v3"
PATTERN = re.compile(r"(?<![A-Za-z0-9])(H\.?\s*R\.?|S\.?)\s*[-#:]?\s*(\d{1,6})(?!\d)", re.I)
CONGRESS_PATTERN = re.compile(r"\\b(\\d{3})(?:st|nd|rd|th)\\s+Congress\\b", re.I)


def mention_status(description, selected_congress):
    """Resolve only explicit Congress context; otherwise retain as a candidate."""
    explicit = {int(m.group(1)) for m in CONGRESS_PATTERN.finditer(description or "")}
    if selected_congress in explicit and len(explicit) == 1:
        return "verified"
    if explicit and selected_congress not in explicit:
        return "rejected"
    if len(explicit) > 1:
        return "ambiguous"
    return "candidate"



def api(session, key, path):
    for attempt in range(5):
        try:
            response = session.get(
                BASE + path, params={"api_key": key, "format": "json", "limit": 250},
                timeout=30,
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Congress.gov API retries exhausted")


def extract(description):
    seen = set()
    for match in PATTERN.finditer(description or ""):
        typ = re.sub(r"[^A-Z]", "", match.group(1).upper())
        key = (typ, int(match.group(2)))
        if key not in seen:
            seen.add(key)
            yield key, match.group(0)


def committee_activities(committee):
    """Congress.gov activities are dated events; retain their original payload."""
    raw = committee.get("activities") or []
    if isinstance(raw, dict):
        raw = raw.get("items", raw.get("item", []))
    if not isinstance(raw, list):
        return []
    return [event for event in raw if isinstance(event, dict)]


def committee_evidence(committee):
    events = committee_activities(committee)
    names = [str(event.get("name", "")).strip() for event in events]
    normalized = [re.sub(r"\\s+", " ", name).casefold() for name in names]
    referral = any(name in {"referred to", "re-referred to", "committed to",
                            "re-committed to"} for name in normalized)
    substantive = any(name in {"hearings by", "hearing by", "markup by",
                               "reported by", "reported original measure"}
                      for name in normalized)
    return {
        "committee_code": committee.get("systemCode"),
        "chamber": committee.get("chamber"),
        "events": events,
        "event_types": sorted(set(name for name in names if name)),
        "has_referral": referral,
        "has_substantive_action": substantive,
        "evidence_class": ("substantive_action" if substantive else
                           "referral_only" if referral else "other_or_unknown"),
        # Conservative pressure eligibility: substantive action is strongest.
        # A sole committee with no classified event remains usable as
        # jurisdiction evidence, but broad unclassified referrals are not.
        "pressure_eligible": substantive,
        "source": committee,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--quarter", default="Q4")
    parser.add_argument("--congress", type=int, default=119)
    parser.add_argument("--top", type=int, default=200)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    load_dotenv()
    db_url, key = os.getenv("DATABASE_URL"), os.getenv("CONGRESS_API_KEY")
    if not db_url or not key:
        raise SystemExit("Set DATABASE_URL and CONGRESS_API_KEY in .env")
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(
            """SELECT a.activity_id,a.description
               FROM lda.activity a JOIN lda.filing f USING(filing_uuid)
               WHERE f.filing_year=%s AND f.filing_type=%s
                 AND a.description IS NOT NULL""",
            (args.year, args.quarter),
        )
        rows = cur.fetchall()
    counts = Counter()
    evidence = {}
    # Caution: count-level discovery does not prove every mention refers to
    # the selected Congress; bill numbers repeat every two years.
    for activity_id, description in rows:
        for (typ, number), matched in extract(description):
            counts[(typ, number)] += 1
            evidence.setdefault((typ, number), []).append((activity_id, matched, description))
    session = requests.Session()
    resolved_bills = 0
    skipped_bills = 0
    mention_totals = Counter()
    try:
        for (typ, number), count in counts.most_common(args.top):
            try:
                bill = api(session, key, f"/bill/{args.congress}/{typ.lower()}/{number}").get("bill", {})
                if not bill or int(bill.get("number", 0)) != number:
                    print(f"SKIP {typ} {number}: bill not verified")
                    skipped_bills += 1
                    continue
                title = bill.get("title", "")
                committees = api(session, key, f"/bill/{args.congress}/{typ.lower()}/{number}/committees").get("committees", [])
                names = [c.get("name", "?") for c in committees]
                evidence_rows = [committee_evidence(c) for c in committees]
                substantive = [c.get("name") for c, e in zip(committees, evidence_rows)
                               if e["has_substantive_action"]]
                referral_only = [c.get("name") for c, e in zip(committees, evidence_rows)
                                 if e["evidence_class"] == "referral_only"]
                unknown = [c.get("name") for c, e in zip(committees, evidence_rows)
                           if e["evidence_class"] == "other_or_unknown"]
                print(f"{typ} {number} | {count} activities | {title} | committees={names}")
                print(f"  evidence: substantive={substantive}; referral_only={referral_only}; other={unknown}")
                if len(committees) == 1 and evidence_rows[0]["evidence_class"] == "other_or_unknown":
                    evidence_rows[0]["pressure_eligible"] = True
                    print("  NOTE: sole committee retained as jurisdiction evidence despite unclassified activity")
                eligible = [c.get("name") for c, e in zip(committees, evidence_rows)
                            if e["pressure_eligible"]]
                print(f"  pressure_eligible={eligible}")
                if len(committees) > 5:
                    print("  WARNING: broad multi-committee association; only substantive committees are pressure-eligible")
                if any(not e["committee_code"] for e in evidence_rows):
                    print("  WARNING: missing committee systemCode; cannot persist those relationships")
                statuses = Counter(mention_status(desc, args.congress)
                                   for _, _, desc in evidence[(typ, number)])
                resolved_bills += 1
                mention_totals.update(statuses)
                print(f"  mention_status={dict(statuses)}")
                if not args.write:
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO policy.bill(congress,bill_type,bill_number,title,congress_url,source_json)
                           VALUES (%s,%s,%s,%s,%s,%s::jsonb)
                           ON CONFLICT(congress,bill_type,bill_number)
                           DO UPDATE SET title=EXCLUDED.title, congress_url=EXCLUDED.congress_url,
                           source_json=EXCLUDED.source_json,verified_at=now()""",
                        (args.congress, typ, number, title, bill.get("url"),
                         json.dumps(bill)),
                    )
                    for activity_id, matched, description in evidence[(typ, number)]:
                        status = mention_status(description, args.congress)
                        cur.execute(
                            """INSERT INTO policy.bill_mention
                               (activity_id,congress,bill_type,bill_number,evidence,resolution_status)
                               VALUES (%s,%s,%s,%s,%s,%s)
                               ON CONFLICT(activity_id,congress,bill_type,bill_number)
                               DO UPDATE SET evidence=EXCLUDED.evidence,
                               resolution_status=EXCLUDED.resolution_status""",
                            (activity_id,args.congress,typ,number,matched,status),
                        )
                    for c in committees:
                        code = c.get("systemCode")
                        if not code:
                            continue
                        cur.execute(
                            """INSERT INTO policy.committee(committee_code,name,chamber,source_json)
                               VALUES (%s,%s,%s,%s::jsonb)
                               ON CONFLICT(committee_code) DO UPDATE SET
                               name=EXCLUDED.name,chamber=EXCLUDED.chamber,
                               source_json=EXCLUDED.source_json""",
                            (code,c.get("name",""),c.get("chamber"),
                             json.dumps(c)),
                        )
                        cur.execute(
                            """INSERT INTO policy.bill_committee
                               (congress,bill_type,bill_number,committee_code,activity_json)
                               VALUES (%s,%s,%s,%s,%s::jsonb)
                               ON CONFLICT(congress,bill_type,bill_number,committee_code)
                               DO UPDATE SET activity_json=EXCLUDED.activity_json""",
                            (args.congress,typ,number,code,
                             json.dumps(committee_evidence(c))),
                        )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(f"ERROR {typ} {number}: {type(exc).__name__}: {exc}")
                if args.write:
                    raise
    finally:
        print(f"SUMMARY resolved_bills={resolved_bills} skipped_bills={skipped_bills} "
              f"mention_status={dict(mention_totals)}")
        session.close()
        conn.close()


if __name__ == "__main__":
    main()
