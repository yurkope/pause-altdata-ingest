"""Resolve frequent LDA bill mentions using official Congress.gov API.

Requires DATABASE_URL and CONGRESS_API_KEY in local .env.
No writes to production unless --write is explicitly supplied; schema must
already have been reviewed and applied. Default mode previews resolutions.
"""
import argparse
import os
import re
import time
from collections import Counter

import psycopg2
import requests
from dotenv import load_dotenv

BASE = "https://api.congress.gov/v3"
PATTERN = re.compile(r"(?<![A-Za-z0-9])(H\.?\s*R\.?|S\.?)\s*[-#:]?\s*(\d{1,6})(?!\d)", re.I)


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--quarter", default="Q4")
    parser.add_argument("--congress", type=int, default=119)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--confirm-congress-context", action="store_true",
                        help="Required for writes: manually confirm bill mentions refer to selected Congress")
    args = parser.parse_args()
    if args.write and not args.confirm_congress_context:
        raise SystemExit("Refusing writes: bill number alone cannot resolve Congress. Confirm context first.")
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
            evidence.setdefault((typ, number), []).append((activity_id, matched))
    session = requests.Session()
    try:
        for (typ, number), count in counts.most_common(args.top):
            try:
                bill = api(session, key, f"/bill/{args.congress}/{typ.lower()}/{number}").get("bill", {})
                if not bill or int(bill.get("number", 0)) != number:
                    print(f"SKIP {typ} {number}: bill not verified")
                    continue
                title = bill.get("title", "")
                committees = api(session, key, f"/bill/{args.congress}/{typ.lower()}/{number}/committees").get("committees", [])
                names = [c.get("name", "?") for c in committees]
                print(f"{typ} {number} | {count} activities | {title} | committees={names}")
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
                         __import__("json").dumps(bill)),
                    )
                    for activity_id, matched in evidence[(typ, number)]:
                        cur.execute(
                            """INSERT INTO policy.bill_mention
                               (activity_id,congress,bill_type,bill_number,evidence,resolution_status)
                               VALUES (%s,%s,%s,%s,%s,'verified')
                               ON CONFLICT(activity_id,congress,bill_type,bill_number)
                               DO UPDATE SET evidence=EXCLUDED.evidence,
                               resolution_status=EXCLUDED.resolution_status""",
                            (activity_id,args.congress,typ,number,matched),
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
                             __import__("json").dumps(c)),
                        )
                        cur.execute(
                            """INSERT INTO policy.bill_committee
                               (congress,bill_type,bill_number,committee_code,activity_json)
                               VALUES (%s,%s,%s,%s,%s::jsonb)
                               ON CONFLICT(congress,bill_type,bill_number,committee_code)
                               DO UPDATE SET activity_json=EXCLUDED.activity_json""",
                            (args.congress,typ,number,code,
                             __import__("json").dumps(c.get("activities",[]))),
                        )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(f"ERROR {typ} {number}: {type(exc).__name__}: {exc}")
                if args.write:
                    raise
    finally:
        session.close()
        conn.close()


if __name__ == "__main__":
    main()
