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
CONGRESS_PATTERN = re.compile(r"\b(\d{3})(?:st|nd|rd|th)?\s+Congress\b", re.I)

def mention_window(description, start, end, radius=220):
    """Compact auditable context around the exact bill token."""
    text = description or ""
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    snippet = re.sub(r"\s+", " ", text[left:right]).strip()
    return ("…" if left else "") + snippet + ("…" if right < len(text) else "")


def normalized_words(text):
    return re.findall(r"[a-z0-9]+", (text or "").casefold())


def title_evidence(window, title):
    """Conservative lexical support; excludes generic stopwords and year alone."""
    stop = {"the","and","of","for","to","in","a","an","act","bill","resolution","2025","2026"}
    title_words = [w for w in normalized_words(title) if w not in stop and len(w) >= 4]
    window_words = set(normalized_words(window))
    if not title_words:
        return 0.0
    return sum(w in window_words for w in set(title_words)) / len(set(title_words))


PUBLIC_LAW_PATTERN = re.compile(r"\bP(?:ublic|ub)?\.?\s*L(?:aw)?\.?\s*(\d{3})[-–](\d+)\b", re.I)
def linked_public_law_congresses(window, matched):
    """Link a public-law Congress only to the nearest bill token.

    LDA descriptions often list several bills and public laws in one evidence
    window. A PL marker is bill-specific only when the target bill is the
    nearest bill token within 90 characters and no semicolon separates them.
    """
    target = re.sub(r"[^A-Z0-9]", "", matched.upper())
    bills = list(PATTERN.finditer(window))
    found = set()
    for pl in PUBLIC_LAW_PATTERN.finditer(window):
        congress = int(pl.group(1))
        candidates = []
        for bill in bills:
            gap = max(pl.start() - bill.end(), bill.start() - pl.end(), 0)
            if gap > 90:
                continue
            between = window[min(pl.end(), bill.end()):max(pl.start(), bill.start())]
            if ";" in between:
                continue
            candidates.append((gap, bill))
        if not candidates:
            continue
        min_gap = min(gap for gap, _ in candidates)
        nearest = [bill for gap, bill in candidates if gap == min_gap]
        if len(nearest) != 1:
            continue
        key = re.sub(r"[^A-Z0-9]", "", nearest[0].group(0).upper())
        if key == target:
            found.add(congress)
    return found


def authoritative_law_congresses(source_json):
    """Congress numbers from Congress.gov's bill-specific laws relationship."""
    laws = (source_json or {}).get("laws") or []
    found = set()
    for law in laws:
        m = re.match(r"^(\\d{3})-", str(law.get("number") or ""))
        if m:
            found.add(int(m.group(1)))
    return found


def mention_resolution(description, start, end, selected_congress, title="", matched="", source_json=None):
    """Classify Congress context locally; title evidence may strengthen candidates."""
    window = mention_window(description, start, end)
    explicit = {int(m.group(1)) for m in CONGRESS_PATTERN.finditer(window)}
    # Explicit Congress prose is only safe when it is in the same local clause
    # as the target token. Long filing windows frequently mix historical and
    # current bills (e.g. "...118th Congress...; S. 2217 ...").
    # Locate the exact target occurrence by normalized chamber/number, rather
    # than window.find(matched): formatting can vary (H.R.1 vs H.R. 1).
    target_key = re.sub(r"[^A-Z0-9]", "", matched.upper()) if matched else ""
    target_match = next(
        (m for m in PATTERN.finditer(window)
         if re.sub(r"[^A-Z0-9]", "", m.group(0).upper()) == target_key),
        None,
    )
    if explicit and target_match:
        target_pos = target_match.start()
        # Periods inside H.R./S. abbreviations are not clause boundaries.
        # Semicolons are reliable separators in LDA issue descriptions; use a
        # bounded local slice when no semicolon is present.
        clause_start = window.rfind(";", 0, target_pos) + 1
        semi = window.find(";", target_match.end())
        clause_end = semi if semi >= 0 else min(len(window), target_match.end() + 140)
        clause = window[clause_start:clause_end]
        explicit = {int(m.group(1)) for m in CONGRESS_PATTERN.finditer(clause)}
    public_law_congresses = linked_public_law_congresses(window, matched) if matched else set()
    authoritative = authoritative_law_congresses(source_json)
    # Congress.gov's bill-specific laws relation is an independent guardrail:
    # if local text claims a PL for this bill but Congress.gov has a different
    # enacted-law Congress, do not promote the mention from text proximity.
    if public_law_congresses and authoritative and not public_law_congresses.issubset(authoritative):
        public_law_congresses = set()
    # A PL marker tied to this exact bill token is stronger than a broad
    # "119th Congress" phrase elsewhere in the ±220-char context.
    if public_law_congresses:
        if public_law_congresses == {selected_congress}:
            return "verified", "public_law_congress_local", 1.0, window
        if selected_congress not in public_law_congresses and len(public_law_congresses) == 1:
            return "rejected", "conflicting_public_law_local", 1.0, window
        return "ambiguous", "multiple_public_law_local", 0.5, window
    if explicit == {selected_congress}:
        return "verified", "explicit_congress_local", 1.0, window
    if explicit and selected_congress not in explicit:
        return "rejected", "conflicting_congress_local", 1.0, window
    if len(explicit) > 1:
        return "ambiguous", "multiple_congress_local", 0.5, window
    overlap = title_evidence(window, title)
    if overlap >= 0.80:
        return "candidate", "title_strong_local", 0.85, window
    if overlap >= 0.55:
        return "candidate", "title_moderate_local", 0.65, window
    return "candidate", "bill_token_only", 0.25, window


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
            yield key, match.group(0), match.start(), match.end()


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
    normalized = [re.sub(r"\s+", " ", name).casefold() for name in names]
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
    parser.add_argument("--rank-by", choices=["mentions", "dollars", "hybrid"], default="mentions",
                        help="Prioritize bill tokens by mention count, allocated filing dollars, or hybrid rank.")
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
            """SELECT a.activity_id,a.description,a.filing_uuid,
                      COALESCE(f.income,f.expenses,0)
               FROM lda.activity a JOIN lda.filing f USING(filing_uuid)
               WHERE f.filing_year=%s AND f.filing_type=%s
                 AND a.description IS NOT NULL""",
            (args.year, args.quarter),
        )
        rows = cur.fetchall()
    counts = Counter()
    evidence = {}
    filing_amount = {}
    filing_tokens = {}
    # Caution: count-level discovery does not prove every mention refers to
    # the selected Congress; bill numbers repeat every two years.
    for activity_id, description, filing_uuid, amount in rows:
        filing_amount[filing_uuid] = float(amount or 0)
        activity_tokens = []
        for (typ, number), matched, start, end in extract(description):
            counts[(typ, number)] += 1
            evidence.setdefault((typ, number), []).append(
                (activity_id, matched, description, start, end)
            )
            activity_tokens.append((typ, number))
        filing_tokens.setdefault(filing_uuid, set()).update(activity_tokens)

    dollar_score = Counter()
    for filing_uuid, tokens in filing_tokens.items():
        if tokens:
            share = filing_amount.get(filing_uuid, 0.0) / len(tokens)
            for token in tokens:
                dollar_score[token] += share

    if args.rank_by == "dollars":
        ranked = sorted(counts, key=lambda x: (dollar_score[x], counts[x]), reverse=True)
    elif args.rank_by == "hybrid":
        by_mentions = {k: i for i, (k, _) in enumerate(counts.most_common(), 1)}
        by_dollars = {k: i for i, k in enumerate(sorted(counts, key=lambda x: dollar_score[x], reverse=True), 1)}
        ranked = sorted(counts, key=lambda x: (by_mentions[x] + by_dollars[x], -counts[x]))
    else:
        ranked = [k for k, _ in counts.most_common()]
    session = requests.Session()
    resolved_bills = 0
    skipped_bills = 0
    mention_totals = Counter()
    try:
        for typ, number in ranked[:args.top]:
            count = counts[(typ, number)]
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
                statuses = Counter(mention_resolution(desc, start, end, args.congress, title, matched, bill)[0]
                                   for _, matched, desc, start, end in evidence[(typ, number)])
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
                    for activity_id, matched, description, start, end in evidence[(typ, number)]:
                        status, method, confidence, context = mention_resolution(
                            description, start, end, args.congress, title, matched, bill
                        )
                        cur.execute(
                            """INSERT INTO policy.bill_mention
                               (activity_id,congress,bill_type,bill_number,evidence,resolution_status,
                                evidence_context,resolution_method,resolution_confidence)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                               ON CONFLICT(activity_id,congress,bill_type,bill_number)
                               DO UPDATE SET evidence=EXCLUDED.evidence,
                               resolution_status=EXCLUDED.resolution_status,
                               evidence_context=EXCLUDED.evidence_context,
                               resolution_method=EXCLUDED.resolution_method,
                               resolution_confidence=EXCLUDED.resolution_confidence""",
                            (activity_id,args.congress,typ,number,matched,status,context,method,confidence),
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
