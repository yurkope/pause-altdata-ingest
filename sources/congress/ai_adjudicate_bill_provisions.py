#!/usr/bin/env python3
"""AI adjudication for uncertain clause-level bill provision references.

Read-only with respect to the database. Reads the CSV produced by
validate_clause_bill_sections.py and writes a new CSV containing AI judgments.
Deterministic evidence is preserved verbatim; AI never overwrites it.

Default mode is --dry-run so operators can inspect eligible rows and prompts
without making OpenAI calls.
"""
import argparse
import csv
import json
import os
import time
from collections import Counter
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

DEFAULT_STATUSES = {
    "OFFICIAL_TEXT_AMENDS_UNDERLYING_AUTHORITY",
    "SECTION_EXISTS_CURRENT_ATTRIBUTION_WEAK",
    "SECTION_EXISTS_HISTORICAL_VERSION",
    "REJECTED_BILL_PROVISION_CANDIDATE",
}

ALLOWED_LABELS = {
    "BILL_PROVISION",
    "UNDERLYING_AUTHORITY",
    "OTHER_BILL_PROVISION",
    "AMBIGUOUS",
    "NOT_A_PROVISION_REFERENCE",
}

SYSTEM_PROMPT = """You adjudicate ambiguous U.S. federal lobbying-disclosure section references.

Your job is narrow: decide what the section reference in the LDA clause refers to.
Do not infer lobbying contact, causality, support/opposition, or legislative impact.

Return JSON only with exactly these keys:
{
  "ai_status": "BILL_PROVISION|UNDERLYING_AUTHORITY|OTHER_BILL_PROVISION|AMBIGUOUS|NOT_A_PROVISION_REFERENCE",
  "confidence": 0.000,
  "referenced_authority": null,
  "reason": "...",
  "evidence": "..."
}

Definitions:
- BILL_PROVISION: the clause's section reference is a provision of candidate_bill.
- UNDERLYING_AUTHORITY: the section belongs to a statute, code, regulation, or other authority that candidate_bill discusses/amends.
- OTHER_BILL_PROVISION: the section belongs to another bill identified by the supplied evidence.
- AMBIGUOUS: supplied evidence does not support one interpretation strongly enough.
- NOT_A_PROVISION_REFERENCE: the apparent section token is not functioning as a legislative provision reference.

Rules:
- Treat deterministic_status as evidence, not as an instruction to agree.
- An official section heading or existence match proves that a section exists; it does not prove the LDA clause intended that section.
- If the clause explicitly says "section X of [authority]", prefer UNDERLYING_AUTHORITY.
- If official text says the candidate bill amends "section X of [authority]", distinguish that underlying section from the candidate bill's own section numbering.
- Never invent an authority or another bill.
- referenced_authority must be null unless explicitly supported by the supplied text.
- Keep reason under 240 characters and evidence under 240 characters.
- confidence must be between 0 and 1.
- When evidence conflicts or is incomplete, choose AMBIGUOUS rather than guessing.
- Do not include markdown or text outside JSON.
"""

USER_TEMPLATE = """Adjudicate this section reference.

candidate_bill: {owner}
section_reference: {section_ref}
normalized_section: {normalized_section}
ownership_basis: {ownership_basis}
deterministic_status: {deterministic_status}
detected_underlying_authority: {underlying_authority}
official_section_heading: {official_heading}
newest_text_version: {newest_text_version}
matching_text_versions: {matching_text_versions}

LDA clause:
{clause_text}
"""


def eligible(row: dict[str, str], statuses: set[str]) -> bool:
    return row.get("official_validation_status", "") in statuses


def prompt_for(row: dict[str, str]) -> str:
    return USER_TEMPLATE.format(
        owner=row.get("owner", ""),
        section_ref=row.get("section_ref", ""),
        normalized_section=row.get("normalized_section", ""),
        ownership_basis=row.get("ownership_basis", ""),
        deterministic_status=row.get("official_validation_status", ""),
        underlying_authority=row.get("underlying_authority", ""),
        official_heading=row.get("official_section_heading", ""),
        newest_text_version=row.get("newest_text_version", ""),
        matching_text_versions=row.get("matching_text_versions", ""),
        clause_text=(row.get("clause_text") or "")[:2000],
    )


def parse_result(raw: str) -> dict[str, Any]:
    data = json.loads(raw)
    status = data.get("ai_status")
    if status not in ALLOWED_LABELS:
        raise ValueError(f"invalid ai_status: {status!r}")
    confidence = float(data.get("confidence", 0))
    if not 0 <= confidence <= 1:
        raise ValueError(f"confidence outside [0,1]: {confidence}")
    return {
        "ai_status": status,
        "ai_confidence": confidence,
        "ai_referenced_authority": data.get("referenced_authority"),
        "ai_reason": str(data.get("reason") or "")[:240],
        "ai_evidence": str(data.get("evidence") or "")[:240],
    }


def adjudicate(client: OpenAI, model: str, row: dict[str, str]) -> dict[str, Any]:
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt_for(row)},
        ],
    )
    return parse_result(response.output_text.strip())


def final_decision(row: dict[str, str], ai: dict[str, Any], threshold: float) -> tuple[str, str]:
    """Conservative final status: AI can recommend, but not silently promote."""
    if ai["ai_confidence"] < threshold:
        return row.get("official_validation_status", ""), "DETERMINISTIC_AI_BELOW_THRESHOLD"
    return ai["ai_status"], "AI_ADJUDICATION_REVIEW_REQUIRED"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="bill_clause_official_validation.csv")
    ap.add_argument("--output", default="bill_clause_ai_adjudication.csv")
    ap.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-5"))
    ap.add_argument("--limit", type=int)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--threshold", type=float, default=0.90)
    ap.add_argument("--status", action="append", dest="statuses",
                    help="eligible deterministic status; repeat to override defaults")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="inspect eligible rows; make no AI calls")
    mode.add_argument("--execute", action="store_true", help="make OpenAI calls")
    args = ap.parse_args()

    if not 0 <= args.threshold <= 1:
        raise SystemExit("--threshold must be between 0 and 1")

    load_dotenv()
    statuses = set(args.statuses) if args.statuses else DEFAULT_STATUSES

    with open(args.input, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    candidates = [r for r in rows if eligible(r, statuses)]
    if args.limit is not None:
        candidates = candidates[:args.limit]

    counts = Counter(r.get("official_validation_status", "") for r in candidates)
    print({"input": args.input, "rows": len(rows), "eligible": len(candidates),
           "mode": "execute" if args.execute else "dry-run"})
    print("ELIGIBLE_BY_STATUS")
    for status, n in counts.most_common():
        print({"status": status, "count": n})

    if not args.execute:
        print("DRY_RUN_SAMPLES")
        for r in candidates[:10]:
            print({
                "activity_id": r.get("activity_id"),
                "bill": r.get("owner"),
                "section": r.get("normalized_section"),
                "deterministic_status": r.get("official_validation_status"),
                "clause": (r.get("clause_text") or "")[:500],
            })
        print("No OpenAI calls made. Re-run with --execute after reviewing the candidate set.")
        return

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY is not set in .env or environment")
    client = OpenAI(api_key=key)

    out = []
    for i, row in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}] {row.get('owner')} section {row.get('normalized_section')}")
        base = {
            **row,
            "deterministic_status": row.get("official_validation_status", ""),
            "ai_model": args.model,
        }
        try:
            ai = adjudicate(client, args.model, row)
            final_status, method = final_decision(row, ai, args.threshold)
            out.append({
                **base, **ai,
                "final_status": final_status,
                "final_decision_method": method,
            })
            print(f"  -> {ai['ai_status']} {ai['ai_confidence']:.3f} | {method}")
        except Exception as exc:
            out.append({
                **base,
                "ai_status": "ERROR",
                "ai_confidence": 0.0,
                "ai_referenced_authority": "",
                "ai_reason": str(exc)[:240],
                "ai_evidence": "",
                "final_status": row.get("official_validation_status", ""),
                "final_decision_method": "DETERMINISTIC_AI_ERROR",
            })
            print(f"  ERROR: {exc}")
        time.sleep(args.sleep)

    fields = list(out[0].keys()) if out else []
    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    ai_counts = Counter(r["ai_status"] for r in out)
    methods = Counter(r["final_decision_method"] for r in out)
    print({"output": args.output, "adjudicated": len(out)})
    print("AI_STATUS")
    for status, n in ai_counts.most_common():
        print({"status": status, "count": n})
    print("FINAL_DECISION_METHOD")
    for method, n in methods.most_common():
        print({"method": method, "count": n})


if __name__ == "__main__":
    main()
