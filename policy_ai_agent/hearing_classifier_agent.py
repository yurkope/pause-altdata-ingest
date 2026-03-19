# hearing_classifier_agent.py
#
# Reads uncategorized hearings from hearing_event,
# sends them to OpenAI for classification,
# and writes AI suggestions into hearing_issue_suggestions_ai.
#
# Before running:
#   1. Put OPENAI_API_KEY in .env
#   2. Put DATABASE_URL in .env
#   3. Make sure the table hearing_issue_suggestions_ai exists
#
# Run:
#   python hearing_classifier_agent.py

import json
import os
import time
from typing import Any, Optional

import psycopg2
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")
BATCH_SIZE = int(os.getenv("HEARING_BATCH_SIZE", "25"))
SLEEP_SECONDS = float(os.getenv("HEARING_SLEEP_SECONDS", "0.3"))

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set in your environment or .env file.")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in your environment or .env file.")

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
You classify U.S. congressional hearings into a stable canonical policy taxonomy.

Return JSON only with exactly these keys:
{
  "normalized_topic_text": "...",
  "suggested_canonical_issue": "...",
  "suggested_issue_family": "...",
  "suggested_issue_group": "...",
  "confidence_score": 0.000
}

Rules:
- normalized_topic_text should be short, reusable, and lowercased plain text.
- suggested_canonical_issue must be short, lowercase, snake_case.
- Prefer reusing an existing canonical issue when there is a strong semantic match.
- Only create a new canonical issue when no existing canonical issue is a good fit.
- suggested_issue_family should be one of:
  budget, defense, trade, tax, healthcare, education, technology,
  infrastructure, agriculture, finance, energy, environment, housing,
  labor, legal, media, sports, manufacturing, other
- suggested_issue_group should be one of:
  appropriations, authorization, regulation, reimbursement, tax_policy,
  trade_policy, education_policy, infrastructure_policy, healthcare_policy,
  technology_policy, agriculture_policy, finance_policy, energy_policy,
  environmental_policy, public_safety, grants_funding, monitoring, other
- confidence_score must be between 0 and 1.
- Do not include markdown.
- Do not include explanation text.
"""

USER_TEMPLATE = """
Classify this congressional hearing into the canonical policy taxonomy.

committee_name_raw: {committee}
title: {title}
description: {description}

Existing canonical issues:
{existing_issues}

Prefer reusing an existing canonical issue whenever possible.
"""

SELECT_SQL = """
SELECT
    h.hearing_id,
    h.committee_name_raw,
    h.title,
    h.description
FROM hearing_event h
WHERE h.hearing_id NOT IN (
    SELECT hearing_id
    FROM hearing_issue_suggestions_ai
)
ORDER BY h.hearing_timestamp DESC, h.hearing_id DESC
LIMIT %s;
"""

UPSERT_SQL = """
INSERT INTO hearing_issue_suggestions_ai (
    hearing_id,
    normalized_topic_text,
    suggested_canonical_issue,
    suggested_issue_family,
    suggested_issue_group,
    confidence_score,
    ai_model,
    approved
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (hearing_id)
DO UPDATE SET
    normalized_topic_text = EXCLUDED.normalized_topic_text,
    suggested_canonical_issue = EXCLUDED.suggested_canonical_issue,
    suggested_issue_family = EXCLUDED.suggested_issue_family,
    suggested_issue_group = EXCLUDED.suggested_issue_group,
    confidence_score = EXCLUDED.confidence_score,
    ai_model = EXCLUDED.ai_model,
    approved = EXCLUDED.approved,
    ai_created_at = CURRENT_TIMESTAMP;
"""

EXISTING_ISSUES_SQL = """
SELECT DISTINCT canonical_issue
FROM canonical_issue_map
WHERE canonical_issue IS NOT NULL
ORDER BY canonical_issue;
"""


def fetch_existing_canonical_issues(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(EXISTING_ISSUES_SQL)
        return [row[0] for row in cur.fetchall() if row[0]]


def fetch_hearings(conn, batch_size: int) -> list[tuple[int, str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(SELECT_SQL, (batch_size,))
        return cur.fetchall()


def classify_hearing(
    committee_name_raw: Optional[str],
    title: Optional[str],
    description: Optional[str],
    existing_issues: list[str],
) -> dict[str, Any]:
    existing_issues_text = ", ".join(existing_issues) if existing_issues else "None yet"

    user_prompt = USER_TEMPLATE.format(
        committee=committee_name_raw or "",
        title=title or "",
        description=description or "",
        existing_issues=existing_issues_text,
    )

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    raw = response.output_text.strip()
    data = json.loads(raw)

    return {
        "normalized_topic_text": data.get("normalized_topic_text"),
        "suggested_canonical_issue": data.get("suggested_canonical_issue"),
        "suggested_issue_family": data.get("suggested_issue_family"),
        "suggested_issue_group": data.get("suggested_issue_group"),
        "confidence_score": float(data.get("confidence_score", 0.0)),
    }


def save_classification(conn, hearing_id: int, result: dict[str, Any]) -> None:
    approved = result["confidence_score"] >= 0.90

    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            (
                hearing_id,
                result["normalized_topic_text"],
                result["suggested_canonical_issue"],
                result["suggested_issue_family"],
                result["suggested_issue_group"],
                result["confidence_score"],
                OPENAI_MODEL,
                approved,
            ),
        )
    conn.commit()


def main() -> None:
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)

    try:
        hearings = fetch_hearings(conn, BATCH_SIZE)
        existing_issues = fetch_existing_canonical_issues(conn)

        if not hearings:
            print("No uncategorized hearings found.")
            return

        print(f"Found {len(hearings)} hearings to classify.")

        for i, row in enumerate(hearings, start=1):
            hearing_id, committee_name_raw, title, description = row
            title_preview = (title or "")[:120]
            print(f"[{i}/{len(hearings)}] Classifying hearing {hearing_id}: {title_preview}")

            try:
                result = classify_hearing(
                    committee_name_raw=committee_name_raw,
                    title=title,
                    description=description,
                    existing_issues=existing_issues,
                )
                save_classification(conn, hearing_id, result)
                print(
                    f"  -> {result['suggested_canonical_issue']} | "
                    f"{result['suggested_issue_family']} | "
                    f"{result['suggested_issue_group']} | "
                    f"{result['confidence_score']:.3f}"
                )
            except Exception as e:
                conn.rollback()
                print(f"  ERROR: {e}")

            time.sleep(SLEEP_SECONDS)

        print("Done.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()