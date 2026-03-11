# topic_classifier_agent.py
#
# Reads unmapped topics from ai_topic_candidates,
# sends them to OpenAI for classification,
# and writes AI suggestions into canonical_issue_suggestions_ai.
#
# Before running:
#   1. Put OPENAI_API_KEY in .env
#   2. Put DATABASE_URL in .env
#   3. Make sure the table canonical_issue_suggestions_ai exists
#
# Run:  
#   python topic_classifier_agent.py

import json
import os
import time
from typing import Any

import psycopg2
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")
BATCH_SIZE = int(os.getenv("TOPIC_BATCH_SIZE", "50"))
SLEEP_SECONDS = float(os.getenv("TOPIC_SLEEP_SECONDS", "0.3"))

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set in your environment or .env file.")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in your environment or .env file.")

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
You classify U.S. lobbying topics into a stable canonical taxonomy.

Return JSON only with exactly these keys:
{
  "suggested_canonical_issue": "...",
  "suggested_issue_family": "...",
  "suggested_issue_group": "...",
  "merge_with_existing_issue": null,
  "confidence_score": 0.000
}

Rules:
- suggested_canonical_issue must be short, lowercase, snake_case.
- Prefer reusing an existing canonical issue when there is a strong semantic match.
- If an existing canonical issue clearly fits, set both:
  - suggested_canonical_issue = that existing canonical issue
  - merge_with_existing_issue = that existing canonical issue
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
- merge_with_existing_issue should be null unless the topic clearly belongs
  to an already established canonical issue.
- confidence_score must be between 0 and 1.
- Do not include markdown.
- Do not include explanation text.
"""

USER_TEMPLATE = """
Classify this lobbying topic.

normalized_topic_text: {topic}
total_activity: {activity}
domains_involved: {domains}

Existing canonical issues:
{existing_issues}

Make the canonical issue stable and reusable across future filings.
Prefer reusing one of the existing canonical issues whenever possible.
"""

SELECT_SQL = """
SELECT
    normalized_topic_text,
    total_activity,
    domains_involved
FROM ai_topic_candidates
WHERE normalized_topic_text NOT IN (
    SELECT normalized_topic_text
    FROM canonical_issue_suggestions_ai
)
ORDER BY total_activity DESC, domains_involved DESC, normalized_topic_text
LIMIT %s;
"""

UPSERT_SQL = """
INSERT INTO canonical_issue_suggestions_ai (
    normalized_topic_text,
    suggested_canonical_issue,
    suggested_issue_family,
    suggested_issue_group,
    merge_with_existing_issue,
    confidence_score,
    ai_model,
    approved
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (normalized_topic_text)
DO UPDATE SET
    suggested_canonical_issue = EXCLUDED.suggested_canonical_issue,
    suggested_issue_family = EXCLUDED.suggested_issue_family,
    suggested_issue_group = EXCLUDED.suggested_issue_group,
    merge_with_existing_issue = EXCLUDED.merge_with_existing_issue,
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

def fetch_topics(conn, batch_size: int) -> list[tuple[str, int, int]]:
    with conn.cursor() as cur:
        cur.execute(SELECT_SQL, (batch_size,))
        return cur.fetchall()


def classify_topic(topic: str, activity: int, domains: int, existing_issues: list[str]) -> dict[str, Any]:
    existing_issues_text = ", ".join(existing_issues) if existing_issues else "None yet"

    user_prompt = USER_TEMPLATE.format(
        topic=topic,
        activity=activity,
        domains=domains,
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
        "suggested_canonical_issue": data.get("suggested_canonical_issue"),
        "suggested_issue_family": data.get("suggested_issue_family"),
        "suggested_issue_group": data.get("suggested_issue_group"),
        "merge_with_existing_issue": data.get("merge_with_existing_issue"),
        "confidence_score": float(data.get("confidence_score", 0.0)),
    }


def save_classification(conn, topic: str, result: dict[str, Any]) -> None:
    approved = result["confidence_score"] >= 0.90
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            (
                topic,
                result["suggested_canonical_issue"],
                result["suggested_issue_family"],
                result["suggested_issue_group"],
                result["merge_with_existing_issue"],
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
        topics = fetch_topics(conn, BATCH_SIZE)
        existing_issues = fetch_existing_canonical_issues(conn)

        if not topics:
            print("No unmapped topics found.")
            return

        print(f"Found {len(topics)} topics to classify.")

        for i, row in enumerate(topics, start=1):
            topic, activity, domains = row
            print(f"[{i}/{len(topics)}] Classifying: {topic[:120]}")

            try:
                result = classify_topic(topic, activity, domains, existing_issues)
                save_classification(conn, topic, result)
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