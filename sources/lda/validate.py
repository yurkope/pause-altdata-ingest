#!/usr/bin/env python3
"""Read-only validation for the normalized LDA ingestion layer."""

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv


def parse_args():
    p = argparse.ArgumentParser(description="Validate LDA rows already loaded into Postgres.")
    p.add_argument("--year", type=int, help="Optional filing year filter.")
    p.add_argument("--filing-type", help="Optional filing type filter, e.g. Q4.")
    return p.parse_args()


def main() -> int:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("Missing required environment variable: DATABASE_URL", file=sys.stderr)
        return 2

    args = parse_args()
    where = []
    params = []
    if args.year is not None:
        where.append("filing_year = %s")
        params.append(args.year)
    if args.filing_type:
        where.append("filing_type = %s")
        params.append(args.filing_type)
    predicate = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    WITH selected_filings AS (
        SELECT filing_uuid, income, expenses
        FROM lda.filing
        {predicate}
    ),
    selected_activities AS (
        SELECT a.*
        FROM lda.activity a
        JOIN selected_filings f USING (filing_uuid)
    ),
    activity_counts AS (
        SELECT filing_uuid, COUNT(*) AS activity_count
        FROM selected_activities
        GROUP BY filing_uuid
    )
    SELECT
        (SELECT COUNT(*) FROM selected_filings) AS filings,
        (SELECT COUNT(*) FROM selected_activities) AS activities,
        (SELECT COUNT(DISTINCT general_issue_code) FROM selected_activities) AS issue_codes,
        (SELECT COUNT(*) FROM lda.activity_government_entity age
            JOIN selected_activities a ON a.activity_id = age.activity_id) AS government_links,
        (SELECT COUNT(*) FROM lda.activity_lobbyist al
            JOIN selected_activities a ON a.activity_id = al.activity_id) AS lobbyist_links,
        (SELECT COUNT(*) FROM (
            SELECT filing_uuid, activity_index
            FROM selected_activities
            GROUP BY filing_uuid, activity_index
            HAVING COUNT(*) > 1
        ) d) AS duplicate_activities,
        (SELECT COUNT(*) FROM selected_filings
            WHERE income IS NOT NULL AND expenses IS NOT NULL) AS both_amounts,
        (SELECT COUNT(*) FROM selected_filings
            WHERE income IS NULL AND expenses IS NULL) AS neither_amount,
        (SELECT COALESCE(SUM(income), 0) FROM selected_filings) AS income_total,
        (SELECT COALESCE(SUM(expenses), 0) FROM selected_filings) AS expense_total,
        (SELECT COUNT(*) FROM selected_filings f
            LEFT JOIN activity_counts a USING (filing_uuid)
            WHERE COALESCE(a.activity_count, 0) = 0) AS filings_without_activities;
    """

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        labels = [
            "Filings", "Activities", "Distinct issue codes", "Government links",
            "Lobbyist links", "Duplicate activities", "Both income+expenses",
            "Neither income nor expenses", "Income total", "Expense total",
            "Filings without activities",
        ]
        for label, value in zip(labels, row):
            print(f"{label}: {value}")
        return 1 if row[5] else 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
