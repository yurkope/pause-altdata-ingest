#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, Optional

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

DEFAULT_BASE_URL = "https://lda.gov/api/v1/filings/"


def parse_args():
    p = argparse.ArgumentParser(description="Ingest LDA filings into normalized Postgres tables.")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--filing-type", help="Optional LDA filing type such as Q1, Q2, Q3, Q4, RR, RA.")
    p.add_argument("--max-pages", type=int, help="Stop after N pages (useful for smoke tests).")
    p.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between pages.")
    return p.parse_args()


def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def execute_schema(conn) -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def api_get(session: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    delay = 1.0
    for attempt in range(6):
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == 5:
                    r.raise_for_status()
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if attempt == 5:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError("Unreachable")


def upsert_client(cur, obj: Optional[dict]) -> Optional[int]:
    if not obj or obj.get("id") is None:
        return None
    cur.execute(
        """
        INSERT INTO lda.client (
            client_id, client_external_id, name, general_description,
            client_government_entity, client_self_select, state, country,
            ppb_state, ppb_country, effective_date, source_json, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        ON CONFLICT (client_id) DO UPDATE SET
            client_external_id=EXCLUDED.client_external_id,
            name=EXCLUDED.name,
            general_description=EXCLUDED.general_description,
            client_government_entity=EXCLUDED.client_government_entity,
            client_self_select=EXCLUDED.client_self_select,
            state=EXCLUDED.state,
            country=EXCLUDED.country,
            ppb_state=EXCLUDED.ppb_state,
            ppb_country=EXCLUDED.ppb_country,
            effective_date=EXCLUDED.effective_date,
            source_json=EXCLUDED.source_json,
            ingested_at=CURRENT_TIMESTAMP
        """,
        (
            obj.get("id"), obj.get("client_id"), obj.get("name"),
            obj.get("general_description"), obj.get("client_government_entity"),
            obj.get("client_self_select"), obj.get("state"), obj.get("country"),
            obj.get("ppb_state"), obj.get("ppb_country"), obj.get("effective_date"),
            Json(obj),
        ),
    )
    return obj.get("id")


def upsert_registrant(cur, obj: Optional[dict]) -> Optional[int]:
    if not obj or obj.get("id") is None:
        return None
    cur.execute(
        """
        INSERT INTO lda.registrant (
            registrant_id, house_registrant_id, name, description, city, state,
            zip, country, ppb_country, contact_name, contact_telephone,
            dt_updated, source_json, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        ON CONFLICT (registrant_id) DO UPDATE SET
            house_registrant_id=EXCLUDED.house_registrant_id,
            name=EXCLUDED.name,
            description=EXCLUDED.description,
            city=EXCLUDED.city,
            state=EXCLUDED.state,
            zip=EXCLUDED.zip,
            country=EXCLUDED.country,
            ppb_country=EXCLUDED.ppb_country,
            contact_name=EXCLUDED.contact_name,
            contact_telephone=EXCLUDED.contact_telephone,
            dt_updated=EXCLUDED.dt_updated,
            source_json=EXCLUDED.source_json,
            ingested_at=CURRENT_TIMESTAMP
        """,
        (
            obj.get("id"), obj.get("house_registrant_id"), obj.get("name"),
            obj.get("description"), obj.get("city"), obj.get("state"),
            obj.get("zip"), obj.get("country"), obj.get("ppb_country"),
            obj.get("contact_name"), obj.get("contact_telephone"),
            obj.get("dt_updated"), Json(obj),
        ),
    )
    return obj.get("id")


def upsert_filing(cur, filing: dict, client_id: Optional[int], registrant_id: Optional[int]) -> None:
    cur.execute(
        """
        INSERT INTO lda.filing (
            filing_uuid, filing_type, filing_type_display, filing_year,
            filing_period, filing_period_display, income, expenses,
            expenses_method, expenses_method_display, dt_posted,
            termination_date, filing_url, filing_document_url,
            filing_document_content_type, posted_by_name, registrant_id,
            client_id, source_json, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        ON CONFLICT (filing_uuid) DO UPDATE SET
            filing_type=EXCLUDED.filing_type,
            filing_type_display=EXCLUDED.filing_type_display,
            filing_year=EXCLUDED.filing_year,
            filing_period=EXCLUDED.filing_period,
            filing_period_display=EXCLUDED.filing_period_display,
            income=EXCLUDED.income,
            expenses=EXCLUDED.expenses,
            expenses_method=EXCLUDED.expenses_method,
            expenses_method_display=EXCLUDED.expenses_method_display,
            dt_posted=EXCLUDED.dt_posted,
            termination_date=EXCLUDED.termination_date,
            filing_url=EXCLUDED.filing_url,
            filing_document_url=EXCLUDED.filing_document_url,
            filing_document_content_type=EXCLUDED.filing_document_content_type,
            posted_by_name=EXCLUDED.posted_by_name,
            registrant_id=EXCLUDED.registrant_id,
            client_id=EXCLUDED.client_id,
            source_json=EXCLUDED.source_json,
            ingested_at=CURRENT_TIMESTAMP
        """,
        (
            filing.get("filing_uuid"), filing.get("filing_type"),
            filing.get("filing_type_display"), filing.get("filing_year"),
            filing.get("filing_period"), filing.get("filing_period_display"),
            filing.get("income"), filing.get("expenses"),
            filing.get("expenses_method"), filing.get("expenses_method_display"),
            filing.get("dt_posted"), filing.get("termination_date"),
            filing.get("url"), filing.get("filing_document_url"),
            filing.get("filing_document_content_type"), filing.get("posted_by_name"),
            registrant_id, client_id, Json(filing),
        ),
    )


def replace_activities(cur, filing: dict) -> None:
    """Upsert activities by filing/index so activity_id remains stable across reruns."""
    filing_uuid = filing["filing_uuid"]
    activities = filing.get("lobbying_activities") or []

    # Remove only source activities that disappeared. Existing matching rows are
    # updated in place, preserving activity_id for downstream bridge tables.
    cur.execute(
        "DELETE FROM lda.activity WHERE filing_uuid = %s AND activity_index >= %s",
        (filing_uuid, len(activities)),
    )

    for index, activity in enumerate(activities):
        cur.execute(
            """
            INSERT INTO lda.activity (
                filing_uuid, activity_index, general_issue_code,
                general_issue_display, description, foreign_entity_issues,
                source_json, ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (filing_uuid, activity_index) DO UPDATE SET
                general_issue_code=EXCLUDED.general_issue_code,
                general_issue_display=EXCLUDED.general_issue_display,
                description=EXCLUDED.description,
                foreign_entity_issues=EXCLUDED.foreign_entity_issues,
                source_json=EXCLUDED.source_json,
                ingested_at=CURRENT_TIMESTAMP
            RETURNING activity_id
            """,
            (
                filing_uuid, index, activity.get("general_issue_code"),
                activity.get("general_issue_code_display"), activity.get("description"),
                Json(activity.get("foreign_entity_issues") or []), Json(activity),
            ),
        )
        activity_id = cur.fetchone()[0]

        # Bridge rows are source snapshots for this activity. Rebuild them while
        # keeping the stable parent activity_id.
        cur.execute(
            "DELETE FROM lda.activity_government_entity WHERE activity_id = %s",
            (activity_id,),
        )
        cur.execute(
            "DELETE FROM lda.activity_lobbyist WHERE activity_id = %s",
            (activity_id,),
        )

        for entity in activity.get("government_entities") or []:
            entity_id = entity.get("id")
            if entity_id is None:
                continue
            cur.execute(
                """
                INSERT INTO lda.government_entity (
                    government_entity_id, name, source_json, ingested_at
                ) VALUES (%s,%s,%s,CURRENT_TIMESTAMP)
                ON CONFLICT (government_entity_id) DO UPDATE SET
                    name=EXCLUDED.name,
                    source_json=EXCLUDED.source_json,
                    ingested_at=CURRENT_TIMESTAMP
                """,
                (entity_id, entity.get("name") or "", Json(entity)),
            )
            cur.execute(
                """
                INSERT INTO lda.activity_government_entity (activity_id, government_entity_id)
                VALUES (%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (activity_id, entity_id),
            )

        for link in activity.get("lobbyists") or []:
            lobbyist = link.get("lobbyist") or {}
            lobbyist_id = lobbyist.get("id")
            if lobbyist_id is None:
                continue
            cur.execute(
                """
                INSERT INTO lda.lobbyist (
                    lobbyist_id, prefix, first_name, nickname, middle_name,
                    last_name, suffix, source_json, ingested_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                ON CONFLICT (lobbyist_id) DO UPDATE SET
                    prefix=EXCLUDED.prefix,
                    first_name=EXCLUDED.first_name,
                    nickname=EXCLUDED.nickname,
                    middle_name=EXCLUDED.middle_name,
                    last_name=EXCLUDED.last_name,
                    suffix=EXCLUDED.suffix,
                    source_json=EXCLUDED.source_json,
                    ingested_at=CURRENT_TIMESTAMP
                """,
                (
                    lobbyist_id, lobbyist.get("prefix"), lobbyist.get("first_name"),
                    lobbyist.get("nickname"), lobbyist.get("middle_name"),
                    lobbyist.get("last_name"), lobbyist.get("suffix"), Json(lobbyist),
                ),
            )
            cur.execute(
                """
                INSERT INTO lda.activity_lobbyist (
                    activity_id, lobbyist_id, covered_position, is_new, source_json
                ) VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (activity_id, lobbyist_id) DO UPDATE SET
                    covered_position=EXCLUDED.covered_position,
                    is_new=EXCLUDED.is_new,
                    source_json=EXCLUDED.source_json
                """,
                (
                    activity_id, lobbyist_id, link.get("covered_position"),
                    link.get("new"), Json(link),
                ),
            )

def start_run(cur, year: int, filing_type: Optional[str]) -> int:
    cur.execute(
        """
        INSERT INTO lda.ingest_run (filing_year, filing_type)
        VALUES (%s,%s)
        RETURNING ingest_run_id
        """,
        (year, filing_type),
    )
    return cur.fetchone()[0]


def update_run(cur, run_id: int, pages: int, filings: int) -> None:
    cur.execute(
        """
        UPDATE lda.ingest_run
        SET pages_processed=%s, filings_processed=%s
        WHERE ingest_run_id=%s
        """,
        (pages, filings, run_id),
    )


def finish_run(cur, run_id: int, status: str, error: Optional[str] = None) -> None:
    cur.execute(
        """
        UPDATE lda.ingest_run
        SET status=%s, error_message=%s, finished_at=CURRENT_TIMESTAMP
        WHERE ingest_run_id=%s
        """,
        (status, error, run_id),
    )


def main() -> int:
    load_dotenv()
    args = parse_args()
    database_url = env("DATABASE_URL")
    api_key = env("LDA_API_KEY")
    base_url = os.getenv("LDA_API_BASE_URL") or DEFAULT_BASE_URL

    session = requests.Session()
    session.headers.update({"Authorization": "Token " + api_key})

    conn = psycopg2.connect(database_url)
    run_id = None
    pages = 0
    filings_processed = 0

    try:
        execute_schema(conn)
        with conn.cursor() as cur:
            run_id = start_run(cur, args.year, args.filing_type)
        conn.commit()

        params: Dict[str, Any] = {"filing_year": args.year}
        if args.filing_type:
            params["filing_type"] = args.filing_type

        url: Optional[str] = base_url
        first_request = True

        while url:
            data = api_get(session, url, params=params if first_request else None)
            first_request = False
            results = data.get("results") or []

            with conn.cursor() as cur:
                for filing in results:
                    client_id = upsert_client(cur, filing.get("client"))
                    registrant_id = upsert_registrant(cur, filing.get("registrant"))
                    upsert_filing(cur, filing, client_id, registrant_id)
                    replace_activities(cur, filing)
                    filings_processed += 1

                pages += 1
                update_run(cur, run_id, pages, filings_processed)
            conn.commit()

            print(
                f"page={pages} page_rows={len(results)} total_rows={filings_processed} "
                f"api_count={data.get('count')}",
                flush=True,
            )

            if args.max_pages and pages >= args.max_pages:
                break

            url = data.get("next")
            if url and args.sleep:
                time.sleep(args.sleep)

        with conn.cursor() as cur:
            finish_run(cur, run_id, "success")
        conn.commit()
        print(f"DONE pages={pages} filings={filings_processed}")
        return 0

    except Exception as exc:
        conn.rollback()
        if run_id is not None:
            try:
                with conn.cursor() as cur:
                    finish_run(cur, run_id, "failed", str(exc)[:4000])
                conn.commit()
            except Exception:
                conn.rollback()
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
