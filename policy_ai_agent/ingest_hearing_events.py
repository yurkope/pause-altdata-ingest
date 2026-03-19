import os
import time

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
LDA_API_KEY = os.getenv("LDA_API_KEY")
HEARING_SOURCE_URL = os.getenv(
    "HEARING_SOURCE_URL",
    "https://api.govinfo.gov/collections/CHRG/2024-01-01T00:00:00Z",
)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")


def fetch_source():
    if not HEARING_SOURCE_URL:
        raise RuntimeError("HEARING_SOURCE_URL is not set in .env")

    print(f"Downloading hearing source from: {HEARING_SOURCE_URL}")

    params = {
        "offsetMark": "*",
        "pageSize": 100,
    }
    if "api.govinfo.gov" in HEARING_SOURCE_URL:
        if not LDA_API_KEY:
            raise RuntimeError(
                "LDA_API_KEY is required for GovInfo sources. Add it to .env and rerun."
            )
        params["api_key"] = LDA_API_KEY

    resp = requests.get(HEARING_SOURCE_URL, params=params, timeout=60)
    resp.raise_for_status()
    print(f"Fetch status: {resp.status_code}")
    print(resp.text[:500])
    return resp.json()


def fetch_package_summary(package_link):
    if not package_link:
        return {}

    params = {}
    if "api.govinfo.gov" in package_link:
        if not LDA_API_KEY:
            raise RuntimeError(
                "LDA_API_KEY is required for GovInfo sources. Add it to .env and rerun."
            )
        params["api_key"] = LDA_API_KEY

    resp = requests.get(package_link, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_rows(payload):
    """
    Convert source payload into rows shaped for hearing_event.

    Expected output row shape:
    (
        hearing_timestamp,
        committee_name_raw,
        title,
        description,
        location,
        source_system,
        source_event_id,
        url,
        su_doc_class_number,
    )
    """
    rows = []
    debug_printed = 0

    if isinstance(payload, dict):
        events = (
            payload.get("results")
            or payload.get("packages")
            or payload.get("items")
            or []
        )
    else:
        events = []

    for event in events:
        package_link = event.get("packageLink") or event.get("detailsLink") or event.get("url")
        package_summary = {}

        if package_link:
            try:
                package_summary = fetch_package_summary(package_link)
                time.sleep(0.05)
            except Exception as e:
                print(f"Warning: failed to fetch package summary for {package_link}: {e}")
                package_summary = {}

        if debug_printed < 3:
            print("\n--- DEBUG EVENT ---")
            print("event keys:", sorted(event.keys()))
            print("package_link:", package_link)
            print(
                "package_summary keys:",
                sorted(package_summary.keys()) if isinstance(package_summary, dict) else type(package_summary),
            )
            if isinstance(package_summary, dict):
                for k in [
                    "committee",
                    "committeeName",
                    "congressCommittee",
                    "title",
                    "packageTitle",
                    "dateIssued",
                    "location",
                    "heldDate",
                    "docClass",
                    "suDocClassNumber",
                    "branch",
                    "congress",
                    "collectionName",
                    "governmentAuthor1",
                    "governmentAuthor2",
                    "chamber",
                ]:
                    if k in package_summary:
                        print(f"package_summary[{k!r}] =", package_summary.get(k))
            debug_printed += 1

        hearing_timestamp = (
            package_summary.get("dateIssued")
            or package_summary.get("hearing_timestamp")
            or event.get("dateIssued")
            or event.get("date")
            or event.get("lastModified")
        )

        committee_name_raw = (
            package_summary.get("committee_name_raw")
            or package_summary.get("committee")
            or package_summary.get("committeeName")
            or package_summary.get("congressCommittee")
            or event.get("committee_name_raw")
            or event.get("committee")
            or event.get("committeeName")
            or event.get("collectionName")
            or event.get("docClass")
        )

        title = (
            package_summary.get("title")
            or package_summary.get("packageTitle")
            or event.get("title")
            or event.get("packageTitle")
        )

        description = (
            package_summary.get("description")
            or package_summary.get("summary")
            or event.get("description")
            or event.get("summary")
            or title
        )

        location = package_summary.get("location") or event.get("location")

        source_event_id = (
            str(event.get("id"))
            if event.get("id") is not None
            else event.get("packageId")
        )

        url = (
            event.get("url")
            or event.get("packageLink")
            or event.get("detailsLink")
            or event.get("link")
        )

        su_doc_class_number = (
            package_summary.get("suDocClassNumber")
            or event.get("suDocClassNumber")
        )

        if committee_name_raw:
            committee_name_raw = committee_name_raw.replace("Committee on ", "")
            committee_name_raw = committee_name_raw.replace("Joint Committee on ", "")
            committee_name_raw = committee_name_raw.replace("the ", "")
            committee_name_raw = committee_name_raw.strip()

        # Keep only rows that have a source id or title, and skip rows without committee info for now.
        if (not source_event_id and not title) or not committee_name_raw:
            continue

        rows.append(
            (
                hearing_timestamp,
                committee_name_raw,
                title,
                description,
                location,
                "govinfo_chrg",
                source_event_id,
                url,
                su_doc_class_number,
            )
        )

    return rows


def load_hearing_events(rows):
    sql = """
    INSERT INTO pause_altdata.public.hearing_event (
        hearing_timestamp,
        committee_name_raw,
        title,
        description,
        location,
        source_system,
        source_event_id,
        url,
        su_doc_class_number
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pause_altdata.public.hearing_event;")
            cur.executemany(sql, rows)
        conn.commit()


def main():
    try:
        payload = fetch_source()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(
            f"Failed to fetch hearing source: {e}. "
            "If this is GovInfo, confirm LDA_API_KEY is set in .env."
        ) from e

    rows = parse_rows(payload)

    print(f"Parsed {len(rows)} hearing rows.")
    print("Rows without committee metadata are currently skipped.")

    if not rows:
        raise RuntimeError("Parsed 0 hearing rows. Inspect source payload and parse logic.")

    load_hearing_events(rows)
    print("Loaded hearing_event.")


if __name__ == "__main__":
    main()