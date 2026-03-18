import os
import xml.etree.ElementTree as ET
from io import BytesIO

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
HOUSE_MEMBERDATA_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")


def load_committee_code_map(root):
    rows = []

    committees_root = root.find("committees")
    if committees_root is None:
        raise RuntimeError("No <committees> section found in House XML.")

    for node in committees_root.iter():
        comcode = node.attrib.get("comcode")
        if not comcode:
            continue

        name = (
            node.findtext("committee-fullname")
            or node.findtext("subcommittee-fullname")
            or node.attrib.get("name")
            or node.attrib.get("committee")
            or node.attrib.get("fullname")
            or node.attrib.get("fullcommittee")
            or (node.text.strip() if node.text and node.text.strip() else None)
        )

        if name:
            rows.append(("House", comcode, name, "house_clerk_memberdata_xml"))

    if not rows:
        raise RuntimeError("Parsed 0 committee code rows from House XML.")

    deduped_rows = list(set(rows))

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pause_altdata.public.committee_code_map;")
            cur.executemany(
                """
                INSERT INTO pause_altdata.public.committee_code_map (
                    chamber,
                    committee_code,
                    committee_name,
                    source_system
                )
                VALUES (%s, %s, %s, %s)
                """,
                deduped_rows,
            )
        conn.commit()


def upsert_stage(rows):
    sql = """
    INSERT INTO pause_altdata.public.stg_committee_member_raw (
        congress,
        chamber,
        committee,
        subcommittee,
        member_name,
        member_id,
        party,
        role,
        state,
        source_system
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pause_altdata.public.stg_committee_member_raw;")
            cur.executemany(sql, rows)
        conn.commit()


def promote_to_committee_member():
    sql = """
    TRUNCATE TABLE pause_altdata.public.committee_member;

    INSERT INTO pause_altdata.public.committee_member (
        congress,
        chamber,
        committee,
        subcommittee,
        member_name,
        member_id,
        party,
        role,
        state
    )
    SELECT DISTINCT
        congress,
        chamber,
        committee,
        subcommittee,
        member_name,
        member_id,
        party,
        role,
        state
    FROM pause_altdata.public.stg_committee_member_raw;
    """
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def committee_code_lookup(root):
    lookup = {}

    committees_root = root.find("committees")
    if committees_root is None:
        return lookup

    for node in committees_root.iter():
        code = node.attrib.get("comcode")
        if not code:
            continue

        name = (
            node.findtext("committee-fullname")
            or node.findtext("subcommittee-fullname")
            or node.attrib.get("name")
            or node.attrib.get("committee")
            or node.attrib.get("fullname")
            or node.attrib.get("fullcommittee")
            or (node.text.strip() if node.text and node.text.strip() else None)
        )

        if name:
            lookup[code] = name

    return lookup


def parse_rows(root):
    rows = []
    code_lookup = committee_code_lookup(root)

    members = root.findall(".//member")
    for member in members:
        state_district = member.findtext("statedistrict")
        state = state_district[:2] if state_district else None

        info = member.find("member-info")
        if info is None:
            continue

        member_name = info.findtext("official-name") or info.findtext("namelist")
        member_id = info.findtext("bioguideID")
        party = info.findtext("party")

        assign = member.find("committee-assignments")
        if assign is None:
            continue

        for comm in assign.findall("committee"):
            comcode = comm.attrib.get("comcode")
            rank = comm.attrib.get("rank")
            committee_name = code_lookup.get(comcode, comcode)

            rows.append((
                None,  # congress not obvious from current XML inspection
                "House",
                committee_name,
                None,
                member_name,
                member_id,
                party,
                rank,
                state,
                "house_clerk_memberdata_xml",
            ))

    return rows


def main():
    print("Downloading House Clerk MemberData.xml ...")
    resp = requests.get(HOUSE_MEMBERDATA_URL, timeout=60)
    resp.raise_for_status()

    root = ET.parse(BytesIO(resp.content)).getroot()
    rows = parse_rows(root)
    load_committee_code_map(root)
    print("Loaded committee_code_map.")

    print(f"Parsed {len(rows)} House committee-member rows.")

    if not rows:
        raise RuntimeError("Parsed 0 rows. Stop and inspect the <committees> section more closely.")

    upsert_stage(rows)
    print("Loaded stg_committee_member_raw.")

    promote_to_committee_member()
    print("Promoted into committee_member.")


if __name__ == "__main__":
    main()