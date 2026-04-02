import hashlib
import pandas as pd
import os
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import create_engine
import json

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Database connection (UPDATE WITH YOUR CREDENTIALS)
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")

engine = create_engine(DATABASE_URL)
conn = engine.connect()

def hash_text(text):
    return hashlib.md5(text.encode()).hexdigest()

def classify_issue(text):
    prompt = f"""
    Extract the lobbying issue categories from the text below.

    Return ONLY a JSON array of short standardized codes.

    Allowed examples:
    TAX, COM, AVI, LBR, DEF, ENE, HLT, EDU, TRADE, GOV

    Text:
    {text}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    content = response.choices[0].message.content.strip()

    # Clean common model formatting issues (e.g., ```json ... ``` or empty responses)
    if not content:
        return []

    if content.startswith("```"):
        content = content.strip("`")
        content = content.replace("json", "").strip()

    # If still not valid JSON, fail safely
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        print("Warning: Failed to parse JSON. Raw response:", content)
        return []


# Load distinct issues
df = pd.read_sql("""
    SELECT DISTINCT
        filing_uuid,
        COALESCE(issue_desc, '') || ' ' || COALESCE(description, '') AS issue_text
    FROM lobbying_activity_lda
""", conn)

rows = []

for _, row in df.iterrows():
    text = row['issue_text']
    h = hash_text(text)

    codes = classify_issue(text)
    print("\n--- DEBUG ---")
    print("TEXT:", text[:200])
    print("CODES:", codes)
    print("-------------")

    rows.append({
        "filing_uuid": row['filing_uuid'],
        "issue_hash": h,
        "issue_text": text,
        "issue_codes": codes
    })

out = pd.DataFrame(rows)

out.to_sql("issue_ai_classification", engine, if_exists="replace", index=False)