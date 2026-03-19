import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("LDA_API_KEY")

url = "https://api.govinfo.gov/collections/CHRG/2024-01-01T00:00:00Z"
params = {
    "api_key": API_KEY,
    "offsetMark": "*",
    "pageSize": 20,
}

resp = requests.get(url, params=params, timeout=60)
resp.raise_for_status()

data = resp.json()
events = data.get("packages", [])

for event in events:
    package_link = event.get("packageLink")
    if not package_link:
        continue

    summary = requests.get(
        package_link,
        params={"api_key": API_KEY},
        timeout=60,
    ).json()

    print(
        summary.get("packageId"),
        "|",
        summary.get("suDocClassNumber"),
        "|",
        summary.get("title"),
    )