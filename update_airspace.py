import anthropic
from google.cloud import bigquery
from datetime import datetime
import json
import os

# Handle GCP credentials from GitHub Actions secret
gcp_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if gcp_key:
    with open("/tmp/gcp-key.json", "w") as f:
        f.write(gcp_key)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-key.json"

# --- CLIENTS ---
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
bq = bigquery.Client(project="flights-490708")
TABLE = "flights-490708.flight_data.restricted_airspace"

def ask_claude_for_restrictions():
    response = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{
            "role": "user",
            "content": """Search the web for countries with currently restricted or closed 
            airspace in 2026 due to conflict, war, or military activity. 
            Use sources like safeairspace.net, FAA NOTAMs, and EASA advisories.
            Return ONLY a raw JSON array, no markdown, no explanation.

            CRITICAL COORDINATE RULES:
            - Use the actual sovereign border coordinates of the country ONLY
            - Do NOT expand the bounding box to cover surrounding conflict regions
            - Do NOT include neighboring countries in the bounding box
            - Bounding boxes must be tight to the country's actual borders, not the broader affected region

            EXAMPLES OF CORRECT vs INCORRECT:
            - Iran: correct is (~29.0 to 39.8 lat, 44.0 to 63.3 lon), NOT 25.0 lat which bleeds into Saudi Arabia
            - Ukraine: correct is (~44.4 to 52.4 lat, 22.1 to 40.2 lon), NOT broader bounds that bleed into Poland or Romania
            - Apply this same precision to every country in the list

            Each object must have exactly these fields:
            {
              "country": "string",
              "reason": "string (one sentence)",
              "min_lat": float,
              "max_lat": float,
              "min_lon": float,
              "max_lon": float,
              "severity": "CLOSED or RESTRICTED or HIGH RISK",
              "since": "YYYY-MM-DD"
            }"""
        }]
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
    if text.startswith("json"):
        text = text[4:]
    text = text.strip()
    return json.loads(text)


def refresh_bigquery(zones):
    now = datetime.utcnow().isoformat()
    for zone in zones:
        zone["updated_at"] = now

    bq.query(f"DELETE FROM `{TABLE}` WHERE TRUE").result()
    print(f"Cleared existing data")

    errors = bq.insert_rows_json(TABLE, zones)
    if not errors:
        print(f"Inserted {len(zones)} restricted zones into BigQuery")
        for z in zones:
            print(f"  {z['severity']:12} | {z['country']:20} | {z['reason'][:60]}")
    else:
        print(f"BigQuery errors: {errors}")


if __name__ == "__main__":
    print(f"--- Starting airspace update {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ---")
    zones = ask_claude_for_restrictions()
    print(f"Claude found {len(zones)} restricted zones")
    refresh_bigquery(zones)
    print(f"--- Done {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ---")