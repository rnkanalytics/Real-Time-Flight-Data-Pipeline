"""
load_country_polygons.py

Run this ONCE on your Mac to load real country border polygons into BigQuery.
It downloads Natural Earth country GeoJSON and loads it as a BigQuery geography table.

Requirements:
    pip install google-cloud-bigquery requests
"""

import json
import requests
from google.cloud import bigquery

PROJECT = "flights-490708"
DATASET = "flight_data"
TABLE   = "country_polygons"

BQ_TABLE = f"{PROJECT}.{DATASET}.{TABLE}"

# Natural Earth low-res countries GeoJSON (hosted on GitHub, no auth needed)
GEOJSON_URL = (
    "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
)

# Map Natural Earth country names -> your KNOWN_BOUNDS names where they differ
NAME_OVERRIDES = {
    "United States of America":       "United States",
    "Czechia":                         "Czech Republic",
    "Democratic Republic of the Congo": "Congo (Kinshasa)",
    "Republic of the Congo":           "Congo (Brazzaville)",
    "South Korea":                     "South Korea",
    "North Korea":                     "North Korea",
    "Ivory Coast":                     "Ivory Coast",
    "eSwatini":                        "Swaziland",
    "Macedonia":                       "Macedonia",
    "Palestine":                       "Palestine",
    "Taiwan":                          "Taiwan",
    "Kosovo":                          "Kosovo",
    "Gaza":                            "Gaza",
    "West Bank":                       "West Bank",
}


def download_geojson():
    print(f"Downloading country polygons from Natural Earth...")
    r = requests.get(GEOJSON_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"  Downloaded {len(data['features'])} country features")
    return data


def build_rows(geojson):
    rows = []
    for feature in geojson["features"]:
        props = feature.get("properties", {})
        name  = props.get("ADMIN") or props.get("name", "")
        iso   = props.get("ISO_A3", "")
        geom  = feature.get("geometry")

        if not name or not geom:
            continue

        # Apply name overrides
        canonical = NAME_OVERRIDES.get(name, name)

        rows.append({
            "country_name":    canonical,
            "natural_earth_name": name,
            "iso_a3":          iso,
            "geometry_geojson": json.dumps(geom),   # stored as STRING, converted in BQ
        })

    print(f"  Built {len(rows)} rows")
    return rows


def load_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT)

    # Create table with geography column
    schema = [
        bigquery.SchemaField("country_name",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("natural_earth_name",  "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("iso_a3",              "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("geometry_geojson",    "STRING",    mode="REQUIRED"),
    ]

    table_ref = bigquery.Table(BQ_TABLE, schema=schema)

    # Drop and recreate
    client.delete_table(BQ_TABLE, not_found_ok=True)
    client.create_table(table_ref)
    print(f"  Created table {BQ_TABLE}")

    # Load rows
    errors = client.insert_rows_json(BQ_TABLE, rows)
    if errors:
        print(f"  Insert errors: {errors}")
    else:
        print(f"  Loaded {len(rows)} rows successfully")

    # Now create a view/materialized version with actual GEOGRAPHY type
    geo_table = f"{PROJECT}.{DATASET}.country_borders"
    client.query(f"""
        CREATE OR REPLACE TABLE `{geo_table}` AS
        SELECT
            country_name,
            natural_earth_name,
            iso_a3,
            ST_GEOGFROMGEOJSON(geometry_geojson) AS border
        FROM `{BQ_TABLE}`
        WHERE ST_ISVALID(ST_GEOGFROMGEOJSON(geometry_geojson))
    """).result()
    print(f"  Created geography table `{geo_table}` with GEOGRAPHY column")


if __name__ == "__main__":
    geojson = download_geojson()
    rows    = build_rows(geojson)
    load_to_bigquery(rows)
    print("\nDone! Run the view SQL next.")