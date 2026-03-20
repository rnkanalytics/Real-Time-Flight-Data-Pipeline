import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER  = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC   = "flights-raw"
API_URL       = "https://api.airplanes.live/v2/point/0/0/99999"
POLL_INTERVAL = 30
MAX_SEEN_POS  = 30.0


def fetch_flights() -> list:
    try:
        resp = requests.get(API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        return []

    aircraft = data.get("ac", [])
    logger.info(f"Fetched {len(aircraft)} aircraft from airplanes.live")

    records = []
    for ac in aircraft:
        if ac.get("lat") is None or ac.get("lon") is None:
            continue
        if ac.get("seen_pos", 9999) > MAX_SEEN_POS:
            continue

        alt_baro = ac.get("alt_baro")
        if isinstance(alt_baro, str):
            alt_baro = 0

        records.append({
            "icao24":    ac.get("hex", "").lower().strip(),
            "callsign":  ac.get("flight", "").strip(),
            "latitude":  ac.get("lat"),
            "longitude": ac.get("lon"),
            "altitude":  alt_baro,
            "heading":   ac.get("track"),
            "velocity":  ac.get("gs"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    return records


def main():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Kafka producer connected → topic: {KAFKA_TOPIC}")
            break
        except Exception as e:
            logger.warning(f"Kafka not ready, retrying in 5 seconds... {e}")
            time.sleep(5)

    while True:
        records = fetch_flights()
        for record in records:
            producer.send(KAFKA_TOPIC, value=record)
        producer.flush()
        logger.info(f"Published {len(records)} records to Kafka")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()