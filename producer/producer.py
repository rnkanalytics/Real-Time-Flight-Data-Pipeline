import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import functools
print = functools.partial(print, flush=True)

def create_producer():
    for attempt in range(10):
        try:
            print(f"Connecting to Kafka... attempt {attempt + 1}")
            return KafkaProducer(
                bootstrap_servers=os.environ.get('KAFKA_BROKER', 'localhost:9092'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable:
            print("Kafka not ready yet — waiting 5 seconds...")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after 10 attempts")

producer = create_producer()

def fetch_flights():
    """Call the OpenSky API and return raw flight states"""
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(
            url,
            timeout=10,
            auth=(
                os.environ.get('OPENSKY_CLIENT_ID'),
                os.environ.get('OPENSKY_CLIENT_SECRET')
            )
        )
        if response.status_code != 200:
            print(f"API returned status {response.status_code} — skipping this fetch")
            return []
        data = response.json()
        return data.get('states', [])
    except Exception as e:
        print(f"Error fetching flights: {e}")
        return []

def parse_flight(state):
    """Convert raw API array into a readable dictionary"""
    return {
        "icao24":         state[0],
        "callsign":       state[1],
        "origin_country": state[2],
        "longitude":      state[5],
        "latitude":       state[6],
        "altitude":       state[7],
        "velocity":       state[9],
        "heading":        state[10],
        "on_ground":      state[8],
        "timestamp":      state[3],
        "category":       state[17] if len(state) > 17 else None,
    }

print("Producer starting — fetching flights every 10 seconds...")

while True:
    flights = fetch_flights()
    print(f"Fetched {len(flights)} flights")

    for state in flights:
        if state[5] and state[6]:
            flight = parse_flight(state)
            producer.send('flights-raw', flight)

    producer.flush()
    print(f"Sent {len(flights)} flights to Kafka topic: flights-raw")
    time.sleep(10)