import os
import dotenv
import json
import random
import time
import uuid

from faker import Faker
from datetime import datetime
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
USERS_COUNT = 100
EVENT_INTERVAL = 1

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

with open("metadata/artists.json", "r", encoding="utf-8") as f:
    artists = json.load(f)

with open("metadata/songs.json", "r", encoding="utf-8") as f:
    songs = json.load(f)

devices = ["mobile", "desktop", "web"]
countries = ["US", "UK", "CA", "AU", "IN", "DE"]
event_types = ["play", "pause", "skip", "add_to_playlist"]

user_ids = [str(uuid.uuid4()) for _ in range(USERS_COUNT)]

def generate_event():
    pair = random.choice(artists)
    artist_songs = [song for song in songs if song["artist_id"] == pair["artist_id"]]
    song = random.choice(artist_songs)

    user_id = random.choice(user_ids)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "artist_id": pair["artist_id"],
        "artist_name": pair["artist_name"],
        "song_id": song["song_id"],
        "song_name": song["song_name"],
        "song_year": song["song_year"],
        "event_type": random.choice(event_types),
        "device": random.choice(devices),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

if __name__ == "__main__":
    print("🎧 Starting Spotify data simulator...")
    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, value=event)
        print(f"🎧 Produced event: {event['song_name']} by {event['artist_name']}")
        time.sleep(EVENT_INTERVAL)