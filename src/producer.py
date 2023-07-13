import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer


def generate_user(fake) -> dict[str, str]:
    return {
        "name": fake.name(),
        "address": fake.address(),
        "age": str(fake.random_int(min=18, max=80)),
        "nationality": fake.country(),
    }


def load_dataset() -> pd.DataFrame:
    df = pd.read_csv(f"{Path(__file__).parent.parent}/data/dataset.csv")
    selected_columns = [
        "artists",
        "album_name",
        "track_name",
        "duration_ms",
        "track_genre",
    ]
    return df[selected_columns]


def pick_song(songs: pd.DataFrame) -> dict[str, str]:
    song = dict(songs.sample().iloc[0])
    song["duration_ms"] = int(song["duration_ms"])
    return song


if __name__ == "__main__":
    fake = Faker()
    spotify_songs = load_dataset()

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        user = generate_user(fake)
        song = pick_song(spotify_songs)
        data = {
            "user": user,
            "song": song,
            "played_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        producer.send("spotify_streaming_topic", value=data)
