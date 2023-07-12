import json
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer


def generate_user(fake: Faker) -> dict[str, str]:
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


def send_msg(fake: Faker, spotify_songs: pd.DataFrame):
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
            "played_at": str(time.time()),
        }
        producer.send("spotify_streaming_topic", value=data)


if __name__ == "__main__":
    num_workers = 3
    fake = Faker()
    spotify_songs = load_dataset()

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with ProcessPoolExecutor(max_workers=num_workers) as exe:
        for _ in range(num_workers):
            exe.submit(send_msg, fake, spotify_songs)
