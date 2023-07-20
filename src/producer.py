import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer


class UserGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate(self) -> dict[str, str]:
        return {
            "name": self.fake.name(),
            "address": self.fake.address(),
            "age": str(self.fake.random_int(min=18, max=80)),
            "nationality": self.fake.country(),
        }


class SongPicker:
    def __init__(self, data_path: str):
        df = pd.read_csv(data_path)
        selected_columns = [
            "artists",
            "album_name",
            "track_name",
            "duration_ms",
            "track_genre",
        ]
        self.songs = df[selected_columns]

    def pick(self) -> dict[str, str]:
        song = dict(self.songs.sample().iloc[0])
        song["duration_ms"] = int(song["duration_ms"])
        return song


class SpotifyStreamingSimulator:
    def __init__(
        self, user_generator: UserGenerator, song_picker: SongPicker, topic_name: str
    ):
        self.user_generator = user_generator
        self.song_picker = song_picker
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic_name = topic_name

    def simulate(self):
        while True:
            user = self.user_generator.generate()
            song = self.song_picker.pick()
            data = {
                "user": user,
                "song": song,
                "played_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            self.producer.send(self.topic_name, value=data)


if __name__ == "__main__":
    user_generator = UserGenerator()
    song_picker = SongPicker(f"{Path(__file__).parent.parent}/data/dataset.csv")
    simulator = SpotifyStreamingSimulator(
        user_generator, song_picker, "spotify_streaming_topic"
    )
    simulator.simulate()
