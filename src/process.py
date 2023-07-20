import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType, IntegerType, TimestampType
from utils import get_schema


class SpotifyStreamingProcessor:
    def __init__(self):
        os.environ[
            "PYSPARK_SUBMIT_ARGS"
        ] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 pyspark-shell"

        self.spark = (
            SparkSession.builder.appName("read_spotify_streaming")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

    def read_kafka_stream(self, bootstrap_servers: str, topic: str) -> DataFrame:
        options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": "earliest",
            "failOnDataLoss": "false",
        }

        schema = get_schema()

        df = self.spark.readStream.format("kafka").options(**options).load()
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(f.from_json(df.value, schema).alias("data")).select("data.*")

        return df

    @staticmethod
    def process_data(df: DataFrame) -> DataFrame:
        df = df.select(
            f.col("user.name").alias("user_name"),
            f.col("user.address").alias("user_address"),
            f.col("user.age").alias("user_age"),
            f.col("user.nationality").alias("user_nationality"),
            f.col("song.artists").alias("song_artist"),
            f.col("song.album_name").alias("song_album_name"),
            f.col("song.track_name").alias("song_name"),
            f.col("song.duration_ms").alias("song_duration_ms"),
            f.col("song.track_genre").alias("song_genre"),
            f.col("played_at").alias("song_played_at"),
        )

        df = df.drop("song_duration_ms")
        df = df.withColumn(
            "song_length", (df.song_duration_ms / 1000).cast(FloatType())
        )
        df = df.withColumn("user_age", df.user_age.cast(IntegerType()))
        df = df.withColumn(
            "song_played_at",
            f.from_utc_timestamp(df.song_played_at, "Asia/Seoul").cast(TimestampType()),
        )

        return df

    def write_to_cassandra(self, df: DataFrame, table: str) -> None:
        df.writeStream.option(
            "spark.cassandra.connection.host", "localhost:9042"
        ).format("org.apache.spark.sql.cassandra").options(
            keyspace="spotify_streaming", table=table
        ).option(
            "checkpointLocation", "checkpoint"
        ).start()

    def compute_top_artists(self, df: DataFrame) -> None:
        df_top_artists = (
            df.withWatermark("song_played_at", "1 minute")
            .groupBy("song_artist")
            .agg(f.avg("user_age").alias("user_age_avg"))
            .withColumn("song_played_at", f.current_timestamp())
        )

        df_top_artists.writeStream.trigger(processingTime="5 seconds").foreachBatch(
            lambda batch_df, batch_id: batch_df.write.format(
                "org.apache.spark.sql.cassandra"
            )
            .option("checkpointLocation", "checkpoint_artist")
            .options(keyspace="spotify_streaming", table="user_age")
            .mode("append")
            .save()
        ).outputMode("update").start()

    def process(self, bootstrap_servers: str, topic: str) -> None:
        df = self.read_kafka_stream(bootstrap_servers, topic)
        df = self.process_data(df)
        self.write_to_cassandra(df, "streams")
        self.compute_top_artists(df)
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    processor = SpotifyStreamingProcessor()
    processor.process("localhost:9092", "spotify_streaming_topic")
