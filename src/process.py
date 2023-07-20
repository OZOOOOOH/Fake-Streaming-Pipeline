import os

from config import Config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType, IntegerType, TimestampType
from utils import get_schema


class SpotifyStreamingProcessor:
    def __init__(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = Config.PYSPARK_SUBMIT_ARGS

        self.spark = (
            SparkSession.builder.appName(Config.SPARK_APP_NAME)
            .config("spark.driver.bindAddress", Config.SPARK_DRIVER_BIND_ADDRESS)
            .config("spark.driver.host", Config.SPARK_DRIVER_HOST)
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel(Config.SPARK_LOG_LEVEL)

    def read_kafka_stream(self) -> DataFrame:
        options = {
            "kafka.bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": Config.KAFKA_SPOTIFY_TOPIC,
            "startingOffsets": Config.KAFKA_STARTING_OFFSETS,
            "failOnDataLoss": Config.KAFKA_FAIL_ON_DATA_LOSS,
        }

        schema = get_schema()

        df = self.spark.readStream.format("kafka").options(**options).load()
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(f.from_json(df.value, schema).alias("data")).select("data.*")

        return df

    @staticmethod
    def process_df(df: DataFrame) -> DataFrame:
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

        df = df.withColumn(
            "song_length", (df.song_duration_ms / 1000).cast(FloatType())
        )
        df = df.withColumn("user_age", df.user_age.cast(IntegerType()))
        df = df.withColumn(
            "song_played_at",
            f.from_utc_timestamp(df.song_played_at, Config.TIMEZONE).cast(
                TimestampType()
            ),
        )
        df = df.drop("song_duration_ms")

        return df

    def write_streams2cassandra(self, df: DataFrame) -> None:
        df.writeStream.option(
            "spark.cassandra.connection.host", Config.CASSANDRA_CONNECTION_HOST
        ).format("org.apache.spark.sql.cassandra").options(
            keyspace=Config.CASSANDRA_KEYSPACE, table=Config.STREAMS_TABLE
        ).option(
            "checkpointLocation", Config.CHECKPOINT_LOCATION
        ).start()

    def get_listener_age(self, df: DataFrame) -> DataFrame:
        listener_age = (
            df.withWatermark("song_played_at", "1 minute")
            .groupBy("song_artist")
            .agg(f.avg("user_age").alias("user_age_avg"))
            .withColumn("song_played_at", f.current_timestamp())
        )

        return listener_age

    def write_listener_age2cassandra(self, listener_age: DataFrame) -> None:
        listener_age.writeStream.trigger(processingTime="5 seconds").foreachBatch(
            lambda batch_df, batch_id: batch_df.write.format(
                "org.apache.spark.sql.cassandra"
            )
            .option("checkpointLocation", Config.CHECKPOINT_ARTIST_LOCATION)
            .options(keyspace=Config.CASSANDRA_KEYSPACE, table=Config.USER_AGE_TABLE)
            .mode("append")
            .save()
        ).outputMode("update").start()

    def process(self) -> None:
        df = self.read_kafka_stream()
        df = self.process_df(df)
        self.write_streams2cassandra(df)
        df_top_artists = self.get_listener_age(df)
        self.write_listener_age2cassandra(df_top_artists)
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    processor = SpotifyStreamingProcessor()
    processor.process()
