import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell"

# Kafka configuration
bootstrap_servers = "localhost:9092"
topic = "spotify_streaming_topic"

options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": topic,
    "startingOffsets": "earliest",
}

schema = StructType(
    [
        StructField(
            "user",
            StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("age", StringType(), True),
                    StructField("nationality", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "song",
            StructType(
                [
                    StructField("artists", StringType(), True),
                    StructField("album_name", StringType(), True),
                    StructField("track_name", StringType(), True),
                    StructField("duration_ms", IntegerType(), True),
                    StructField("track_genre", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("played_at", TimestampType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("read_spotify_streaming")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").options(**options).load()

# make a new dataframe with the json data
df = df.selectExpr("CAST(value AS STRING)")
df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

# flatten the user and song columns
df = df.select(
    col("user.name").alias("user_name"),
    col("user.address").alias("user_address"),
    col("user.age").alias("user_age"),
    col("user.nationality").alias("user_nationality"),
    col("song.artists").alias("song_artist"),
    col("song.album_name").alias("song_album_name"),
    col("song.track_name").alias("song_name"),
    col("song.duration_ms").alias("song_duration_ms"),
    col("song.track_genre").alias("song_genre"),
    col("played_at").alias("song_played_at"),
)


# song duration_ms is in milliseconds, so we replace it to seconds
df = df.withColumn("song_length", (df.song_duration_ms / 1000).cast(DoubleType()))

# remove duration_ms column
df = df.drop("song_duration_ms")


# age is a string, so we cast it to integer
df = df.withColumn("user_age", df.user_age.cast(IntegerType()))

# write the data to the console
query = df.writeStream.format("console").start()
query.awaitTermination()
