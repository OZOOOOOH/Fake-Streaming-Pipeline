import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 pyspark-shell"

# Kafka configuration
bootstrap_servers = "localhost:9092"
topic = "spotify_streaming_topic"

options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": topic,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
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
df = df.select(f.from_json(df.value, schema).alias("data")).select("data.*")

# flatten the user and song columns
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


# song duration_ms is in milliseconds, so we replace it to seconds
df = df.withColumn("song_length", (df.song_duration_ms / 1000).cast(FloatType()))

# remove duration_ms column
df = df.drop("song_duration_ms")


# age is a string, so we cast it to integer
df = df.withColumn("user_age", df.user_age.cast(IntegerType()))

# change played_at from UTC to KST
df = df.withColumn(
    "song_played_at",
    f.from_utc_timestamp(df.song_played_at, "Asia/Seoul").cast(TimestampType()),
)
# write the data to the cassandra database
df.writeStream.option("spark.cassandra.connection.host", "localhost:9042").format(
    "org.apache.spark.sql.cassandra"
).options(keyspace="spotify_streaming", table="streams").option(
    "checkpointLocation", "checkpoint"
).start()


df_top_artists = (
    df.withWatermark("song_played_at", "1 minute")
    .groupBy("song_artist")
    .agg(f.avg("user_age").alias("user_age_avg"))
    .withColumn("song_played_at", f.current_timestamp())
)

df_top_artists.writeStream.trigger(processingTime="5 seconds").foreachBatch(
    lambda batch_df, batch_id: batch_df.write.format("org.apache.spark.sql.cassandra")
    .option("checkpointLocation", "checkpoint_artist")
    .options(keyspace="spotify_streaming", table="user_age")
    .mode("append")
    .save()
).outputMode("update").start()


spark.streams.awaitAnyTermination()
