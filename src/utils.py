from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_schema() -> StructType:
    return StructType(
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
