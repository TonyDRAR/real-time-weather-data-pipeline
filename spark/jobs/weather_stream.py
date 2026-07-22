import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/pipeline/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/pipeline/data/silver")
CHECKPOINT_ROOT = os.getenv(
    "CHECKPOINT_ROOT", "/opt/pipeline/data/checkpoints"
)

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://weather-db:5432/weather",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me_weather")
POSTGRES_TABLE = os.getenv(
    "POSTGRES_TABLE",
    "weather_observations",
)

WEATHER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("city", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("observed_at", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("relative_humidity_2m", IntegerType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
    ]
)


def write_bronze(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.select(
            col("kafka_key"),
            col("raw_json"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
        )
        .withColumn("processed_at", current_timestamp())
        .write.mode("append")
        .partitionBy("kafka_partition")
        .json(BRONZE_PATH)
    )


def write_silver(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    clean_df = (
        batch_df.filter(col("payload").isNotNull())
        .select(
            "payload.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("observed_at", to_timestamp("observed_at"))
        .withColumn("ingested_at", to_timestamp("ingested_at"))
        .withColumn("processed_at", current_timestamp())
        .withColumn("observation_date", to_date("observed_at"))
        .dropDuplicates(["event_id"])
        .filter(col("temperature_2m").between(-90.0, 60.0))
        .filter(col("relative_humidity_2m").between(0, 100))
    )

    (
        clean_df.write.mode("append")
        .partitionBy("city", "observation_date")
        .parquet(SILVER_PATH)
    )

    postgres_df = clean_df.select(
        "event_id",
        "city",
        "latitude",
        "longitude",
        "observed_at",
        "ingested_at",
        "processed_at",
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "weather_code",
        "wind_speed_10m",
        "kafka_partition",
        "kafka_offset",
    )

    (
        postgres_df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "500")
        .option("isolationLevel", "READ_COMMITTED")
        .mode("append")
        .save()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("openmeteo-weather-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "payload",
        from_json(col("raw_json"), WEATHER_SCHEMA),
    )

    bronze_query = (
        parsed_df.writeStream.foreachBatch(write_bronze)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .trigger(processingTime="20 seconds")
        .queryName("weather_bronze_writer")
        .start()
    )

    silver_query = (
        parsed_df.writeStream.foreachBatch(write_silver)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
        .trigger(processingTime="20 seconds")
        .queryName("weather_silver_writer")
        .start()
    )

    spark.streams.awaitAnyTermination()

    bronze_query.stop()
    silver_query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
