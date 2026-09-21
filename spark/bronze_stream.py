import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("ACTIVITIES_TOPIC", "sport_data.raw.activities")
LAKE_BUCKET = os.getenv("S3_LAKEHOUSE_BUCKET", "lakehouse")

BRONZE_PATH = f"s3a://{LAKE_BUCKET}/bronze/activities"
CHECKPOINT = f"s3a://{LAKE_BUCKET}/checkpoints/bronze_activities"

spark = (
    SparkSession.builder
    .appName("sport-data-bronze-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Schéma de l'activité contenue dans payload
# ---------------------------------------------------------

activity_schema = StructType(
    [
        StructField("activity_id", LongType()),
        StructField("employee_id", LongType()),
        StructField("start_at", StringType()),
        StructField("end_at", StringType()),
        StructField("sport_type", StringType()),
        StructField("distance_m", DoubleType()),
        StructField("comment", StringType()),
        StructField("source", StringType()),
        StructField("created_at", StringType()),
        StructField("__op", StringType()),
        StructField("__table", StringType()),
        StructField("__source_ts_ms", LongType()),
    ]
)


debezium_schema = StructType(
    [
        StructField("payload", activity_schema)
    ]
)


# ---------------------------------------------------------
# Lecture du topic Redpanda
# ---------------------------------------------------------

kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ---------------------------------------------------------
# Extraction du payload Debezium
# ---------------------------------------------------------

parsed = kafka.select(
    from_json(
        col("value").cast("string"),
        debezium_schema
    ).alias("debezium"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
)


# ---------------------------------------------------------
# Bronze
# ---------------------------------------------------------

bronze = (
    parsed
    .select(
        "debezium.payload.*",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
    )
    .withColumn(
        "bronze_ingested_at",
        current_timestamp()
    )
)


# ---------------------------------------------------------
# Écriture Delta Lake
# ---------------------------------------------------------

query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .start(BRONZE_PATH)
)

query.awaitTermination()