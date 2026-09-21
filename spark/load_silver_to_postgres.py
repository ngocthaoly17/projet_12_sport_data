import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

LAKE_BUCKET = os.getenv("S3_LAKEHOUSE_BUCKET", "lakehouse")
SILVER_PATH = f"s3a://{LAKE_BUCKET}/silver/employee_activities"

JDBC_URL = "jdbc:postgresql://db:5432/sport_data"
JDBC_TABLE = "analytics.silver_employee_activities"

DB_USER = "sport_user"
DB_PASSWORD = "SportData2026Local"

spark = (
    SparkSession.builder
    .appName("silver-to-postgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Lecture de Silver...")

silver = spark.read.format("delta").load(SILVER_PATH)

export_df = silver.select(
    col("activity_id").cast("long"),
    col("employee_id").cast("integer"),
    "first_name",
    "last_name",
    "business_unit",
    "contract_type",
    "sport_type",
    "declared_sport",
    "activity_date",
    "start_ts",
    "end_ts",
    "duration_minutes",
    "distance_km",
    "source",
    "silver_ingested_at",
)

count = export_df.count()

print(f"{count} lignes Silver à charger dans PostgreSQL")

(
    export_df.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", JDBC_TABLE)
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

print(f"Chargement terminé : {count} lignes envoyées vers PostgreSQL")

spark.stop()