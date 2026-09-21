import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    round as spark_round,
    to_date,
    to_timestamp,
)

REF_BUCKET = os.getenv("S3_REFERENCE_BUCKET", "references")
LAKE_BUCKET = os.getenv("S3_LAKEHOUSE_BUCKET", "lakehouse")

BRONZE_PATH = f"s3a://{LAKE_BUCKET}/bronze/activities"
SILVER_PATH = f"s3a://{LAKE_BUCKET}/silver/employee_activities"
CHECKPOINT = f"s3a://{LAKE_BUCKET}/checkpoints/silver_employee_activities"

EMPLOYEES_PATH = f"s3a://{REF_BUCKET}/company/employees.csv"
SPORTS_PATH = f"s3a://{REF_BUCKET}/sports/employee_sports.csv"

# PostgreSQL interne au réseau Docker
JDBC_URL = "jdbc:postgresql://db:5432/sport_data"
JDBC_USER = "sport_user"
JDBC_PASSWORD = "SportData2026Local"

JDBC_STAGING_TABLE = "analytics.silver_employee_activities_staging"


spark = (
    SparkSession.builder
    .appName("sport-data-silver-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# ATTENTE DE BRONZE
# =========================================================

while True:
    try:
        bronze = (
            spark.readStream
            .format("delta")
            .load(BRONZE_PATH)
        )
        break

    except Exception as exc:
        print(
            f"Delta Bronze pas encore disponible: {exc}",
            flush=True,
        )
        time.sleep(5)


# =========================================================
# MICRO-BATCH SILVER
# =========================================================

def enrich_and_write(batch_df, batch_id):

    print(
        f"Début Silver micro-batch {batch_id}",
        flush=True,
    )

    # -----------------------------------------------------
    # Référentiel collaborateurs
    # -----------------------------------------------------

    employees = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(EMPLOYEES_PATH)
        .select(
            col("employee_id")
            .cast("long")
            .alias("ref_employee_id"),

            "first_name",
            "last_name",
            "business_unit",
            "contract_type",

            col("annual_gross_salary")
            .cast("double")
            .alias("annual_gross_salary"),
        )
    )

    # -----------------------------------------------------
    # Référentiel sports
    # -----------------------------------------------------

    sports = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(SPORTS_PATH)
        .select(
            col("employee_id")
            .cast("long")
            .alias("sport_employee_id"),

            col("sport")
            .alias("declared_sport"),
        )
    )

    # -----------------------------------------------------
    # Enrichissement Silver
    # -----------------------------------------------------

    enriched = (
        batch_df

        .withColumn(
            "start_ts",
            to_timestamp(col("start_at"))
        )

        .withColumn(
            "end_ts",
            to_timestamp(col("end_at"))
        )

        .join(
            employees,
            col("employee_id") == col("ref_employee_id"),
            "left",
        )

        .join(
            sports,
            col("employee_id") == col("sport_employee_id"),
            "left",
        )

        .withColumn(
            "activity_date",
            to_date("start_ts")
        )

        .withColumn(
            "duration_minutes",
            spark_round(
                (
                    col("end_ts").cast("long")
                    - col("start_ts").cast("long")
                ) / 60,
                1,
            ),
        )

        .withColumn(
            "distance_km",
            spark_round(
                col("distance_m") / 1000,
                2,
            ),
        )

        .withColumn(
            "silver_ingested_at",
            current_timestamp(),
        )

        .drop(
            "ref_employee_id",
            "sport_employee_id",
        )
    )

    # =====================================================
    # 1. ECRITURE DELTA SILVER
    # =====================================================

    enriched.write \
        .format("delta") \
        .mode("append") \
        .save(SILVER_PATH)

    # =====================================================
    # 2. PREPARATION POUR POSTGRESQL / POWER BI
    # =====================================================

    postgres_df = enriched.select(
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

    # -----------------------------------------------------
    # Table temporaire de staging
    # -----------------------------------------------------

    (
        postgres_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", JDBC_STAGING_TABLE)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # =====================================================
    # 3. UPSERT VERS LA TABLE POWER BI
    # =====================================================

    jdbc_properties = {
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        JDBC_USER,
        JDBC_PASSWORD,
    )

    statement = connection.createStatement()

    upsert_sql = """
        INSERT INTO analytics.silver_employee_activities (
            activity_id,
            employee_id,
            first_name,
            last_name,
            business_unit,
            contract_type,
            sport_type,
            declared_sport,
            activity_date,
            start_ts,
            end_ts,
            duration_minutes,
            distance_km,
            source,
            silver_ingested_at
        )

        SELECT
            activity_id,
            employee_id,
            first_name,
            last_name,
            business_unit,
            contract_type,
            sport_type,
            declared_sport,
            activity_date,
            start_ts,
            end_ts,
            duration_minutes,
            distance_km,
            source,
            silver_ingested_at

        FROM analytics.silver_employee_activities_staging

        ON CONFLICT (activity_id)

        DO UPDATE SET
            employee_id = EXCLUDED.employee_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            business_unit = EXCLUDED.business_unit,
            contract_type = EXCLUDED.contract_type,
            sport_type = EXCLUDED.sport_type,
            declared_sport = EXCLUDED.declared_sport,
            activity_date = EXCLUDED.activity_date,
            start_ts = EXCLUDED.start_ts,
            end_ts = EXCLUDED.end_ts,
            duration_minutes = EXCLUDED.duration_minutes,
            distance_km = EXCLUDED.distance_km,
            source = EXCLUDED.source,
            silver_ingested_at = EXCLUDED.silver_ingested_at
    """

    statement.executeUpdate(upsert_sql)

    statement.close()
    connection.close()

    print(
        f"Silver micro-batch {batch_id}: "
        f"Delta + PostgreSQL synchronisés",
        flush=True,
    )


# =========================================================
# STREAMING
# =========================================================

query = (
    bronze.writeStream

    .foreachBatch(enrich_and_write)

    .option(
        "checkpointLocation",
        CHECKPOINT,
    )

    .start()
)

query.awaitTermination()