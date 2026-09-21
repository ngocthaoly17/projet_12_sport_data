import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum

LAKE_BUCKET = os.getenv("S3_LAKEHOUSE_BUCKET", "lakehouse")
SILVER_PATH = f"s3a://{LAKE_BUCKET}/silver/employee_activities"
OUTPUT_DIR = "/powerbi/output/_tmp"
FINAL_NAME = "/powerbi/output/sport_activity_powerbi.csv"

spark = SparkSession.builder.appName("sport-data-powerbi-export").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load(SILVER_PATH)

# Table détaillée, prête à être importée dans Power BI Desktop.
columns = [
    "activity_id",
    "employee_id",
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
]

(df.select(*columns).orderBy("activity_id").coalesce(1)
   .write.mode("overwrite").option("header", "true").csv(OUTPUT_DIR))

# Spark crée un part-*.csv : le renommage final est fait par le shell du service.
print("EXPORT_TMP=" + OUTPUT_DIR)
spark.stop()
