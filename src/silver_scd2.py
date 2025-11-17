import argparse, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from scd_utils import merge_scd_type2

DEFAULT_BRONZE="./data/bronze/customer"
DEFAULT_SILVER="./data/silver/customer"

def get_spark(app="silver-scd2"):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def run(bronze_path, silver_path):
    spark=get_spark()

    bronze=spark.read.format("delta").load(bronze_path)

    df=(bronze
        .withColumn("customer_id", col("customer_id").cast("string"))
        .withColumn("effective_from", to_timestamp(col("event_time"))))

    tracked=["name","email","address"]

    merge_scd_type2(
        spark,
        target_path=silver_path,
        updates_df=df.select("customer_id","name","email","address","effective_from"),
        business_key="customer_id",
        tracked_cols=tracked
    )
    print("SCD2 merge complete:", silver_path)
    spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--bronze", default=DEFAULT_BRONZE)
    p.add_argument("--silver", default=DEFAULT_SILVER)
    args=p.parse_args()
    run(args.bronze,args.silver)
