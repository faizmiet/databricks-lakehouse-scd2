import argparse, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

DEFAULT_RAW="./data/raw/customer"
DEFAULT_BRONZE="./data/bronze/customer"

def get_spark(app="bronze-ingest"):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def run(raw_path, bronze_path):
    spark=get_spark()
    df=spark.read.json(raw_path).withColumn("ingest_date", current_date())
    df.write.format("delta").mode("append").save(bronze_path)
    print("Bronze ingest complete:", bronze_path)
    spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--raw", default=DEFAULT_RAW)
    p.add_argument("--bronze", default=DEFAULT_BRONZE)
    args=p.parse_args()
    run(args.raw,args.bronze)
