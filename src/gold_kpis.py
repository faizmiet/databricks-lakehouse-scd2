import argparse, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

DEFAULT_SILVER="./data/silver/customer"
DEFAULT_GOLD="./data/gold/customer_kpis"

def get_spark(app="gold-kpis"):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def run(silver_path, gold_path):
    spark=get_spark()
    silver=spark.read.format("delta").load(silver_path)

    active=silver.filter(col("is_current")==True)

    kpi=active.agg(countDistinct("customer_id").alias("active_customer_count"))
    kpi.write.format("delta").mode("overwrite").save(gold_path)

    print("Gold KPIs written:", gold_path)
    spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--silver", default=DEFAULT_SILVER)
    p.add_argument("--gold", default=DEFAULT_GOLD)
    args=p.parse_args()
    run(args.silver,args.gold)
