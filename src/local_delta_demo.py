from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = (SparkSession.builder
    .appName("local-delta-demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

data = [(1, "Alice"), (2, "Bob")]
cols = ["id", "name"]
df = spark.createDataFrame(data, cols)

out_path = "./data/delta/customer"

(df.withColumn("ingest_date", current_date())
   .write
   .format("delta")
   .mode("overwrite")
   .save(out_path))

print("Wrote Delta to", out_path)
spark.read.format("delta").load(out_path).show()
spark.stop()
