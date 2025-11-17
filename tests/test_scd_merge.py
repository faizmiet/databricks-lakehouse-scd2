import shutil, os, pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.scd_utils import merge_scd_type2

TESTDIR="./data/test_scd"

@pytest.fixture(scope="module")
def spark():
    spark=(SparkSession.builder
           .master("local[2]")
           .appName("pytest-scd")
           .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .getOrCreate())
    yield spark
    spark.stop()
    shutil.rmtree(TESTDIR, ignore_errors=True)

def test_scd2(spark):
    target=f"{TESTDIR}/silver_customer"
    os.makedirs(TESTDIR, exist_ok=True)

    df1=spark.createDataFrame(
        [("c1","Alice","alice@example.com","addr1")],
        ["customer_id","name","email","address"]
    ).withColumn("effective_from", current_timestamp())

    merge_scd_type2(spark, target, df1, "customer_id", ["name","email","address"])

    df2=spark.createDataFrame(
        [("c1","Alice","alice@example.com","addr2")],
        ["customer_id","name","email","address"]
    ).withColumn("effective_from", current_timestamp())

    merge_scd_type2(spark, target, df2, "customer_id", ["name","email","address"])

    silver=spark.read.format("delta").load(target)
    assert silver.filter("is_current=true").count()==1
    assert silver.count()==2
