from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit

def ensure_delta_table(spark: SparkSession, path: str, schema_df: DataFrame = None):
    if not DeltaTable.isDeltaTable(spark, path):
        if schema_df is None:
            spark.createDataFrame([], schema=[]).write.format("delta").mode("overwrite").save(path)
        else:
            schema_df.limit(0).write.format("delta").mode("overwrite").save(path)

def merge_scd_type2(spark: SparkSession,
                    target_path: str,
                    updates_df: DataFrame,
                    business_key: str,
                    tracked_cols: list,
                    effective_from_col: str = "effective_from",
                    effective_to_col: str = "effective_to",
                    is_current_col: str = "is_current"):

    if not DeltaTable.isDeltaTable(spark, target_path):
        base = updates_df.limit(0)
        for colname in [is_current_col, effective_from_col, effective_to_col, "created_ts", "updated_ts"]:
            base = base.withColumn(colname, lit(None))
        base.write.format("delta").mode("overwrite").save(target_path)

    target = DeltaTable.forPath(spark, target_path)

    change_conditions = []
    for c in tracked_cols:
        change_conditions.append(f"NOT (t.`{c}` <=> s.`{c}`)")
    change_sql = " OR ".join(change_conditions)

    updates_df.createOrReplaceTempView("updates_temp")

    expire_sql = f"""
    MERGE INTO delta.`{target_path}` t
    USING updates_temp s
    ON t.`{business_key}` = s.`{business_key}` AND t.{is_current_col} = true
    WHEN MATCHED AND ({change_sql})
      THEN UPDATE SET {is_current_col} = false,
                      {effective_to_col} = s.{effective_from_col},
                      updated_ts = current_timestamp()
    """
    spark.sql(expire_sql)

    insert_df = updates_df.withColumn(is_current_col, lit(True)) \
                          .withColumn(effective_to_col, lit(None)) \
                          .withColumn("created_ts", current_timestamp()) \
                          .withColumn("updated_ts", lit(None))

    insert_df.write.format("delta").mode("append").save(target_path)

    return DeltaTable.forPath(spark, target_path)
