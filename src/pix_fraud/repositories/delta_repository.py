from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def table_exists(spark, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def merge_delta(
    spark,
    df: DataFrame,
    target_table: str,
    key: str = "transaction_id",
) -> None:
    """Upsert idempotente por chave natural/determinística."""
    if not table_exists(spark, target_table):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("target")
        .merge(df.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def get_watermark(spark, table_name: str, layer_name: str):
    if not table_exists(spark, table_name):
        return None

    row = (
        spark.table(table_name)
        .filter(F.col("layer_name") == layer_name)
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .first()
    )
    return row["last_ingestion_timestamp"] if row else None


def set_watermark(spark, table_name: str, layer_name: str, value) -> None:
    row_df = spark.createDataFrame(
        [(layer_name, value)],
        "layer_name string, last_ingestion_timestamp timestamp",
    ).withColumn("updated_at", F.current_timestamp())

    if not table_exists(spark, table_name):
        row_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("target")
        .merge(row_df.alias("source"), "target.layer_name = source.layer_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
