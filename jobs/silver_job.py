from pix_fraud.transformations.silver import transform_silver
from pix_fraud.repositories.delta_repository import (
    get_watermark,
    merge_delta,
    set_watermark,
)
from pyspark.sql import functions as F


BRONZE_TABLE = spark.conf.get(
    "pix_fraud.bronze_table", "pix_fraud_dev.bronze_pix_transacoes"
)
SILVER_TABLE = spark.conf.get(
    "pix_fraud.silver_table", "pix_fraud_dev.silver_pix_transacoes"
)
THRESHOLD_TABLE = spark.conf.get(
    "pix_fraud.threshold_table", "pix_fraud_dev.risk_thresholds"
)
WATERMARK_TABLE = spark.conf.get(
    "pix_fraud.watermark_table", "pix_fraud_dev.layer_watermarks"
)

threshold = (
    spark.table(THRESHOLD_TABLE)
    .filter(F.col("valid_to").isNull())
    .orderBy(F.col("valid_from").desc())
    .limit(1)
    .first()
)

if not threshold:
    raise RuntimeError(f"Nenhum threshold ativo encontrado em {THRESHOLD_TABLE}")

watermark = get_watermark(spark, WATERMARK_TABLE, "silver")

bronze = spark.table(BRONZE_TABLE)
if watermark is not None:
    bronze = bronze.filter(F.col("ingestion_timestamp") > F.lit(watermark))

if bronze.limit(1).count() == 0:
    print("Nenhum registro novo para Silver.")
else:
    silver = transform_silver(
        bronze,
        p95_valor_brl=threshold["p95_valor_brl"],
        p95_razao_saldo_residual=threshold["p95_razao_saldo_residual"],
        p95_proporcao_valor_recebedor=threshold["p95_proporcao_valor_recebedor"],
        threshold_version=threshold["threshold_version"],
    )

    merge_delta(
        spark=spark,
        df=silver,
        target_table=SILVER_TABLE,
        key="transaction_id",
    )

    max_ingestion = silver.agg(F.max("ingestion_timestamp")).first()[0]
    set_watermark(spark, WATERMARK_TABLE, "silver", max_ingestion)
