from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DoubleType,
)

from pix_fraud.repositories.delta_repository import overwrite_delta


BRONZE_TABLE = spark.conf.get(
    "pix_fraud.bronze_table",
    "pix_fraud_dev.bronze_pix_transacoes",
)
THRESHOLD_TABLE = spark.conf.get(
    "pix_fraud.threshold_table",
    "pix_fraud_dev.risk_thresholds",
)
THRESHOLD_VERSION = spark.conf.get(
    "pix_fraud.threshold_version",
    "2026-01-001",
)

bronze = spark.table(BRONZE_TABLE)

values = bronze.select(
    F.expr("percentile_approx(valor_brl, 0.95)").alias("p95_valor_brl"),
    F.expr(
        "percentile_approx("
        "CASE WHEN saldo_anterior_pagador != 0 "
        "THEN saldo_posterior_pagador / saldo_anterior_pagador "
        "ELSE 0 END, 0.95)"
    ).alias("p95_razao_saldo_residual"),
    F.expr(
        "percentile_approx("
        "CASE WHEN saldo_anterior_recebedor != 0 "
        "THEN valor_brl / saldo_anterior_recebedor "
        "ELSE 0 END, 0.95)"
    ).alias("p95_proporcao_valor_recebedor"),
).first()

threshold_df = spark.createDataFrame(
    [(
        THRESHOLD_VERSION,
        spark.conf.get("pix_fraud.environment", "dev"),
        None,
        values["p95_valor_brl"],
        values["p95_razao_saldo_residual"],
        values["p95_proporcao_valor_recebedor"],
    )],
    schema=StructType([
        StructField("threshold_version", StringType(), False),
        StructField("environment", StringType(), False),
        StructField("valid_from", TimestampType(), True),
        StructField("p95_valor_brl", DoubleType(), False),
        StructField("p95_razao_saldo_residual", DoubleType(), False),
        StructField("p95_proporcao_valor_recebedor", DoubleType(), False),
    ]),
).withColumn(
    "valid_from",
    F.current_timestamp(),
).withColumn(
    "valid_to",
    F.lit(None).cast("timestamp"),
).withColumn(
    "created_at",
    F.current_timestamp(),
)

overwrite_delta(threshold_df, THRESHOLD_TABLE)
