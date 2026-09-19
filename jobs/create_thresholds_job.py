import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pix_fraud.repositories.delta_repository import table_exists


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--threshold-table", required=True)
    parser.add_argument("--environment", required=True)
    return parser.parse_args()


args = parse_args()
BRONZE_TABLE = args.bronze_table
THRESHOLD_TABLE = args.threshold_table
ENVIRONMENT = args.environment

bronze = spark.table(BRONZE_TABLE)
if bronze.limit(1).count() == 0:
    raise RuntimeError(f"A tabela Bronze {BRONZE_TABLE} está vazia.")

values = bronze.agg(
    F.percentile_approx("valor_brl", 0.95).alias("p95_valor_brl"),
    F.percentile_approx(
        F.when(
            F.col("saldo_anterior_pagador") != 0,
            F.col("saldo_posterior_pagador") / F.col("saldo_anterior_pagador"),
        ).otherwise(F.lit(0.0)),
        0.95,
    ).alias("p95_razao_saldo_residual"),
    F.percentile_approx(
        F.when(
            F.col("saldo_anterior_recebedor") != 0,
            F.col("valor_brl") / F.col("saldo_anterior_recebedor"),
        ).otherwise(F.lit(0.0)),
        0.95,
    ).alias("p95_proporcao_valor_recebedor"),
).first()

calibrated_at = datetime.now(timezone.utc)
threshold_version = calibrated_at.strftime("%Y%m%dT%H%M%S%fZ")

threshold_df = spark.createDataFrame(
    [(
        threshold_version,
        ENVIRONMENT,
        calibrated_at,
        None,
        float(values["p95_valor_brl"]),
        float(values["p95_razao_saldo_residual"]),
        float(values["p95_proporcao_valor_recebedor"]),
        calibrated_at,
    )],
    """
    threshold_version string,
    environment string,
    valid_from timestamp,
    valid_to timestamp,
    p95_valor_brl double,
    p95_razao_saldo_residual double,
    p95_proporcao_valor_recebedor double,
    created_at timestamp
    """,
)

if table_exists(spark, THRESHOLD_TABLE):
    target = DeltaTable.forName(spark, THRESHOLD_TABLE)
    target.update(
        condition=(F.col("valid_to").isNull()) & (F.col("environment") == ENVIRONMENT),
        set={"valid_to": F.lit(calibrated_at)},
    )

    threshold_df.write.format("delta").mode("append").saveAsTable(THRESHOLD_TABLE)
else:
    threshold_df.write.format("delta").mode("overwrite").saveAsTable(THRESHOLD_TABLE)

print(
    "Thresholds recalibrados com sucesso: "
    f"version={threshold_version}, "
    f"p95_valor_brl={values['p95_valor_brl']}, "
    f"p95_razao_saldo_residual={values['p95_razao_saldo_residual']}, "
    f"p95_proporcao_valor_recebedor={values['p95_proporcao_valor_recebedor']}"
)
