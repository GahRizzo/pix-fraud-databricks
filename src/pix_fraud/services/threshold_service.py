from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pix_fraud.repositories.delta_repository import table_exists


def get_active_threshold(
    spark,
    threshold_table: str,
):
    """Retorna o threshold atualmente ativo."""

    if not table_exists(spark, threshold_table):
        return None

    return (
        spark.table(threshold_table)
        .filter(F.col("valid_to").isNull())
        .orderBy(F.col("valid_from").desc())
        .limit(1)
        .first()
    )


def calculate_thresholds(
    bronze: DataFrame,
) -> dict:
    """Calcula os thresholds de risco a partir da Bronze."""

    metrics = (
        bronze
        .select(
            "valor_brl",
            "saldo_anterior_pagador",
            "saldo_posterior_pagador",
            "saldo_anterior_recebedor",
        )
        .withColumn(
            "razao_saldo_residual",
            F.when(
                F.col("saldo_anterior_pagador") != 0,
                F.col("saldo_posterior_pagador")
                / F.col("saldo_anterior_pagador"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "proporcao_valor_recebedor",
            F.when(
                F.col("saldo_anterior_recebedor") != 0,
                F.col("valor_brl")
                / F.col("saldo_anterior_recebedor"),
            ).otherwise(F.lit(0.0)),
        )
        .agg(
            F.percentile_approx(
                "valor_brl",
                0.95,
            ).alias("p95_valor_brl"),

            F.percentile_approx(
                "razao_saldo_residual",
                0.95,
            ).alias("p95_razao_saldo_residual"),

            F.percentile_approx(
                "proporcao_valor_recebedor",
                0.95,
            ).alias("p95_proporcao_valor_recebedor"),
        )
        .first()
    )

    if metrics["p95_valor_brl"] is None:
        raise RuntimeError(
            "Não foi possível calcular thresholds: Bronze vazia."
        )

    return {
        "p95_valor_brl": float(metrics["p95_valor_brl"]),
        "p95_razao_saldo_residual": float(
            metrics["p95_razao_saldo_residual"]
        ),
        "p95_proporcao_valor_recebedor": float(
            metrics["p95_proporcao_valor_recebedor"]
        ),
    }


def publish_thresholds(
    spark,
    threshold_table: str,
    thresholds: dict,
) -> str:
    """
    Encerra o threshold ativo e publica uma nova versão.
    """

    calculated_at = datetime.now(timezone.utc)

    threshold_version = calculated_at.strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    # Encerra a versão atualmente ativa.
    if table_exists(spark, threshold_table):
        spark.sql(
            f"""
            UPDATE {threshold_table}
            SET valid_to = TIMESTAMP '{calculated_at.isoformat()}'
            WHERE valid_to IS NULL
            """
        )

    threshold_df = spark.createDataFrame(
        [
            (
                threshold_version,
                thresholds["p95_valor_brl"],
                thresholds["p95_razao_saldo_residual"],
                thresholds["p95_proporcao_valor_recebedor"],
                calculated_at,
                None,
            )
        ],
        """
        threshold_version string,
        p95_valor_brl double,
        p95_razao_saldo_residual double,
        p95_proporcao_valor_recebedor double,
        valid_from timestamp,
        valid_to timestamp
        """,
    )

    (
        threshold_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(threshold_table)
    )

    return threshold_version