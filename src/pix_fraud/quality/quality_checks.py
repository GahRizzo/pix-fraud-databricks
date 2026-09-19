from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_quality_checks(
    bronze: DataFrame,
    silver: DataFrame,
) -> dict:
    """
    Executa os quality checks do batch Bronze -> Silver.

    Retorna métricas de qualidade sem decidir se o pipeline
    deve ou não ser interrompido.
    """

    bronze_count = bronze.count()

    metrics = (
        silver
        .agg(
            F.count("*").alias("silver_count"),

            F.sum(
                F.when(F.col("fraude").isNull(), 1).otherwise(0)
            ).alias("fraude_nulls"),

            F.sum(
                F.when(F.col("valor_brl") <= 0, 1).otherwise(0)
            ).alias("invalid_values"),

            F.sum(
                F.when(
                    F.col("fraude").isNotNull()
                    & (~F.col("fraude").isin(0, 1)),
                    1,
                ).otherwise(0)
            ).alias("invalid_fraud"),

            F.sum(
                F.when(F.col("score_risco").isNull(), 1).otherwise(0)
            ).alias("missing_score"),

            F.avg(
                F.col("fraude").cast("double")
            ).alias("fraud_rate"),

            F.sum(
                F.when(
                    F.col("nivel_risco") == "CRITICO",
                    1,
                ).otherwise(0)
            ).alias("critical_count"),
        )
        .first()
    )

    silver_count = metrics["silver_count"] or 0
    critical_count = metrics["critical_count"] or 0

    fraud_rate = (
        float(metrics["fraud_rate"])
        if metrics["fraud_rate"] is not None
        else 0.0
    )

    critical_rate = (
        critical_count / silver_count
        if silver_count > 0
        else 0.0
    )

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "counts_match": bronze_count == silver_count,
        "fraude_nulls": metrics["fraude_nulls"] or 0,
        "invalid_values": metrics["invalid_values"] or 0,
        "invalid_fraud": metrics["invalid_fraud"] or 0,
        "missing_score": metrics["missing_score"] or 0,
        "fraud_rate": fraud_rate,
        "critical_rate": critical_rate,
    }


def validate_quality_checks(results: dict) -> None:
    """
    Quality gate da Silver.

    Lança RuntimeError quando algum check bloqueante falha.
    Métricas observacionais, como fraud_rate e critical_rate,
    não interrompem o pipeline.
    """

    errors = []

    if not results["counts_match"]:
        errors.append(
            "Contagem Bronze/Silver divergente: "
            f"bronze={results['bronze_count']}, "
            f"silver={results['silver_count']}."
        )

    if results["fraude_nulls"] > 0:
        errors.append(
            f"{results['fraude_nulls']} registro(s) "
            "com fraude NULL."
        )

    if results["invalid_values"] > 0:
        errors.append(
            f"{results['invalid_values']} registro(s) "
            "com valor_brl <= 0."
        )

    if results["invalid_fraud"] > 0:
        errors.append(
            f"{results['invalid_fraud']} registro(s) "
            "com fraude fora do domínio {0, 1}."
        )

    if results["missing_score"] > 0:
        errors.append(
            f"{results['missing_score']} registro(s) "
            "sem score_risco."
        )

    if errors:
        raise RuntimeError(
            "Quality gate da Silver falhou:\n- "
            + "\n- ".join(errors)
        )
