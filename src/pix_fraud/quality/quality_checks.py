from pyspark.sql import functions as F


def run_quality_checks(bronze, silver) -> dict:
    bronze_count = bronze.count()
    silver_count = silver.count()

    fraud_nulls = silver.filter(F.col("fraude").isNull()).count()
    invalid_values = silver.filter(F.col("valor_brl") <= 0).count()
    invalid_fraud = silver.filter(~F.col("fraude").isin(0, 1)).count()
    missing_score = silver.filter(F.col("score_risco").isNull()).count()

    fraud_rate = (
        silver.agg(F.avg(F.col("fraude"))).first()[0]
        if silver_count
        else 0.0
    )

    critical_count = silver.filter(
        F.col("nivel_risco") == "CRITICO"
    ).count()

    critical_rate = (
        critical_count / silver_count
        if silver_count
        else 0.0
    )

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "counts_match": bronze_count == silver_count,
        "fraude_nulls": fraud_nulls,
        "invalid_values": invalid_values,
        "invalid_fraud": invalid_fraud,
        "missing_score": missing_score,
        "fraud_rate": fraud_rate,
        "critical_rate": critical_rate,
    }
