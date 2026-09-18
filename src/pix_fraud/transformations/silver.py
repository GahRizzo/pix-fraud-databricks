from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pix_fraud.domain.risk_rules import add_risk_features


def transform_silver(
    df: DataFrame,
    p95_valor_brl: float,
    p95_razao_saldo_residual: float,
    p95_proporcao_valor_recebedor: float,
    threshold_version: str,
) -> DataFrame:
    result = add_risk_features(
        df,
        p95_valor_brl=p95_valor_brl,
        p95_razao_saldo_residual=p95_razao_saldo_residual,
        p95_proporcao_valor_recebedor=p95_proporcao_valor_recebedor,
    )

    return result.withColumn(
        "threshold_version",
        F.lit(threshold_version),
    )
