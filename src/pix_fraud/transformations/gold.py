from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_gold_risk(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("nivel_risco")
        .agg(
            F.count("*").alias("transacoes"),
            F.sum("fraude").alias("fraudes"),
            F.sum("valor_brl").alias("valor_medio_soma"),
        )
    )


def build_gold_operational(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("dia_util", "horario_noturno")
        .agg(
            F.count("*").alias("transacoes"),
            F.sum("fraude").alias("fraudes"),
            F.sum("valor_brl").alias("faturamento"),
        )
    )


def build_gold_alerts(
    df: DataFrame,
    alert_score: int = 3,
) -> DataFrame:
    return (
        df.filter(F.col("score_risco") >= alert_score)
        .select(
            "transaction_id",
            "datetime_brasilia",
            "id_pagador",
            "id_recebedor",
            "valor_brl",
            "score_risco",
            "nivel_risco",
            "threshold_version",
        )
    )
