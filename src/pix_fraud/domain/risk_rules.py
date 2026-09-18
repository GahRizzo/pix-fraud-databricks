from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_risk_features(
    df: DataFrame,
    p95_valor_brl: float,
    p95_razao_saldo_residual: float,
    p95_proporcao_valor_recebedor: float,
) -> DataFrame:

    df = (
        df
        .withColumn("hora_dia", F.hour("datetime_brasilia"))
        .withColumn("dia_semana", F.dayofweek("datetime_brasilia"))
        .withColumn(
            "dia_util",
            F.dayofweek("datetime_brasilia").between(2, 6),
        )
        .withColumn(
            "horario_noturno",
            (F.col("hora_dia") <= 5) | (F.col("hora_dia") >= 22),
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
    )

    return (
        df
        .withColumn(
            "flag_horario_noturno",
            F.col("horario_noturno").cast("int"),
        )
        .withColumn(
            "flag_limite_noturno",
            (
                F.col("horario_noturno")
                & (F.col("valor_brl") > F.lit(p95_valor_brl))
            ).cast("int"),
        )
        .withColumn(
            "flag_valor_alto",
            (F.col("valor_brl") > F.lit(p95_valor_brl)).cast("int"),
        )
        .withColumn(
            "flag_saldo_anormal",
            (
                F.col("razao_saldo_residual")
                > F.lit(p95_razao_saldo_residual)
            ).cast("int"),
        )
        .withColumn(
            "flag_recebedor_anormal",
            (
                F.col("proporcao_valor_recebedor")
                > F.lit(p95_proporcao_valor_recebedor)
            ).cast("int"),
        )
        .withColumn(
            "score_risco",
            F.col("flag_horario_noturno")
            + F.col("flag_limite_noturno")
            + F.col("flag_valor_alto")
            + F.col("flag_saldo_anormal")
            + F.col("flag_recebedor_anormal"),
        )
        .withColumn(
            "nivel_risco",
            F.when(F.col("score_risco") >= 4, F.lit("CRITICO"))
            .when(F.col("score_risco") >= 3, F.lit("ALTO"))
            .when(F.col("score_risco") >= 2, F.lit("MEDIO"))
            .when(F.col("score_risco") >= 1, F.lit("BAIXO"))
            .otherwise(F.lit("NORMAL")),
        )
    )
