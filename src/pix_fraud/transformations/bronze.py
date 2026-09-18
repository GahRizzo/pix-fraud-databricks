from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_COLUMNS = [
    "transaction_id",
    "id_pagador",
    "id_recebedor",
    "tipo_transacao",
    "valor_brl",
    "saldo_anterior_pagador",
    "saldo_posterior_pagador",
    "saldo_anterior_recebedor",
    "saldo_posterior_recebedor",
    "datetime_brasilia",
    "fraude",
    "source_file",
    "ingestion_timestamp",
    "ingestion_date",
]


def add_transaction_id(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "transaction_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("id_pagador"),
                F.col("id_recebedor"),
                F.col("tipo_transacao"),
                F.col("datetime_brasilia").cast("string"),
                F.col("valor_brl").cast("string"),
            ),
            256,
        ),
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
    )


def transform_bronze(df: DataFrame) -> DataFrame:
    return (
        add_ingestion_metadata(add_transaction_id(df))
        .select(*BRONZE_COLUMNS)
    )
