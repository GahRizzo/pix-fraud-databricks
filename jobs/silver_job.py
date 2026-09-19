import argparse

from pyspark.sql import functions as F

from pix_fraud.transformations.silver import transform_silver
from pix_fraud.repositories.delta_repository import (
    get_watermark,
    merge_delta,
    set_watermark,
)
from pix_fraud.quality.quality_checks import (
    run_quality_checks,
    validate_quality_checks,
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--threshold-table", required=True)
    parser.add_argument("--watermark-table", required=True)

    return parser.parse_args()


args = parse_args()

BRONZE_TABLE = args.bronze_table
SILVER_TABLE = args.silver_table
THRESHOLD_TABLE = args.threshold_table
WATERMARK_TABLE = args.watermark_table


# ---------------------------------------------------------------------------
# 1. Recupera a versão ativa dos thresholds
# ---------------------------------------------------------------------------

threshold = (
    spark.table(THRESHOLD_TABLE)
    .filter(F.col("valid_to").isNull())
    .orderBy(F.col("valid_from").desc())
    .limit(1)
    .first()
)

if not threshold:
    raise RuntimeError(
        f"Nenhum threshold ativo encontrado em {THRESHOLD_TABLE}"
    )


# ---------------------------------------------------------------------------
# 2. Recupera o último watermark processado pela Silver
# ---------------------------------------------------------------------------

watermark = get_watermark(
    spark,
    WATERMARK_TABLE,
    "silver",
)


# ---------------------------------------------------------------------------
# 3. Lê a Bronze incrementalmente
# ---------------------------------------------------------------------------

bronze_df = spark.table(BRONZE_TABLE)

if watermark is not None:
    bronze_df = bronze_df.filter(
        F.col("ingestion_timestamp") > F.lit(watermark)
    )


# ---------------------------------------------------------------------------
# 4. Verifica se existem registros novos
# ---------------------------------------------------------------------------

if bronze_df.limit(1).count() == 0:
    print("Nenhum registro novo para processar na Silver.")

else:
    # -----------------------------------------------------------------------
    # 5. Aplica as transformações e regras da Silver
    # -----------------------------------------------------------------------

    silver_df = transform_silver(
        bronze_df,
        p95_valor_brl=threshold["p95_valor_brl"],
        p95_razao_saldo_residual=threshold[
            "p95_razao_saldo_residual"
        ],
        p95_proporcao_valor_recebedor=threshold[
            "p95_proporcao_valor_recebedor"
        ],
        threshold_version=threshold["threshold_version"],
    )


    # ---------------------------------------------------------------------------
    # 6. Quality Gate
    # ---------------------------------------------------------------------------

    quality_results = run_quality_checks(
        bronze=bronze_df,
        silver=silver_df,
    )

    print("Resultado dos quality checks:")
    print(quality_results)

    validate_quality_checks(quality_results)


    # -----------------------------------------------------------------------
    # 7. MERGE idempotente utilizando transaction_id
    # -----------------------------------------------------------------------

    merge_delta(
        spark=spark,
        df=silver_df,
        target_table=SILVER_TABLE,
        key="transaction_id",
    )


    # -----------------------------------------------------------------------
    # 8. Obtém o maior ingestion_timestamp processado
    # -----------------------------------------------------------------------

    max_ingestion_timestamp = (
        silver_df
        .agg(F.max("ingestion_timestamp"))
        .first()[0]
    )

    if max_ingestion_timestamp is None:
        raise RuntimeError(
            "Não foi possível determinar o watermark da Silver."
        )


    # -----------------------------------------------------------------------
    # 9. Atualiza o watermark somente após o MERGE bem-sucedido
    # -----------------------------------------------------------------------

    set_watermark(
        spark,
        WATERMARK_TABLE,
        "silver",
        max_ingestion_timestamp,
    )

    print(
        "Processamento Silver concluído com sucesso. "
        f"Watermark atualizado para {max_ingestion_timestamp}."
    )
