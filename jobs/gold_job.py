import argparse

from pyspark.sql import functions as F

from pix_fraud.transformations.gold import (
    build_gold_risk,
    build_gold_operational,
    build_gold_alerts,
)
from pix_fraud.repositories.delta_repository import (
    get_watermark,
    merge_delta,
    set_watermark,
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-risk-table", required=True)
    parser.add_argument("--gold-operational-table", required=True)
    parser.add_argument("--gold-alerts-table", required=True)
    parser.add_argument("--watermark-table", required=True)
    parser.add_argument("--alert-score", type=int, default=3)

    return parser.parse_args()


args = parse_args()

SILVER_TABLE = args.silver_table
GOLD_RISK = args.gold_risk_table
GOLD_OPERATIONAL = args.gold_operational_table
GOLD_ALERTS = args.gold_alerts_table
WATERMARK_TABLE = args.watermark_table
ALERT_SCORE = args.alert_score


# ---------------------------------------------------------------------------
# 1. Recupera watermark da Gold
# ---------------------------------------------------------------------------

watermark = get_watermark(
    spark,
    WATERMARK_TABLE,
    "gold",
)


# ---------------------------------------------------------------------------
# 2. Lê estado atual da Silver
# ---------------------------------------------------------------------------

silver_full = spark.table(SILVER_TABLE)


# ---------------------------------------------------------------------------
# 3. Identifica registros novos desde a última execução da Gold
# ---------------------------------------------------------------------------

silver_incremental = silver_full

if watermark is not None:
    silver_incremental = silver_incremental.filter(
        F.col("ingestion_timestamp") > F.lit(watermark)
    )


# ---------------------------------------------------------------------------
# 4. Encerra caso não exista nenhum registro novo
# ---------------------------------------------------------------------------

if silver_incremental.limit(1).count() == 0:
    print("Nenhum registro novo para processar na Gold.")

else:

    # -----------------------------------------------------------------------
    # 5. Gold Risk
    #
    # Recalculada sobre todo o estado atual da Silver.
    # A tabela é pequena e o overwrite garante idempotência.
    # -----------------------------------------------------------------------

    risk_df = build_gold_risk(silver_full)

    (
        risk_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_RISK)
    )


    # -----------------------------------------------------------------------
    # 6. Gold Operational
    #
    # Mesmo princípio: snapshot agregado da Silver atual.
    # -----------------------------------------------------------------------

    operational_df = build_gold_operational(silver_full)

    (
        operational_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_OPERATIONAL)
    )


    # -----------------------------------------------------------------------
    # 7. Gold Alerts
    #
    # Alertas permanecem incrementais porque transaction_id fornece
    # uma chave idempotente para o MERGE.
    # -----------------------------------------------------------------------

    alerts_df = build_gold_alerts(
        silver_incremental,
        ALERT_SCORE,
    )

    merge_delta(
        spark=spark,
        df=alerts_df,
        target_table=GOLD_ALERTS,
        key="transaction_id",
    )


    # -----------------------------------------------------------------------
    # 8. Atualiza watermark somente depois de todas as Golds concluírem
    # -----------------------------------------------------------------------

    max_ingestion_timestamp = (
        silver_incremental
        .agg(F.max("ingestion_timestamp"))
        .first()[0]
    )

    if max_ingestion_timestamp is None:
        raise RuntimeError(
            "Não foi possível determinar o watermark da Gold."
        )

    set_watermark(
        spark,
        WATERMARK_TABLE,
        "gold",
        max_ingestion_timestamp,
    )

    print(
        "Processamento Gold concluído com sucesso. "
        f"Watermark atualizado para {max_ingestion_timestamp}."
    )
