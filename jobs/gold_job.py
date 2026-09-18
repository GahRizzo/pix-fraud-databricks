from pyspark.sql import functions as F

from pix_fraud.transformations.gold import (
    build_gold_risk,
    build_gold_operational,
    build_gold_alerts,
)
from pix_fraud.repositories.delta_repository import (
    get_watermark,
    merge_delta,
    merge_delta_additive,
    set_watermark,
)


SILVER_TABLE = spark.conf.get(
    "pix_fraud.silver_table", "pix_fraud_dev.silver_pix_transacoes"
)
GOLD_RISK = spark.conf.get(
    "pix_fraud.gold_risk_table", "pix_fraud_dev.gold_eficacia_risco_pix"
)
GOLD_OPERATIONAL = spark.conf.get(
    "pix_fraud.gold_operational_table", "pix_fraud_dev.gold_operacional"
)
GOLD_ALERTS = spark.conf.get(
    "pix_fraud.gold_alerts_table", "pix_fraud_dev.gold_alertas_pix"
)
WATERMARK_TABLE = spark.conf.get(
    "pix_fraud.watermark_table", "pix_fraud_dev.layer_watermarks"
)
ALERT_SCORE = int(spark.conf.get("pix_fraud.alert_score", "3"))

watermark = get_watermark(spark, WATERMARK_TABLE, "gold")
silver = spark.table(SILVER_TABLE)
if watermark is not None:
    silver = silver.filter(F.col("ingestion_timestamp") > F.lit(watermark))

if silver.limit(1).count() == 0:
    print("Nenhum registro novo para Gold.")
else:
    risk = build_gold_risk(silver)
    merge_delta_additive(
        spark, risk, GOLD_RISK,
        keys=["nivel_risco"],
        additive_columns=["transacoes", "fraudes", "valor_medio_soma"],
    )

    operational = build_gold_operational(silver)
    merge_delta_additive(
        spark, operational, GOLD_OPERATIONAL,
        keys=["dia_util", "horario_noturno"],
        additive_columns=["transacoes", "fraudes", "faturamento"],
    )

    alerts = build_gold_alerts(silver, ALERT_SCORE)
    merge_delta(
        spark=spark,
        df=alerts,
        target_table=GOLD_ALERTS,
        key="transaction_id",
    )

    max_ingestion = silver.agg(F.max("ingestion_timestamp")).first()[0]
    set_watermark(spark, WATERMARK_TABLE, "gold", max_ingestion)
