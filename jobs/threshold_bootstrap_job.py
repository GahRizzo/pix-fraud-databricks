import argparse

from pix_fraud.services.threshold_service import (
    calculate_thresholds,
    get_active_threshold,
    publish_thresholds,
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bronze-table",
        required=True,
    )

    parser.add_argument(
        "--threshold-table",
        required=True,
    )

    return parser.parse_args()


args = parse_args()

BRONZE_TABLE = args.bronze_table
THRESHOLD_TABLE = args.threshold_table


active_threshold = get_active_threshold(
    spark,
    THRESHOLD_TABLE,
)


if active_threshold is not None:

    print(
        "Threshold ativo já existe. "
        f"Versão: {active_threshold['threshold_version']}"
    )

    print(
        "Bootstrap não necessário."
    )

else:

    print(
        "Nenhum threshold ativo encontrado. "
        "Executando bootstrap."
    )

    bronze = spark.table(BRONZE_TABLE)

    thresholds = calculate_thresholds(bronze)

    print(
        "Thresholds calculados:",
        thresholds,
    )

    threshold_version = publish_thresholds(
        spark=spark,
        threshold_table=THRESHOLD_TABLE,
        thresholds=thresholds,
    )

    print(
        "Bootstrap concluído. "
        f"Threshold criado: {threshold_version}"
    )