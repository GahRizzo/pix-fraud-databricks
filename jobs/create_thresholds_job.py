import argparse

from pix_fraud.services.threshold_service import (
    calculate_thresholds,
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


print(
    f"Iniciando recalibração de thresholds usando {BRONZE_TABLE}"
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
    f"Nova versão de threshold publicada: {threshold_version}"
)