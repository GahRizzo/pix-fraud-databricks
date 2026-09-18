from pyspark.sql import functions as F

from pix_fraud.transformations.bronze import transform_bronze
from pix_fraud.repositories.delta_repository import merge_delta, table_exists

import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--source-path", required=True)
parser.add_argument("--bronze-table", required=True)
parser.add_argument("--control-table", required=True)

args = parser.parse_args()

SOURCE_PATH = args.source_path
BRONZE_TABLE = args.bronze_table
CONTROL_TABLE = args.control_table


# Batch incremental explícito:
# 1. lista os arquivos disponíveis na origem;
# 2. remove os que já foram processados com sucesso;
# 3. lê somente os arquivos novos;
# 4. faz MERGE por transaction_id;
# 5. registra os arquivos processados.
files = [
    item.path
    for item in dbutils.fs.ls(SOURCE_PATH)
    if item.isFile() and item.path.lower().endswith(".parquet")
]

processed = set()
if table_exists(spark, CONTROL_TABLE):
    processed = {
        row["source_file"]
        for row in (
            spark.table(CONTROL_TABLE)
            .filter(F.col("status") == "SUCCESS")
            .select("source_file")
            .distinct()
            .collect()
        )
    }

new_files = sorted(set(files) - processed)

if not new_files:
    print("Nenhum arquivo novo para processar.")
else:
    source_df = spark.read.parquet(*new_files)
    bronze_df = transform_bronze(source_df).cache()

    merge_delta(
        spark=spark,
        df=bronze_df,
        target_table=BRONZE_TABLE,
        key="transaction_id",
    )

    control_df = spark.createDataFrame(
        [(path, "SUCCESS") for path in new_files],
        "source_file string, status string",
    ).withColumn("processed_at", F.current_timestamp())

    (
        control_df
        .select("source_file", "processed_at", "status")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(CONTROL_TABLE)
    )

    bronze_df.unpersist()
