import argparse

from pyspark.sql import functions as F

from pix_fraud.transformations.bronze import transform_bronze
from pix_fraud.repositories.delta_repository import merge_delta, table_exists


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--source-path", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--control-table", required=True)

    return parser.parse_args()


args = parse_args()

SOURCE_PATH = args.source_path
BRONZE_TABLE = args.bronze_table
CONTROL_TABLE = args.control_table


# ---------------------------------------------------------------------------
# 1. Identifica os arquivos Parquet disponíveis na origem
# ---------------------------------------------------------------------------

files = [
    item.path
    for item in dbutils.fs.ls(SOURCE_PATH)
    if item.isFile() and item.path.lower().endswith(".parquet")
]


# ---------------------------------------------------------------------------
# 2. Identifica arquivos que já foram processados com sucesso
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 3. Mantém somente arquivos ainda não processados
# ---------------------------------------------------------------------------

new_files = sorted(set(files) - processed)

if not new_files:
    print("Nenhum arquivo novo para processar.")

else:
    print(f"Arquivos novos encontrados: {len(new_files)}")

    # -----------------------------------------------------------------------
    # 4. Lê somente os arquivos novos
    #
    # No Unity Catalog, input_file_name() não é suportado.
    # Por isso, capturamos _metadata.file_path no momento da leitura.
    # -----------------------------------------------------------------------

    source_df = (
        spark.read
        .format("parquet")
        .load(new_files)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
        )
    )


    # -----------------------------------------------------------------------
    # 5. Aplica as transformações da camada Bronze
    # -----------------------------------------------------------------------

    bronze_df = transform_bronze(source_df)


    # -----------------------------------------------------------------------
    # 6. MERGE idempotente utilizando transaction_id
    # -----------------------------------------------------------------------

    merge_delta(
        spark=spark,
        df=bronze_df,
        target_table=BRONZE_TABLE,
        key="transaction_id",
    )


    # -----------------------------------------------------------------------
    # 7. Registra os arquivos processados com sucesso
    # -----------------------------------------------------------------------

    control_df = (
        spark.createDataFrame(
            [(path, "SUCCESS") for path in new_files],
            "source_file string, status string",
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )

    (
        control_df
        .select(
            "source_file",
            "processed_at",
            "status",
        )
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(CONTROL_TABLE)
    )

    print(
        f"Processamento Bronze concluído com sucesso. "
        f"{len(new_files)} arquivo(s) processado(s)."
    )

