-- Executar uma vez por ambiente.
-- Ajuste catálogo/schema conforme o workspace.

CREATE SCHEMA IF NOT EXISTS pix_fraud_dev;
CREATE SCHEMA IF NOT EXISTS pix_fraud_prod;

-- Controle dos arquivos já ingeridos pela Bronze.
CREATE TABLE IF NOT EXISTS pix_fraud_dev.ingestion_control (
    source_file STRING,
    processed_at TIMESTAMP,
    status STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS pix_fraud_prod.ingestion_control (
    source_file STRING,
    processed_at TIMESTAMP,
    status STRING
) USING DELTA;

-- Watermark por camada. Permite que Silver e Gold leiam somente o lote novo.
CREATE TABLE IF NOT EXISTS pix_fraud_dev.layer_watermarks (
    layer_name STRING,
    last_ingestion_timestamp TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS pix_fraud_prod.layer_watermarks (
    layer_name STRING,
    last_ingestion_timestamp TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

-- Bronze e Silver são criadas pelo primeiro MERGE.
-- risk_thresholds é criado pelo create_thresholds_job.py.
