-- Objetos de controle. Bronze, Silver e Gold são criadas pelos próprios Jobs.
-- Ajuste catálogo/schema para cada ambiente.

CREATE SCHEMA IF NOT EXISTS workspace.pix_fraud_dev;

CREATE TABLE IF NOT EXISTS workspace.pix_fraud_dev.ingestion_control (
    source_file STRING,
    processed_at TIMESTAMP,
    status STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.pix_fraud_dev.layer_watermarks (
    layer_name STRING,
    last_ingestion_timestamp TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

-- risk_thresholds é criada automaticamente pelo primeiro run bem-sucedido
-- de jobs/create_thresholds_job.py.
