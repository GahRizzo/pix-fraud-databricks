# PIX Fraud — Databricks

Pipeline **batch incremental e idempotente** em Bronze, Silver e Gold, executado uma vez ao dia por Databricks Workflows.

## Arquitetura

```text
                    Databricks Workflow — 1 execução/dia
                                  |
                                  v
                       +----------------------+
                       |     Bronze Job       |
                       | novos arquivos       |
                       +----------+-----------+
                                  |
                                  v
                       +----------------------+
                       |      Silver Job      |
                       | novas transações     |
                       +----------+-----------+
                                  |
                                  v
                       +----------------------+
                       |       Gold Job       |
                       | novos agregados      |
                       +----------------------+
```

Há um Job lógico por medalhão. Cada execução processa somente o que chegou desde a execução anterior.

## Batch incremental explícito

O pipeline **não usa Structured Streaming nem Auto Loader**.

### Bronze

1. Lista os arquivos Parquet disponíveis na origem.
2. Consulta `ingestion_control` para identificar arquivos já processados com sucesso.
3. Lê somente os arquivos novos.
4. Calcula `transaction_id` determinístico.
5. Faz `MERGE` na Bronze.
6. Registra os arquivos como `SUCCESS` em `ingestion_control`.

Se a execução falhar antes do registro de controle, uma nova execução pode reler o arquivo; o `MERGE` por `transaction_id` mantém a operação idempotente.

### Silver

A tabela `layer_watermarks` guarda o maior `ingestion_timestamp` processado pela Silver. A cada execução, a Silver lê somente registros da Bronze posteriores ao watermark e faz `MERGE` por `transaction_id`.

### Gold

A Gold também usa watermark e processa somente o novo lote da Silver. As tabelas agregadas usam `MERGE` aditivo para somar as métricas do lote incremental. A tabela de alertas usa `MERGE` por `transaction_id`.

> A abordagem assume que os arquivos de origem são imutáveis após serem disponibilizados. Se arquivos existentes puderem ser alterados, o controle deve incluir versão/checksum do arquivo.

## Contrato Bronze

A Bronze contém os campos raw da transação e metadados técnicos:

- transaction_id
- id_pagador
- id_recebedor
- tipo_transacao
- valor_brl
- saldo_anterior_pagador
- saldo_posterior_pagador
- saldo_anterior_recebedor
- saldo_posterior_recebedor
- datetime_brasilia
- fraude
- source_file
- ingestion_timestamp
- ingestion_date

As colunas derivadas do dataset original não são carregadas para Bronze; elas são recalculadas em Silver.

## Execução diária

O Workflow está configurado para executar diariamente às 02:00 no fuso `America/Sao_Paulo`.

O intervalo pode ser alterado no arquivo `databricks/resources/pix_fraud_pipeline.yml`.

## Thresholds

Os thresholds estatísticos usados pelo risco são persistidos e versionados em `risk_thresholds`. Cada registro Silver guarda `threshold_version`.

O processo `create_thresholds_job.py` deve ser executado para criar/publicar uma versão de threshold antes da primeira execução da Silver.

## Observação

Os nomes de catálogo/schema e o caminho de origem são exemplos e devem ser ajustados ao workspace.
