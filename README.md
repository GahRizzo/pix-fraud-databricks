# PIX Fraud — Databricks

Pipeline de engenharia de dados para análise de risco em transações PIX, implementado em PySpark/Delta Lake e implantado com Databricks Asset Bundles (DAB). O processamento principal é **batch incremental e idempotente**, organizado nas camadas Bronze, Silver e Gold.

## Arquitetura

```text
Parquet em Unity Catalog Volume
            |
            v
+-----------------------+      diário 02:00 America/Sao_Paulo
| Bronze                |
| arquivos incrementais |
+-----------+-----------+
            |
            v
+-----------------------+
| Silver                |
| regras de risco       |
| + quality gate        |
+-----------+-----------+
            |
            v
+-----------------------+
| Gold                  |
| snapshots + alertas   |
+-----------------------+

Bronze completa
      |
      +----> Threshold Job semanal (domingo 03:00)
                    |
                    v
              risk_thresholds
              versionado
                    |
                    +----> utilizado pela Silver
```

O pipeline não utiliza Structured Streaming nem Auto Loader. A incrementalidade é explícita e controlada por arquivos processados e watermarks.

## Workflows

### `pix_fraud_daily_pipeline`

Executado diariamente às 02:00 (`America/Sao_Paulo`) com três tasks dependentes:

1. `bronze`
2. `silver`, após Bronze
3. `gold`, após Silver

### `pix_fraud_weekly_thresholds`

Executado aos domingos às 03:00. Recalcula os percentis de risco sobre o estado completo da Bronze e publica uma nova versão em `risk_thresholds`. A versão anterior recebe `valid_to`; a nova permanece ativa com `valid_to IS NULL`.

O Job semanal é independente do pipeline diário para evitar recalibração silenciosa dos critérios de risco em cada ingestão.

## Bronze

A Bronze representa os atributos próximos do dado cru e adiciona somente metadados técnicos. Colunas derivadas de regra de negócio são reconstruídas na Silver.

Campos esperados:

- `transaction_id`
- `id_pagador`
- `id_recebedor`
- `tipo_transacao`
- `valor_brl`
- `saldo_anterior_pagador`
- `saldo_posterior_pagador`
- `saldo_anterior_recebedor`
- `saldo_posterior_recebedor`
- `datetime_brasilia`
- `fraude`
- `source_file`
- `ingestion_timestamp`
- `ingestion_date`

O Job lista os Parquets no Volume, consulta `ingestion_control`, lê apenas arquivos ainda não marcados como `SUCCESS`, transforma os registros e executa `MERGE` por `transaction_id`. O caminho do arquivo é obtido por `_metadata.file_path`, compatível com Unity Catalog.

A abordagem assume que os arquivos disponibilizados na origem são imutáveis. Caso arquivos existentes possam ser alterados, o controle de ingestão deve evoluir para incluir versão ou checksum.

## Silver

A Silver lê somente registros da Bronze posteriores ao watermark `silver` em `layer_watermarks`. As regras de risco derivam atributos como horário, dia útil, razões de saldo, flags, `score_risco` e `nivel_risco`.

A execução utiliza a versão ativa de `risk_thresholds` e grava `threshold_version` em cada registro, permitindo rastrear quais parâmetros classificaram a transação.

### Quality gate

Antes do `MERGE` na Silver, `run_quality_checks` mede o batch Bronze → Silver e `validate_quality_checks` interrompe a execução quando encontra:

- divergência entre contagem Bronze e Silver;
- `fraude` nulo;
- `valor_brl <= 0`;
- `fraude` fora do domínio `{0, 1}`;
- `score_risco` nulo.

`fraud_rate` e `critical_rate` são métricas observacionais e não bloqueiam o pipeline. Se o quality gate falhar, a Silver não é persistida e o watermark não avança.

## Thresholds de risco

`create_thresholds_job.py` calcula semanalmente P95 para:

- valor da transação (`valor_brl`);
- razão de saldo residual do pagador;
- proporção entre valor da transação e saldo anterior do recebedor.

Cada calibração recebe uma `threshold_version` baseada no timestamp UTC da execução. A tabela mantém histórico por `valid_from` e `valid_to`; somente uma versão por ambiente deve permanecer ativa.

Para a primeira execução da Silver, `risk_thresholds` precisa conter uma versão ativa. O Job semanal pode ser executado manualmente uma vez para inicialização.

## Gold

A Gold usa o watermark `gold` para detectar se há novas transações na Silver.

- `gold_eficacia_risco_pix`: snapshot agregado recalculado sobre toda a Silver e gravado com `overwrite`.
- `gold_operacional`: snapshot agregado recalculado sobre toda a Silver e gravado com `overwrite`.
- `gold_alertas_pix`: somente novos alertas são processados e persistidos por `MERGE` idempotente em `transaction_id`.

O watermark da Gold só é atualizado depois que todas as saídas terminam com sucesso.

## Estrutura do repositório

```text
.
├── databricks.yml
├── databricks/
│   └── resources/
│       ├── pix_fraud_pipeline.yml
│       └── pix_fraud_thresholds.yml
├── ddl/
│   └── create_tables.sql
├── jobs/
│   ├── bronze_job.py
│   ├── silver_job.py
│   ├── gold_job.py
│   └── create_thresholds_job.py
├── src/pix_fraud/
│   ├── domain/
│   │   └── risk_rules.py
│   ├── quality/
│   │   └── quality_checks.py
│   ├── repositories/
│   │   └── delta_repository.py
│   └── transformations/
│       ├── bronze.py
│       ├── silver.py
│       └── gold.py
├── tests/
├── pyproject.toml
└── README.md
```

## Configuração atual de desenvolvimento

Os recursos DAB apontam para:

```text
Volume de entrada:
/Volumes/workspace/pix_fraud_dev/pix_ingestion/input/

Bronze:
workspace.pix_fraud_dev.bronze_pix_transacoes

Silver:
workspace.pix_fraud_dev.silver_pix_transacoes

Thresholds:
workspace.pix_fraud_dev.risk_thresholds

Controle de ingestão:
workspace.pix_fraud_dev.ingestion_control

Watermarks:
workspace.pix_fraud_dev.layer_watermarks
```

Os parâmetros de deployment estão centralizados nos arquivos em `databricks/resources/`. Os antigos YAMLs de configuração da aplicação foram removidos porque não participavam da execução atual.

## Deploy com DAB

O bundle está definido em `databricks.yml` e inclui todos os recursos de `databricks/resources/*.yml`. O pacote Python é construído como wheel e instalado no environment Serverless das tasks.

Fluxo esperado após uma alteração:

```text
Validate -> Deploy -> Run
```

Após mudanças no código de `src/`, faça novo deploy para que o wheel atualizado seja instalado no ambiente das tasks.

## Idempotência

- Bronze: controle por arquivo + `MERGE(transaction_id)`.
- Silver: watermark + `MERGE(transaction_id)`.
- Gold snapshots: reconstrução determinística + `overwrite`.
- Gold alertas: watermark + `MERGE(transaction_id)`.
- Thresholds: histórico versionado; cada execução publica uma nova calibração.

## Testes

Os testes unitários ficam em `tests/unit`. Execute localmente com:

```bash
pytest
```

## Próximas evoluções possíveis

- persistir resultados dos quality checks para observabilidade histórica;
- adicionar checksum/versionamento ao controle de arquivos da Bronze;
- parametrizar targets adicionais (`prod`) no DAB;
- adicionar testes Spark para transformações e quality gate;
- adicionar CI para testes, build e validação do bundle.
