# Spark Lakehouse Lab

Ambiente local e autocontido para estudar **Data Lakehouse / Delta Lake**,
cobrindo os conceitos da certificação Databricks Data Engineer Associate.

Substitui o ambiente do curso original (que depende de MinIO remoto e
dados externos) por uma stack que roda inteira na sua máquina, sem
dependências de rede além do download inicial dos JARs.

## Stack

- **Spark 3.5.4** em modo Standalone (1 master + 2 workers + History Server)
- **MinIO** como S3 local (substitui o S3/MinIO remoto da demo original)
- **Delta Lake 3.2.0** carregado via `--packages` no `spark-submit`
- **Hadoop AWS 3.3.4** para conectar Spark ao MinIO via `s3a://`

## Pré-requisitos

- Docker + Docker Compose
- Python 3 no host (apenas para gerar os dados sintéticos)
- Internet na primeira execução (download de JARs do Maven Central)

## Setup inicial (uma vez)

```bash
# 1. Sobe Spark + MinIO + cria buckets
make up

# 2. Gera dados sintéticos UberEats e sobe pro bucket s3://raw-data/
make seed
```

Pronto. A partir daqui, `make up` / `make down` controla o ambiente.

### Serviços e UIs

| Serviço | URL | Credenciais |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker 1 UI | http://localhost:8081 | — |
| Spark Worker 2 UI | http://localhost:8082 | — |
| Spark App UI | http://localhost:4040 | (ativa só enquanto job roda) |
| Spark History Server | http://localhost:18080 | (jobs já finalizados) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## Rodando as demos

```bash
make demo1   # Fundamentos: criação, leitura, escrita, conversão Parquet→Delta
make demo2   # Operações: time travel, MERGE/upsert, OPTIMIZE/ZORDER, VACUUM
```

> **Primeira execução:** o `spark-submit` baixa Delta Lake + Hadoop-AWS
> + dependências do Maven Central. São ~80 JARs, leva 2-5 min. Não é
> travamento. Da segunda em diante o cache Ivy resolve em segundos.

## Estrutura

```
spark-lakehouse-lab/
├── docker-compose.yml          # Spark + MinIO + auto-criação de buckets
├── Makefile                    # Atalhos: make up, make seed, make demo1, make demo2
├── README.md
├── data/
│   └── generate_data.py        # Gera UberEats sintético, sobe pro MinIO
├── lib/
│   └── spark_session.py        # Factory de SparkSession (Delta + S3A + event log)
├── jobs/
│   └── mod4/
│       ├── demo1_delta_foundation.py    # Fundamentos do Delta
│       └── demo2_delta_operations.py    # Operações avançadas
└── spark-events/               # Event logs (alimentam o History Server)
```

A estrutura `jobs/mod4/` deixa pronto para `mod5/`, `mod6/`, etc.,
conforme o estudo avança.

## O que cada demo cobre

### Demo 1 — Delta Foundation
1. Criação de tabela via DataFrame API
2. Criação de tabela via Spark SQL (CTAS)
3. Read & write operations (overwrite, append, predicate pushdown)
4. Conversão Parquet → Delta in-place (`CONVERT TO DELTA`)
5. Exploração de metadados (`history()`, `DESCRIBE DETAIL`)

### Demo 2 — Delta Operations
1. **Time travel** — `VERSION AS OF` e `TIMESTAMP AS OF`
2. **MERGE / upsert** — insert + update atômico (base de pipelines incrementais)
3. **DELETE & UPDATE** — mutações com predicado, impossíveis em Parquet puro
4. **OPTIMIZE + ZORDER** — compactação de small files e data skipping
5. **VACUUM** — limpeza de arquivos órfãos e o trade-off com time travel

## Buckets

- **`s3a://raw-data/`** — dados crus, fontes "externas" (mysql, postgres, kafka, mongodb).
  É o que `make seed` popula.
- **`s3a://lakehouse/`** — onde as tabelas Delta vivem. As demos escrevem aqui.

## Comandos úteis

```bash
make help            # lista todos os comandos
make ps              # vê containers rodando
make logs            # streama logs (Ctrl+C pra sair)
make shell-master    # shell no container Spark master
make shell-minio     # shell no MinIO (com mc client)
make down            # para containers (mantém dados)
make nuke            # apaga TUDO (containers, volumes, dados)
```

## Diferenças vs. demo original do curso

| | Curso original | Este lab |
|---|---|---|
| Storage | MinIO remoto | MinIO local |
| Dados | Dataset externo | Gerados por `make seed` |
| Buckets | nomes do curso | `raw-data`, `lakehouse` |
| Spark | Cluster compartilhado | Cluster local (1 master + 2 workers) |
| Delta version | 2.4.0 | 3.2.0 (compatível com Spark 3.5.x) |

Os conceitos são idênticos: code paths, comandos SQL e comportamento
do `_delta_log/` espelham o que roda no Databricks.
