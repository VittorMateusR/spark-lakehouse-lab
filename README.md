# Spark Lakehouse Lab

Ambiente local para estudar o **Módulo 4** do curso (Data Lakehouse / Delta Lake / Iceberg).
Substitui o ambiente do curso original (que depende de MinIO remoto e dados externos)
por uma stack autocontida que roda na sua máquina.

## Stack

- **Spark 3.5.1** (master + 1 worker, modo Standalone)
- **MinIO** como S3 local (substitui o S3/MinIO remoto da demo do curso)
- **Delta Lake 3.2.0** carregado via `--packages` no `spark-submit`
- **Hadoop AWS 3.3.4** para conectar Spark ao MinIO via `s3a://`

## Pré-requisitos

- Docker + Docker Compose
- Python 3 no host (apenas para gerar os dados sintéticos)
- Internet na primeira execução (download de JARs do Maven Central)

## Setup inicial (uma vez)

```bash
# 1. Sobe Spark + MinIO
make up

# 2. Gera dados sintéticos UberEats e sobe pro bucket s3://raw-data/
make seed
```

Pronto. A partir daqui, `make up` / `make down` controla o ambiente.

### O que aparece

| Serviço | URL | Credenciais |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Spark App UI | http://localhost:4040 | (só ativa enquanto job roda) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## Rodando uma demo

```bash
make demo1
```

> **Primeira execução:** o `spark-submit` baixa Delta Lake + Hadoop-AWS
> + dependências do Maven Central. São ~80 JARs, leva 2-5 min. Não é
> travamento. Da segunda em diante o cache Ivy resolve em segundos.

## Estrutura

```
spark-lakehouse-lab/
├── docker-compose.yml          # Spark + MinIO + auto-criação de buckets
├── Makefile                    # Atalhos: make up, make seed, make demo1
├── README.md
├── data/
│   └── generate_data.py        # Gera UberEats sintético, sobe pro MinIO
├── lib/
│   └── spark_session.py        # Factory de SparkSession (Delta + S3A)
└── jobs/
    └── mod4/
        └── demo1_delta_foundation.py
```

A estrutura `jobs/mod4/` deixa pronto para `mod5/`, `mod6/`, etc., quando
chegarmos lá. **Por enquanto, só Módulo 4.**

## Fluxo de estudo recomendado

1. Assista a aula teórica
2. Mande o resumo aqui no chat (protocolo pós-aula)
3. Quando tiver demo correspondente, rode `make demo<N>` e observe:
   - Logs do `spark-submit` no terminal
   - MinIO Console (http://localhost:9001) para ver arquivos `_delta_log/` aparecendo
   - Spark UI (http://localhost:4040) para ver o job sendo executado
4. Mande o que observou aqui (protocolo pós-demo)

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
| Storage | MinIO remoto (`24.144.65.249`) | MinIO local |
| Dados | `owshq-shadow-traffic-uber-eats` (externo) | Gerados por `make seed` |
| Buckets | `owshq-uber-eats-lakehouse` | `lakehouse` |
| Spark | Cluster compartilhado | Cluster local controlado por você |
| Delta version | 2.4.0 | 3.2.0 (mais recente, compatível com Spark 3.5.1) |

**Os conceitos são idênticos.** Code paths, comandos SQL, comportamento
do `_delta_log/` — tudo igual ao Databricks.
