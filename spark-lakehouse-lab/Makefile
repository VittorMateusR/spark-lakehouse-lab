# ────────────────────────────────────────────────────────────────
# Atalhos para o lab. Roda assim: `make up`, `make demo1`, etc.
# Tudo passa por aqui pra você não precisar memorizar comandos
# longos de spark-submit.
# ────────────────────────────────────────────────────────────────

.PHONY: help up down restart logs ps seed shell-master shell-minio demo1 clean nuke

# Versões dos pacotes Spark. Centralizadas pra ser fácil atualizar.
DELTA_PKG    := io.delta:delta-spark_2.12:3.2.0
HADOOP_AWS   := org.apache.hadoop:hadoop-aws:3.3.4
PACKAGES     := $(DELTA_PKG),$(HADOOP_AWS)

# Spark Ivy precisa de um cache writable. Apontamos pro /tmp/.ivy2
# que casa com o volume mapeado no docker-compose.yml (sobrevive restart).
IVY_CONF     := -Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2

SUBMIT       := /opt/bitnami/spark/bin/spark-submit \
                  --master spark://spark-master:7077 \
                  --packages $(PACKAGES) \
                  --conf spark.driver.extraJavaOptions="$(IVY_CONF)" \
                  --conf spark.executor.extraJavaOptions="$(IVY_CONF)"

help:  ## Lista todos os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up:  ## Sobe o ambiente (Spark + MinIO + cria buckets)
	docker compose up -d
	@echo ""
	@echo "Ambiente no ar:"
	@echo "  Spark Master UI:  http://localhost:8080"
	@echo "  Spark Worker UI:  http://localhost:8081"
	@echo "  MinIO Console:    http://localhost:9001  (minioadmin/minioadmin)"
	@echo ""
	@echo "Próximo passo: make seed  (gera dados sintéticos no MinIO)"

down:  ## Para os containers (mantém volumes/dados)
	docker compose down

restart: down up  ## Reinicia o ambiente

logs:  ## Mostra logs de todos os serviços (Ctrl+C pra sair)
	docker compose logs -f

ps:  ## Lista containers rodando
	docker compose ps

seed:  ## Gera dados sintéticos UberEats e sobe pro MinIO
	@command -v python3 > /dev/null || (echo "python3 não encontrado no host" && exit 1)
	@python3 -c "import boto3" 2>/dev/null || pip install boto3
	python3 data/generate_data.py

shell-master:  ## Shell bash no container do Spark master
	docker exec -it spark-master bash

shell-minio:  ## Shell no MinIO (com mc client) — útil pra inspecionar buckets
	docker exec -it minio sh

# ─── Demos ─────────────────────────────────────────────────────
demo1:  ## Roda jobs/mod4/demo1_delta_foundation.py
	docker exec spark-master $(SUBMIT) /opt/spark-apps/jobs/mod4/demo1_delta_foundation.py

# ─── Limpeza ───────────────────────────────────────────────────
clean:  ## Remove containers (mantém volumes)
	docker compose down

nuke:  ## ⚠️  Apaga TUDO: containers, volumes, dados do MinIO, cache Ivy
	docker compose down -v
	@echo "Tudo limpo. Próximo 'make up' começa do zero."
