"""
Demo 1 — Delta Lake Foundation & Setup
=======================================

Cobre os 5 conceitos que o curso original cobre na Demo 1:

  1. table_creation_api      → criar tabela Delta via DataFrame API
  2. table_creation_sql      → criar tabela Delta via SQL DDL
  3. read_write_operations   → modos de escrita (overwrite, append) + leitura
  4. parquet_to_delta        → converter Parquet existente pra Delta
  5. metadata_exploration    → inspecionar histórico e metadados

Por padrão, o main() roda APENAS a função 5 (igual à demo original do
curso que vem com 1-4 comentadas). Descomente as outras conforme você
for estudando — cada função é independente e pode rodar isolada.

Como rodar:
    docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
      --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp \\
      /opt/spark-apps/jobs/mod4/demo1_delta_foundation.py

Atalho equivalente:
    make demo1
"""

import sys

# Deixa o Python achar lib/spark_session.py (montada em /opt/spark-apps/lib)
sys.path.insert(0, "/opt/spark-apps")

from pyspark.sql.functions import col
from delta.tables import DeltaTable

from lib.spark_session import get_spark


# ─── Paths centralizados ───────────────────────────────────────
# raw-data/   = bucket "bronze de bronze", dados crus vindos das fontes
# lakehouse/  = bucket onde vivem nossas tabelas Delta
RAW = "s3a://raw-data"
LK = "s3a://lakehouse"


# ─────────────────────────────────────────────────────────────
# 1. TABLE CREATION VIA DATAFRAME API
# Conceito: criar Delta programaticamente, sem SQL.
# Use quando: você está num pipeline ETL e já tem um DataFrame.
# ─────────────────────────────────────────────────────────────
def table_creation_api(spark):
    print("\n" + "=" * 60)
    print("1. TABLE CREATION VIA DATAFRAME API")
    print("=" * 60)

    src = f"{RAW}/mysql/restaurants/restaurants.json"
    restaurants = spark.read.json(src)
    print(f"Lidos {restaurants.count()} restaurantes de {src}")
    restaurants.show(3, truncate=False)

    dst = f"{LK}/bronze/restaurants"

    # MODE OVERWRITE: substitui o conteúdo da tabela inteira.
    # overwriteSchema=true permite substituir o schema também (caso
    # tenha mudado). Sem isso, schema mismatch quebra a escrita.
    (restaurants.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(dst))

    # Re-leitura confirma que a Delta foi criada e é legível.
    out = spark.read.format("delta").load(dst)
    print(f"Tabela Delta criada em {dst} com {out.count()} registros")
    out.select("name", "cuisine_type", "city", "average_rating").show(5)


# ─────────────────────────────────────────────────────────────
# 2. TABLE CREATION VIA SPARK SQL
# Conceito: criar Delta via DDL, igual a um banco tradicional.
# Use quando: você está num notebook, query interativa, ou tem
# pessoas SQL-first no time.
# ─────────────────────────────────────────────────────────────
def table_creation_sql(spark):
    print("\n" + "=" * 60)
    print("2. TABLE CREATION VIA SPARK SQL")
    print("=" * 60)

    src = f"{RAW}/postgres/drivers/drivers.json"
    drivers = spark.read.json(src)
    # Cria uma view temporária pra usar em SQL.
    drivers.createOrReplaceTempView("vw_drivers")

    dst = f"{LK}/bronze/drivers"

    # CREATE OR REPLACE TABLE ... USING DELTA ... AS SELECT é o
    # equivalente Delta do CTAS (Create Table As Select).
    # LOCATION torna ela uma "external table": os dados vivem onde
    # você apontar, não num warehouse default.
    spark.sql(f"""
        CREATE OR REPLACE TABLE drivers
        USING DELTA
        LOCATION '{dst}'
        AS SELECT
            driver_id,
            first_name,
            last_name,
            license_number,
            vehicle_type,
            vehicle_make,
            vehicle_model,
            vehicle_year,
            city,
            country,
            date_birth,
            phone_number,
            uuid,
            dt_current_timestamp
        FROM vw_drivers
    """)

    # Agregação de exemplo: drivers por tipo de veículo.
    print("Drivers por tipo de veículo:")
    spark.sql("""
        SELECT
            vehicle_type,
            COUNT(*) AS driver_count,
            ROUND(AVG(vehicle_year), 1) AS avg_vehicle_year
        FROM drivers
        GROUP BY vehicle_type
        ORDER BY driver_count DESC
    """).show()


# ─────────────────────────────────────────────────────────────
# 3. READ & WRITE OPERATIONS
# Conceito: modos de escrita (overwrite, append, errorIfExists, ignore)
# e como Delta lida com isso. Append é o que você usa em ingestão diária.
# ─────────────────────────────────────────────────────────────
def read_write_operations(spark):
    print("\n" + "=" * 60)
    print("3. READ & WRITE OPERATIONS")
    print("=" * 60)

    src = f"{RAW}/kafka/orders/orders.json"
    orders = spark.read.json(src)
    dst = f"{LK}/bronze/orders"

    # Primeiro: overwrite (escrita inicial)
    print("Escrevendo orders com mode=overwrite...")
    (orders.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(dst))

    initial = spark.read.format("delta").load(dst).count()
    print(f"  count inicial: {initial}")

    # Append: simula nova batelada de dados (10 orders com valor +100)
    print("Append de 10 novos orders (com total_amount + 100)...")
    new_orders = orders.limit(10).withColumn("total_amount", col("total_amount") + 100)
    (new_orders.write
        .format("delta")
        .mode("append")
        .save(dst))

    final = spark.read.format("delta").load(dst).count()
    print(f"  count após append: {final}  (delta = {final - initial})")

    # Filtro com predicate pushdown.
    # IMPORTANTE: o pushdown opera no nível do row group dentro dos
    # arquivos Parquet (via min/max stats no transaction log). Isso é
    # o que faz Delta ser rápido em filtros.
    big = spark.read.format("delta").load(dst).filter(col("total_amount") > 100)
    print(f"  orders com total_amount > 100: {big.count()}")


# ─────────────────────────────────────────────────────────────
# 4. PARQUET → DELTA CONVERSION
# Conceito: você herda um data lake em Parquet. Como migra pra Delta
# sem reescrever os dados? CONVERT TO DELTA é a resposta.
# ─────────────────────────────────────────────────────────────
def parquet_to_delta(spark):
    print("\n" + "=" * 60)
    print("4. PARQUET → DELTA CONVERSION")
    print("=" * 60)

    src = f"{RAW}/mongodb/users/users.json"
    users = spark.read.json(src)

    # Passo 1: criar uma tabela Parquet "legada" pra ter algo a converter
    parquet_path = f"{LK}/parquet/users"
    print(f"Criando tabela Parquet em {parquet_path}...")
    users.write.format("parquet").mode("overwrite").save(parquet_path)

    # Passo 2: CONVERT TO DELTA — conversão IN-PLACE.
    # NÃO reescreve os dados. Só cria o _delta_log/ ao lado dos
    # arquivos Parquet existentes. Operação atômica e barata.
    print("Convertendo Parquet → Delta IN-PLACE (CONVERT TO DELTA)...")
    spark.sql(f"CONVERT TO DELTA parquet.`{parquet_path}`")

    # Validação: o mesmo path agora é uma Delta table.
    delta_users = spark.read.format("delta").load(parquet_path)
    print(f"  registros após conversão: {delta_users.count()}")
    delta_users.select("user_id", "email", "city", "country").show(5)

    # Método alternativo: criar Delta em LOCATION nova via CTAS.
    # Esse SIM copia os dados. Use quando quer separar physical layout.
    new_path = f"{LK}/bronze/users"
    print(f"\nMétodo alternativo: CTAS pra novo path {new_path}...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE users_new
        USING DELTA
        LOCATION '{new_path}'
        AS SELECT * FROM delta.`{parquet_path}`
    """)
    print(f"  Delta em {new_path} pronta")


# ─────────────────────────────────────────────────────────────
# 5. METADATA EXPLORATION
# Conceito: o transaction log (_delta_log/) é o coração do Delta.
# .history() lê esse log e mostra todas as operações já feitas na tabela.
# DESCRIBE DETAIL mostra schema, location, num arquivos, tamanho, etc.
# ─────────────────────────────────────────────────────────────
def metadata_exploration(spark):
    print("\n" + "=" * 60)
    print("5. METADATA EXPLORATION")
    print("=" * 60)

    path = f"{LK}/bronze/restaurants"

    print(f"Explorando metadados de {path}\n")

    # .history() lê o transaction log e mostra cada commit.
    # Cada linha = 1 versão da tabela. Esse é o mecanismo que viabiliza
    # time travel (Aula #11).
    print("Histórico (transaction log):")
    delta_table = DeltaTable.forPath(spark, path)
    (delta_table.history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .show(truncate=False))

    print("Detalhes da tabela:")
    spark.sql(f"DESCRIBE DETAIL delta.`{path}`").show(truncate=False)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    spark = get_spark("Demo-1-Delta-Foundation")

    # Comentado por padrão pra você descomentar conforme estudar.
    # Recomendação: rode 1, depois 2, etc., observando o MinIO console
    # (http://localhost:9001) entre cada etapa pra ver os arquivos
    # aparecendo (especialmente _delta_log/).
    table_creation_api(spark)
    table_creation_sql(spark)
    read_write_operations(spark)
    parquet_to_delta(spark)
    metadata_exploration(spark)

    spark.stop()


if __name__ == "__main__":
    main()
