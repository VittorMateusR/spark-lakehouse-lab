"""
Demo 2 — Delta Lake Operations: Time Travel, MERGE, OPTIMIZE, VACUUM
"""
import sys

sys.path.insert(0, "/opt/spark-apps")

from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

from lib.spark_session import get_spark


RAW = "s3a://raw-data"
LK = "s3a://lakehouse"

ORDERS = f"{LK}/bronze/orders"


# ─────────────────────────────────────────────────────────────
# Setup auxiliar: garante que bronze/orders existe antes das demos.
# Idempotente — se a Demo 1 já criou, só confirma. Se não, cria.
# ─────────────────────────────────────────────────────────────
def ensure_orders(spark):
    try:
        cnt = spark.read.format("delta").load(ORDERS).count()
        print(f"Tabela orders já existe ({cnt} registros). Seguindo.")
    except Exception:
        print("Tabela orders não encontrada. Criando a partir do raw...")
        orders = spark.read.json(f"{RAW}/kafka/orders/orders.json")
        (orders.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(ORDERS))
        print(f"  orders criada com {orders.count()} registros")


# ─────────────────────────────────────────────────────────────
# 1. TIME TRAVEL
# Conceito: cada commit no _delta_log/ é uma versão imutável. Você
# consegue ler qualquer versão passada por número (VERSION AS OF) ou
# por momento no tempo (TIMESTAMP AS OF).
# Use quando: auditoria, reprodução de relatório antigo, rollback,
# debugging de "o número mudou, o que aconteceu entre ontem e hoje?".
# ─────────────────────────────────────────────────────────────
def time_travel(spark):
    print("\n" + "=" * 60)
    print("1. TIME TRAVEL")
    print("=" * 60)

    # Geramos uma segunda versão fazendo um append, pra ter o que viajar.
    base = spark.read.format("delta").load(ORDERS)
    v0_count = base.count()
    print(f"Versão atual tem {v0_count} registros")

    extra = base.limit(5).withColumn("order_status", lit("time_travel_test"))
    extra.write.format("delta").mode("append").save(ORDERS)
    v_latest = spark.read.format("delta").load(ORDERS).count()
    print(f"Após append de 5: {v_latest} registros")

    # Lê o histórico pra descobrir quais versões existem.
    dt = DeltaTable.forPath(spark, ORDERS)
    print("\nHistórico de versões:")
    dt.history().select("version", "timestamp", "operation").show(truncate=False)

    # VERSION AS OF: lê a tabela como ela estava na versão 0.
    # Note que voltamos ao count original — a versão 0 não "sabe"
    # do append que veio depois.
    v0 = (spark.read.format("delta")
          .option("versionAsOf", 0)
          .load(ORDERS))
    print(f"Lendo VERSION AS OF 0: {v0.count()} registros (esperado: {v0_count})")

    # Equivalente em SQL — é a forma que mais aparece na prova.
    print("\nMesma coisa via SQL (VERSION AS OF 0):")
    spark.sql(f"""
        SELECT COUNT(*) AS registros_v0
        FROM delta.`{ORDERS}` VERSION AS OF 0
    """).show()


# ─────────────────────────────────────────────────────────────
# 2. MERGE / UPSERT
# Conceito: MERGE INTO combina insert + update + delete numa operação
# atômica, comparando uma tabela alvo com uma fonte por uma condição.
# É o coração de qualquer pipeline incremental (CDC, SCD, dedup).
# Use quando: chegou um lote novo e você precisa atualizar linhas que
# já existem E inserir as que não existem, sem duplicar.
# ─────────────────────────────────────────────────────────────
def merge_upsert(spark):
    print("\n" + "=" * 60)
    print("2. MERGE / UPSERT")
    print("=" * 60)

    target = DeltaTable.forPath(spark, ORDERS)

    # Monta uma fonte de "atualizações": pega 3 orders existentes e
    # muda o status, e inventa 2 orders novos (ids que não existem).
    existing = (spark.read.format("delta").load(ORDERS)
                .select("order_id").limit(3))
    existing_ids = [r["order_id"] for r in existing.collect()]
    print(f"IDs que serão atualizados: {existing_ids}")

    updates = spark.createDataFrame(
        [
            (existing_ids[0], 1, 1, 1, 999.0, "MERGED_UPDATE", "2025-06-01T00:00:00", "pix"),
            (existing_ids[1], 1, 1, 1, 999.0, "MERGED_UPDATE", "2025-06-01T00:00:00", "pix"),
            (existing_ids[2], 1, 1, 1, 999.0, "MERGED_UPDATE", "2025-06-01T00:00:00", "pix"),
            (99991, 2, 2, 2, 50.0, "MERGED_INSERT", "2025-06-01T00:00:00", "cash"),
            (99992, 2, 2, 2, 60.0, "MERGED_INSERT", "2025-06-01T00:00:00", "cash"),
        ],
        ["order_id", "user_id", "restaurant_id", "driver_id",
         "total_amount", "order_status", "order_date", "payment_method"],
    )

    # A operação: quando o order_id casa, atualiza; quando não casa
    # (registro só existe na fonte), insere. Tudo num commit atômico.
    (target.alias("t")
        .merge(updates.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    print("\nMERGE executado. Conferindo resultados:")
    result = spark.read.format("delta").load(ORDERS)
    print("Orders com status MERGED_UPDATE:")
    result.filter(col("order_status") == "MERGED_UPDATE") \
        .select("order_id", "total_amount", "order_status").show()
    print("Orders com status MERGED_INSERT (novos):")
    result.filter(col("order_status") == "MERGED_INSERT") \
        .select("order_id", "total_amount", "order_status").show()


# ─────────────────────────────────────────────────────────────
# 3. DELETE & UPDATE
# Conceito: Delta suporta DELETE e UPDATE com predicado direto na
# tabela — algo impossível em Parquet puro (que é imutável). Por baixo,
# Delta reescreve só os arquivos afetados e registra no log.
# ─────────────────────────────────────────────────────────────
def delete_and_update(spark):
    print("\n" + "=" * 60)
    print("3. DELETE & UPDATE")
    print("=" * 60)

    dt = DeltaTable.forPath(spark, ORDERS)

    before = spark.read.format("delta").load(ORDERS).count()
    print(f"Registros antes: {before}")

    # UPDATE com predicado: dá 10% de desconto em orders 'cancelled'.
    print("UPDATE: -10% no total_amount de orders cancelled...")
    dt.update(
        condition=col("order_status") == "cancelled",
        set={"total_amount": col("total_amount") * 0.9},
    )

    # DELETE com predicado: remove os registros de teste que criamos
    # nas demos anteriores, pra não poluir.
    print("DELETE: removendo registros de teste (time_travel_test)...")
    dt.delete(col("order_status") == "time_travel_test")

    after = spark.read.format("delta").load(ORDERS).count()
    print(f"Registros depois: {after}  (removidos: {before - after})")

    print("\nMesmas operações existem em SQL:")
    print("  UPDATE delta.`path` SET col = ... WHERE ...")
    print("  DELETE FROM delta.`path` WHERE ...")


# ─────────────────────────────────────────────────────────────
# 4. OPTIMIZE + ZORDER
# Conceito: escritas incrementais geram muitos arquivos pequenos
# (small files problem), o que degrada leitura. OPTIMIZE compacta
# vários arquivos pequenos em poucos grandes. ZORDER BY clusteriza
# fisicamente os dados por uma coluna, acelerando filtros nela
# (data skipping mais eficiente).
# Use quando: tabela com muitas escritas append/merge, queries lentas.
# ─────────────────────────────────────────────────────────────
def optimize_zorder(spark):
    print("\n" + "=" * 60)
    print("4. OPTIMIZE + ZORDER")
    print("=" * 60)

    # DESCRIBE DETAIL mostra numFiles antes da compactação.
    print("Antes do OPTIMIZE:")
    spark.sql(f"DESCRIBE DETAIL delta.`{ORDERS}`") \
        .select("numFiles", "sizeInBytes").show()

    # OPTIMIZE compacta. ZORDER BY clusteriza por restaurant_id, que
    # é uma coluna comum em filtros analíticos sobre orders.
    print("Rodando OPTIMIZE ... ZORDER BY (restaurant_id)...")
    spark.sql(f"""
        OPTIMIZE delta.`{ORDERS}`
        ZORDER BY (restaurant_id)
    """).show(truncate=False)

    print("Depois do OPTIMIZE:")
    spark.sql(f"DESCRIBE DETAIL delta.`{ORDERS}`") \
        .select("numFiles", "sizeInBytes").show()


# ─────────────────────────────────────────────────────────────
# 5. VACUUM
# Conceito: OPTIMIZE, DELETE, UPDATE e MERGE deixam arquivos antigos
# para trás (são o que viabiliza time travel). VACUUM remove arquivos
# mais antigos que o retention threshold (default 7 dias).
# TRADE-OFF CRÍTICO: depois do VACUUM, você PERDE a capacidade de time
# travel para versões cujos arquivos foram removidos. Retention curto
# = menos storage, menos time travel. É decisão de governança.
# ─────────────────────────────────────────────────────────────
def vacuum_cleanup(spark):
    print("\n" + "=" * 60)
    print("5. VACUUM")
    print("=" * 60)

    dt = DeltaTable.forPath(spark, ORDERS)

    # DRY RUN: lista o que SERIA removido, sem remover. Sempre rode
    # dry run antes de um vacuum real em produção.
    print("VACUUM DRY RUN (lista candidatos, não remove):")
    spark.sql(f"VACUUM delta.`{ORDERS}` RETAIN 168 HOURS DRY RUN") \
        .show(truncate=False)

    # Em produção, o retention default de 7 dias (168h) protege jobs
    # concorrentes e time travel recente. Aqui em lab, mostramos o
    # comando mas mantemos o retention seguro — NÃO forçamos retention 0,
    # que é justamente o anti-pattern que a prova cobra que você evite.
    print("\nNota: retention abaixo de 168h exige desabilitar uma trava de")
    print("segurança (spark.databricks.delta.retentionDurationCheck.enabled).")
    print("Em produção isso quase nunca deve ser feito — quebra time travel")
    print("e pode corromper leituras concorrentes em andamento.")

    print("\nHistórico final da tabela:")
    dt.history().select("version", "operation").show(truncate=False)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    spark = get_spark("Demo-2-Delta-Operations")

    ensure_orders(spark)
    time_travel(spark)
    merge_upsert(spark)
    delete_and_update(spark)
    optimize_zorder(spark)
    vacuum_cleanup(spark)

    spark.stop()


if __name__ == "__main__":
    main()
