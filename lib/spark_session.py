"""
Factory de SparkSession reutilizável para todos os jobs do lab.

Centraliza:
- Configuração Delta Lake (extensions + catalog)
- Configuração S3A apontando pro MinIO local
- Otimizações de leitura/escrita para Parquet/Delta

Uso típico em qualquer job:

    from lib.spark_session import get_spark
    spark = get_spark("Demo-1-Delta-Foundation")
    # ... seu código aqui ...
    spark.stop()

Por que centralizar: nas demos do curso original, cada arquivo .py
repete 30 linhas de config. Isso polui o que importa (o conceito).
Centralizando, cada nova demo vira "qual conceito estou aplicando?"
e não "será que copiei a config certa?".
"""

from pyspark.sql import SparkSession


# ─────────────────────────────────────────────────────────────
# Endpoints do MinIO dentro da rede Docker.
# "minio" é o hostname resolvido pelo DNS interno do Docker Compose.
# Porta 9000 = API S3 (NÃO é o console web, que é 9001).
# ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"


def get_spark(app_name: str) -> SparkSession:
    """
    Retorna uma SparkSession configurada para Delta Lake + MinIO (S3A).

    Args:
        app_name: nome da aplicação que aparece na Spark UI (porta 4040).
                  Use algo descritivo, ex.: "Demo-1-Delta-Foundation".

    Returns:
        SparkSession pronta pra ler/escrever em s3a://bucket/path.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # ─── Delta Lake ────────────────────────────────────────
        # Estas duas configs são OBRIGATÓRIAS pra Delta funcionar com SQL.
        # Sem a primeira, comandos como CONVERT TO DELTA não existem.
        # Sem a segunda, CREATE TABLE ... USING DELTA quebra.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ─── S3A → MinIO ───────────────────────────────────────
        # Usamos o conector S3A do Hadoop apontando pro MinIO.
        # Pra Spark, MinIO é indistinguível de S3 real.
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        # path.style.access=true é OBRIGATÓRIO pro MinIO.
        # S3 real usa virtual-hosted style (bucket.s3.amazonaws.com).
        # MinIO usa path style (minio:9000/bucket). Sem isso, 403.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # SSL desligado: MinIO local roda em HTTP puro.
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Implementação do filesystem S3A
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # AWS SDK precisa de uma região mesmo apontando pro MinIO
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        # ─── Otimizações úteis ─────────────────────────────────
        # Adaptive Query Execution: Spark reotimiza o plano em runtime.
        # Não é específico de Delta, mas ajuda em quase todo job.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Kryo: serializer mais rápido que o default (Java).
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        # .config("spark.eventLog.enabled", "true")  # Habilita logs de eventos pra Spark UI funcionar
        # .config("spark.eventLog.dir", "/tmp/spark-events")  # Diretório para logs de eventos
    )

    spark = builder.getOrCreate()

    # Reduz ruído nos logs. Mude pra "INFO" se quiser ver tudo o que
    # o Spark está fazendo por baixo (útil pra debug).
    spark.sparkContext.setLogLevel("WARN")

    return spark
