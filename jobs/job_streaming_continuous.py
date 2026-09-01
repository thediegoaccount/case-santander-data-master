"""
Job: Streaming Contínuo
Processa transacoes financeiras em tempo real via Structured Streaming.

Diferença do job_streaming.py:
- Fica aguardando atualizações 24/7
- Não depende de trigger agendado
- Executa como serviço contínuo
- Não limpa checkpoint (mantém estado)
- Não limpa destino (mantém histórico)

Ou via Databricks Workflow:
    Job: streaming_continuous (serviço 24/7)
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.config.settings import configure_adls


def main():
    inicio = datetime.now()
    info("job_streaming_continuous", f"=== STREAMING CONTÍNUO INICIADO: {inicio} ===")
    info("job_streaming_continuous", "Modo: Aguardando atualizações 24/7")
    info("job_streaming_continuous", "Trigger: processingTime='1 minute'")
    info("job_streaming_continuous", "Pressione Ctrl+C para parar")

    spark = DatabricksSession.builder.getOrCreate()

    # Configuração ADLS
    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    bronze_kafka_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/"
    silver_streaming_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/streaming/"
    checkpoint_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/checkpoints/streaming_continuous/"

    # Parar streams anteriores com mesmo nome
    for stream in spark.streams.active:
        if stream.name == "streaming_continuous_query":
            stream.stop()
            info("job_streaming_continuous", f"Stream anterior parado: {stream.name}")

    # Schema de transações
    schema_transacao = StructType([
        StructField("timestamp",    StringType(),  True),
        StructField("ticker",       StringType(),  True),
        StructField("preco",        DoubleType(),  True),
        StructField("quantidade",   LongType(),    True),
        StructField("tipo",         StringType(),  True),
        StructField("corretora",    StringType(),  True),
        StructField("id_transacao", StringType(),  True)
    ])

    # Auto Loader - NÃO limpa checkpoint (mantém estado)
    df_stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("cloudFiles.maxFilesPerTrigger", 1) \
        .schema(schema_transacao) \
        .load(bronze_kafka_path)

    # Transformações
    df_processado = df_stream \
        .withColumn("timestamp",   F.to_timestamp("timestamp")) \
        .withColumn("hora",        F.hour("timestamp")) \
        .withColumn("minuto",      F.minute("timestamp")) \
        .withColumn("valor_total", F.round(F.col("preco") * F.col("quantidade"), 2)) \
        .withColumn("alerta_volume",
            F.when(F.col("quantidade") > 8000, "Volume Alto")
            .when(F.col("quantidade") > 5000, "Volume Medio")
            .otherwise("Normal")) \
        .withColumn("alerta_preco",
            F.when(F.col("preco") > 80, "Preco Alto")
            .when(F.col("preco") < 15, "Preco Baixo")
            .otherwise("Normal")) \
        .withColumn("processado_em", F.lit(datetime.now().isoformat()))

    # Write Stream - CONTÍNUO
    # NÃO limpa destino (mantém histórico)
    query = df_processado.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .queryName("streaming_continuous_query") \
        .trigger(processingTime='1 minute') \
        .start(silver_streaming_path)

    info("job_streaming_continuous", "Streaming contínuo iniciado")
    info("job_streaming_continuous", f"Aguardando atualizações... (latencia: ~1 minuto)")
    info("job_streaming_continuous", f"Bronze: {bronze_kafka_path}")
    info("job_streaming_continuous", f"Silver: {silver_streaming_path}")
    info("job_streaming_continuous", f"Checkpoint: {checkpoint_path}")

    # Fica aguardando indefinidamente
    query.awaitTermination()


if __name__ == "__main__":
    main()
