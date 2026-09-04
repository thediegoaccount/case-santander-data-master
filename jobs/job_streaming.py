"""
Job: Structured Streaming
Processa transacoes financeiras em tempo real via Structured Streaming.

Ou via Databricks Workflow:
    Task: t5_streaming
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
from src.config.tables import register_external_table
from src.config.tables import SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_streaming", f"=== JOB STREAMING INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    bronze_kafka_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/"
    silver_streaming_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/streaming/"
    checkpoint_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/checkpoints/streaming/"

    # Parar todos os streams ativos antes de iniciar
    for stream in spark.streams.active:
        stream.stop()
        info("job_streaming", f"Stream anterior parado: {stream.name}")

    # fmt: off
    schema_transacao = StructType([
        StructField("timestamp",    StringType(),  True),
        StructField("ticker",       StringType(),  True),
        StructField("preco",        DoubleType(),  True),
        StructField("quantidade",   LongType(),    True),
        StructField("tipo",         StringType(),  True),
        StructField("corretora",    StringType(),  True),
        StructField("id_transacao", StringType(),  True)
    ])
    # fmt: on

    # Limpar checkpoint anterior
    dbutils.fs.rm(checkpoint_path, recurse=True)

    # Auto Loader: ingestion incremental cloud-native com rastreamento de arquivos
    # e inferência/evolução de schema automática via cloudFiles
    # fmt: off
    df_stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "avro") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("cloudFiles.maxFilesPerTrigger", 1) \
        .load(bronze_kafka_path)

    # Event Hub Capture grava Avro com envelope
    # {SequenceNumber, Offset, EnqueuedTimeUtc, SystemProperties, Properties,
    # Body}. O payload JSON original vem em Body, como bytes.
    df_stream = df_stream.select(
        F.from_json(F.col("Body").cast("string"), schema_transacao).alias("t")
    ).select("t.*")

    # processado_em usa F.current_timestamp() (nativo do Spark), nao
    # F.lit(datetime.now().isoformat()). O antigo era Python puro, avaliado
    # UMA VEZ quando o plano de streaming e montado, nao por micro-lote.
    # Auto Loader com trigger(availableNow=True) pode processar varios lotes
    # internos numa unica execucao se houver backlog -- todos sairiam com o
    # mesmo processado_em, o instante em que o job comecou.
    # Mantido como string (nao timestamp nativo) para nao quebrar
    # mergeSchema numa tabela ja existente com essa coluna como string.
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
        .withColumn("processado_em", F.current_timestamp().cast("string"))

    # NAO limpar o destino: job_streaming_continuous escreve no MESMO
    # silver/streaming/ com um checkpoint proprio. O `dbutils.fs.rm` que
    # existia aqui apagava o diretorio Delta sob o stream continuo, que
    # abortava com FileNotFoundException e ficava com o checkpoint apontando
    # para uma tabela inexistente -- estado irrecuperavel sem reset.
    query = df_processado.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(silver_streaming_path)

    query.awaitTermination()
    info("job_streaming", " Streaming processado!")

    # Registra a tabela sem DROP: o DROP anterior apagava a propriedade
    # delta.enableChangeDataFeed a cada execucao, matando o CDF que
    # job_unity_catalog tinha acabado de habilitar.
    register_external_table(spark, "silver", "streaming", silver_streaming_path)
    # fmt: on

    info("job_streaming", f" {SCHEMA_SILVER}.streaming gravado")

    fim = datetime.now()
    info("job_streaming", "\n=== JOB STREAMING CONCLUIDO ===")
    info("job_streaming", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
