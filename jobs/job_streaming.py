"""
Job: Structured Streaming
Processa transacoes financeiras em tempo real via Structured Streaming.

Ou via Databricks Workflow:
    Task: t5_streaming
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from src.config.settings import configure_adls


def main():
    inicio = datetime.now()
    print(f"=== JOB STREAMING INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id       = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id       = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret   = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    bronze_kafka_path  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/"
    silver_streaming_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/streaming/"
    checkpoint_path    = f"abfss://silver@{storage_account}.dfs.core.windows.net/checkpoints/streaming/"

    # Parar todos os streams ativos antes de iniciar
    for stream in spark.streams.active:
        stream.stop()
        print(f"Stream anterior parado: {stream.name}")
        
    schema_transacao = StructType([
        StructField("timestamp",    StringType(),  True),
        StructField("ticker",       StringType(),  True),
        StructField("preco",        DoubleType(),  True),
        StructField("quantidade",   LongType(),    True),
        StructField("tipo",         StringType(),  True),
        StructField("corretora",    StringType(),  True),
        StructField("id_transacao", StringType(),  True)
    ])

    # Limpar checkpoint anterior
    dbutils.fs.rm(checkpoint_path, recurse=True)

    # Auto Loader: ingestion incremental cloud-native com rastreamento de arquivos
    # e inferência/evolução de schema automática via cloudFiles
    df_stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("cloudFiles.maxFilesPerTrigger", 1) \
        .schema(schema_transacao) \
        .load(bronze_kafka_path)

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

    # Limpar destino e recriar
    dbutils.fs.rm(silver_streaming_path, recurse=True)

    query = df_processado.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(silver_streaming_path)

    query.awaitTermination()
    print(f"✅ Streaming processado!")

    # Registrar no Unity Catalog
    spark.sql("DROP TABLE IF EXISTS case_santander.silver.streaming")
    df_resultado = spark.read.format("delta").load(silver_streaming_path)
    df_resultado.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.silver.streaming")

    total = spark.sql("SELECT COUNT(*) as total FROM case_santander.silver.streaming").collect()[0]["total"]
    print(f"✅ case_santander.silver.streaming → {total} registros")

    fim = datetime.now()
    print(f"\n=== JOB STREAMING CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
