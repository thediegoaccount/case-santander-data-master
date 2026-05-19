"""
Job: Silver Clientes e Ordens
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F

from src.config.settings import configure_adls


def main():
    inicio = datetime.now()
    print(f"=== JOB CLIENTES_SILVER INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    print("Transformando Bronze Clientes para Silver...")
    df_clientes_silver = spark.sql("SELECT * FROM case_santander.bronze.clientes") \
        .withColumn("faixa_etaria",
            F.when(F.col("idade") < 30, "Jovem")
            .when(F.col("idade") < 50, "Adulto")
            .otherwise("Senior")) \
        .withColumn("score_categoria",
            F.when(F.col("score_credito") >= 750, "Excelente")
            .when(F.col("score_credito") >= 650, "Bom")
            .when(F.col("score_credito") >= 550, "Regular")
            .otherwise("Ruim")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_clientes_silver.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.silver.clientes")

    print("✅ silver.clientes gravado")

    print("Transformando Bronze Ordens para Silver...")
    df_ordens_silver = spark.sql("SELECT * FROM case_santander.bronze.ordens") \
        .withColumn("data_ordem", F.to_date("data_ordem")) \
        .withColumn("ano", F.year("data_ordem")) \
        .withColumn("mes", F.month("data_ordem")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_ordens_silver.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.silver.ordens")

    print("✅ silver.ordens gravado")

    fim = datetime.now()
    print("\n=== JOB CLIENTES_SILVER CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
