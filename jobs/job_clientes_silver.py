"""
Job: Silver Clientes e Ordens
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

from src.config.settings import configure_adls
from src.config.tables import SCHEMA_BRONZE, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_clientes_silver", f"=== JOB CLIENTES_SILVER INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    info("job_clientes_silver", "Transformando Bronze Clientes para Silver...")
    df_clientes_silver = spark.sql(f"SELECT * FROM {SCHEMA_BRONZE}.clientes") \
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
        .saveAsTable(f"{SCHEMA_SILVER}.clientes")

    info("job_clientes_silver", " silver.clientes gravado")

    info("job_clientes_silver", "Transformando Bronze Ordens para Silver...")
    df_ordens_silver = spark.sql(f"SELECT * FROM {SCHEMA_BRONZE}.ordens") \
        .withColumn("data_ordem", F.to_date("data_ordem")) \
        .withColumn("ano", F.year("data_ordem")) \
        .withColumn("mes", F.month("data_ordem")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_ordens_silver.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{SCHEMA_SILVER}.ordens")

    info("job_clientes_silver", " silver.ordens gravado")

    fim = datetime.now()
    info("job_clientes_silver", "\n=== JOB CLIENTES_SILVER CONCLUIDO ===")
    info("job_clientes_silver", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
