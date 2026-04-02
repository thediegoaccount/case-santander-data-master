"""
Transformacao Bronze -> Silver para dados do World Bank
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def transformar_world_bank(spark: SparkSession, storage_account: str) -> int:
    """
    Le dados brutos do World Bank do Bronze,
    aplica transformacoes e grava no Silver em Delta Lake.

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/"

    print("Transformando World Bank Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)

    df_silver = df \
        .withColumn("data",  F.to_date(F.concat(F.col("data"), F.lit("-01-01")), "yyyy-MM-dd")) \
        .withColumn("ano",   F.year("data")) \
        .withColumn("valor", F.round("valor", 4)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("data").isNotNull())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("indicador", "ano") \
        .save(silver_path)

    total = df_silver.count()
    print(f"Silver World Bank gravado: {total} registros")
    return total
