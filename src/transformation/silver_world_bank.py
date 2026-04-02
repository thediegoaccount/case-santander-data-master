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
    """
    data_hoje   = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/"

    print("Transformando World Bank Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)

    # Debug — ver schema e amostra
    print("Schema Bronze:")
    df.printSchema()
    print("Amostra:")
    df.show(3)

    df_silver = df \
        .withColumn("ano", F.col("data").cast("integer")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("ano").isNotNull())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    total = df_silver.count()
    print(f"Silver World Bank gravado: {total} registros")
    return total
