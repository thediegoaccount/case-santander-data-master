from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def transformar_bcb(spark: SparkSession, storage_account: str) -> int:
    data_hoje   = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/bcb/"

    print("Transformando BCB Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)

    df_silver = df \
        .withColumn("data",      F.to_date("data", "dd/MM/yyyy")) \
        .withColumn("ano",       F.year("data")) \
        .withColumn("mes",       F.month("data")) \
        .withColumn("trimestre", F.quarter("data")) \
        .withColumn("valor",     F.round("valor", 6)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("data").isNotNull()) \
        .dropDuplicates(["data", "indicador"])

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("indicador", "ano") \
        .save(silver_path)

    total = df_silver.count()
    print(f"Silver BCB gravado: {total} registros")
    return total
