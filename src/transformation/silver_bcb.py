from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transformar_bcb(spark: SparkSession, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/bcb/"

    print("Transformando BCB Bronze -> Silver...")

    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}extracao=*/")
    print("Schema Bronze:")
    df.printSchema()
    print("Amostra:")
    df.show(3)

    # fmt: off
    df_silver = df \
        .withColumn("data",      F.to_date("data", "dd/MM/yyyy")) \
        .withColumn("ano",       F.year("data")) \
        .withColumn("mes",       F.month("data")) \
        .withColumn("trimestre", F.quarter("data")) \
        .withColumn("valor",     F.round("valor", 6)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("data").isNotNull()) \
        .dropDuplicates(["data", "indicador"]) \
        .drop("extracao")
    # fmt: on

    print(f"Registros após transformação: {df_silver.count()}")
    df_silver.show(3)

    # fmt: off
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)
    # fmt: on

    total = df_silver.count()
    print(f"Silver BCB gravado: {total} registros")
    return total
