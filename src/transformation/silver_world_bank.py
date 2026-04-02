from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def transformar_world_bank(spark: SparkSession, storage_account: str) -> int:
    data_hoje   = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/"

    print("Transformando World Bank Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)
    df.printSchema()
    df.show(3)

    df_silver = df \
        .withColumn("ano",   F.col("ano").cast("integer")) \
        .withColumn("valor", F.round("valor", 4)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("ano").isNotNull())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("indicador", "ano") \
        .save(silver_path)

    total = df_silver.count()
    print(f"Silver World Bank gravado: {total} registros")
    return total
