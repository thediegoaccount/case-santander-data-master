from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def transformar_world_bank(spark: SparkSession, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/"

    print("Transformando World Bank Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)
    print("Colunas disponíveis:", df.columns)

    # Compatível com schema antigo (data) e novo (ano)
    if "ano" in df.columns:
        df = df.withColumn("ano", F.col("ano").cast("integer"))
    elif "data" in df.columns:
        df = df.withColumn("ano", F.col("data").cast("integer"))
    else:
        raise Exception("Coluna de ano não encontrada no Bronze!")

    # fmt: off
    df_silver = df \
        .withColumn("valor", F.round("valor", 4)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("ano").isNotNull())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)
    # fmt: on

    total = df_silver.count()
    print(f"Silver World Bank gravado: {total} registros")
    return total
