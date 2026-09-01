"""
Calculo de performance das acoes por setor e ano
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def calcular_performance(spark: SparkSession, storage_account: str) -> int:
    """
    Calcula performance media das acoes por setor e ano
    e grava na camada Gold.

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/acoes/"
    gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/"

    print("Calculando performance...")

    df = spark.read.format("delta").load(silver_path)

    # fmt: off
    df_performance = df \
        .groupBy("ticker", "empresa", "setor", "ano") \
        .agg(
            F.round(F.avg("close"), 2).alias("preco_medio"),
            F.round(F.min("close"), 2).alias("preco_minimo"),
            F.round(F.max("close"), 2).alias("preco_maximo"),
            F.round(F.avg("variacao_diaria_pct"), 4).alias("variacao_media_pct"),
            F.round(F.stddev("variacao_diaria_pct"), 4).alias("volatilidade"),
            F.round(F.avg("volume"), 0).alias("volume_medio"),
            F.round(F.sum("volume"), 0).alias("volume_total"),
            F.count("*").alias("dias_negociados")
        ) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .orderBy("setor", "ticker", "ano")

    df_performance.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_path)
    # fmt: on

    print("Gold performance gravado")
    return 0  # Metrics available in Spark UI
