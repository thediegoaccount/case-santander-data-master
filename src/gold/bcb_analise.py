"""
Análise de indicadores econômicos do Banco Central do Brasil
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def analisar_indicadores_bcb(spark: SparkSession) -> int:
    """
    Analisa séries do BCB: Selic, Câmbio USD/BRL, IPCA
    Identifica tendências e gera alertas de mudanças significativas.

    Retorna: número de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Analisando indicadores BCB...")

    # Ler dados Silver do BCB
    df_bcb = spark.sql(f"SELECT * FROM {SCHEMA_SILVER}.bcb")

    # Pivot para ter cada indicador em uma coluna
    # fmt: off
    df_pivot = df_bcb \
        .groupBy("data") \
        .pivot("indicador") \
        .agg(F.first("valor")) \
        .orderBy("data")
    # fmt: on

    # Calcular indicadores técnicos (SMA 7d, volatilidade, mudança)
    window_7d = Window.orderBy("data").rangeBetween(-7 * 86400, 0)
    window_30d = Window.orderBy("data").rangeBetween(-30 * 86400, 0)
    window_12m = Window.orderBy("data").rangeBetween(-365 * 86400, 0)

    # fmt: off
    df_analise = df_pivot \
        .withColumn("selic_media_7d",
            F.round(F.avg("selic").over(window_7d), 4)) \
        .withColumn("selic_volatilidade_30d",
            F.round(F.stddev("selic").over(window_30d), 4)) \
        .withColumn("cambio_media_7d",
            F.round(F.avg("cambio_usd_brl").over(window_7d), 4)) \
        .withColumn("cambio_variacao_pct",
            F.round(
                (F.col("cambio_usd_brl") - F.col("cambio_media_7d")) /
                F.col("cambio_media_7d") * 100, 2)) \
        .withColumn("ipca_acumulado_12m",
            F.sum("ipca").over(window_12m)) \
        .withColumn("tendencia_selic",
            F.when(F.col("selic") > F.col("selic_media_7d"), "Alta")
            .when(F.col("selic") < F.col("selic_media_7d"), "Queda")
            .otherwise("Estavel")) \
        .withColumn("alerta_cambio",
            F.when(F.abs(F.col("cambio_variacao_pct")) > 5, "Critico")
            .when(F.abs(F.col("cambio_variacao_pct")) > 2, "Alto")
            .otherwise("Normal")) \
        .withColumn("alerta_inflacao",
            F.when(F.col("ipca_acumulado_12m") > 5, "Critico")
            .when(F.col("ipca_acumulado_12m") > 3, "Alto")
            .otherwise("Normal")) \
        .withColumn("data_processamento", F.lit(data_hoje))
    # fmt: on

    # Gravar Gold
    df_analise.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{SCHEMA_GOLD}.indicadores_bcb"
    )

    print("Gold indicadores_bcb gravado")
    return 0  # Metrics in Spark UI
