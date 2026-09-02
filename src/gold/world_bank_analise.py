"""
Análise de contexto macroeconômico do Banco Mundial
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def analisar_contexto_macro(spark: SparkSession) -> int:
    """
    Analisa indicadores macroeconômicos do Banco Mundial:
    PIB e Desemprego para contexto global de performance de ações.

    Retorna: número de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Analisando contexto macroeconômico...")

    # Ler dados Silver do World Bank
    df_wb = spark.sql(f"SELECT * FROM {SCHEMA_SILVER}.world_bank")

    # Pivot para ter cada indicador em uma coluna
    # fmt: off
    df_pivot = df_wb \
        .groupBy("ano") \
        .pivot("indicador") \
        .agg(F.first("valor")) \
        .orderBy("ano")
    # fmt: on

    # Calcular variações e tendências
    # fmt: off
    df_analise = df_pivot \
        .withColumn("pib_variacao_pct",
            F.round(
                (F.col("pib_anual") - F.lag("pib_anual").over(Window.orderBy("ano"))) /
                F.lag("pib_anual").over(Window.orderBy("ano")) * 100,
                2)) \
        .withColumn("desemprego_variacao_pct",
            F.round(
                F.col("desemprego") - F.lag("desemprego").over(Window.orderBy("ano")),
                2)) \
        .withColumn("tendencia_pib",
            F.when(F.col("pib_variacao_pct") > 2, "Crescimento Alto")
            .when(F.col("pib_variacao_pct") > 0, "Crescimento Moderado")
            .when(F.col("pib_variacao_pct") > -2, "Queda Moderada")
            .otherwise("Queda Severa")) \
        .withColumn("tendencia_desemprego",
            F.when(F.col("desemprego_variacao_pct") < -1, "Melhorando (Rápido)")
            .when(F.col("desemprego_variacao_pct") < 0, "Melhorando")
            .when(F.col("desemprego_variacao_pct") < 1, "Piorando")
            .otherwise("Piorando (Rápido)")) \
        .withColumn("cenario_macro",
            F.when(
                (F.col("pib_variacao_pct") > 2) & (F.col("desemprego_variacao_pct") < 0),
                "Expansao Economica"
            )
            .when(
                (F.col("pib_variacao_pct") > 0) & (F.col("desemprego_variacao_pct") > 0),
                "Desaceleracao"
            )
            .when(
                (F.col("pib_variacao_pct") < 0) & (F.col("desemprego_variacao_pct") > 1),
                "Recessao"
            )
            .otherwise("Transicao")) \
        .withColumn("alerta_risco",
            F.when(
                (F.col("pib_variacao_pct") < -2) | (F.col("desemprego") > 15),
                "Alto"
            )
            .when(
                (F.col("pib_variacao_pct") < 0) | (F.col("desemprego") > 10),
                "Moderado"
            )
            .otherwise("Baixo")) \
        .withColumn("impacto_bolsa",
            F.when(F.col("cenario_macro") == "Expansao Economica",
                "Positivo - Favoravel para acoes ciclicas")
            .when(F.col("cenario_macro") == "Desaceleracao",
                "Negativo - Favora acoes defensivas")
            .when(F.col("cenario_macro") == "Recessao",
                "Muito Negativo - Ouro e defensivas")
            .otherwise("Neutro")) \
        .withColumn("data_processamento", F.lit(data_hoje))
    # fmt: on

    # Gravar Gold
    df_analise.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{SCHEMA_GOLD}.contexto_macroeconomico"
    )

    print("Gold contexto_macroeconomico gravado")
    return 0  # Metrics in Spark UI
