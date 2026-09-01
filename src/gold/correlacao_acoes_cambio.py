"""
Análise de correlação entre ações e câmbio USD/BRL
Identifica ações que se beneficiam ou sofrem com variações cambiais
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def correlacionar_acoes_cambio(spark: SparkSession) -> int:
    """
    Correlaciona performance de ações com variações do câmbio USD/BRL.

    Lógica:
      - Exportadoras (VALE, PETR): ganham com câmbio alto
      - Importadoras (MGLU): perdem com câmbio alto
      - Domésticas (ITUB, BBDC): menos sensíveis

    Retorna: número de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Correlacionando ações vs câmbio...")

    # Ler dados Silver
    df_acoes = spark.sql("""
        SELECT date, ticker, close, variacao_diaria_pct, volume, ano, mes
        FROM case_santander.silver.acoes
    """)

    df_bcb = spark.sql("""
        SELECT data as date, valor as cambio
        FROM case_santander.silver.bcb
        WHERE indicador = 'cambio_usd_brl'
    """)

    # Agregar por dia para cada ativo
    # fmt: off
    df_acoes_diarias = df_acoes \
        .groupBy("date", "ticker") \
        .agg(
            F.round(F.avg("close"), 2).alias("preco_medio"),
            F.round(F.avg("variacao_diaria_pct"), 4).alias("variacao_media_pct"),
            F.round(F.sum("volume"), 0).alias("volume_dia"),
            F.first("ano").alias("ano"),
            F.first("mes").alias("mes")
        )
    # fmt: on

    # Join ações com câmbio
    # fmt: off
    df_correlacao = df_acoes_diarias \
        .join(df_bcb, on="date", how="left") \
        .dropna(subset=["cambio"]) \
        .withColumn("cambio_variacao_pct",
            F.round(
                (F.col("cambio") - F.lag("cambio").over(
                    Window.partitionBy("ticker").orderBy("date")
                )) / F.lag("cambio").over(
                    Window.partitionBy("ticker").orderBy("date")
                ) * 100, 4))
    # fmt: on

    # Calcular correlação por ticker (últimos 90 dias)
    window_90d = Window.partitionBy("ticker").orderBy("date").rangeBetween(-90 * 86400, 0)

    # fmt: off
    df_resultado = df_correlacao \
        .withColumn("correlacao_cambio",
            F.round(
                F.covar_pop("variacao_media_pct", "cambio_variacao_pct").over(window_90d) /
                (F.stddev_pop("variacao_media_pct").over(window_90d) *
                 F.stddev_pop("cambio_variacao_pct").over(window_90d)),
                4)) \
        .withColumn("sensibilidade_cambio",
            F.when(F.col("correlacao_cambio") > 0.5, "Alta Positiva (Exportadora)")
            .when(F.col("correlacao_cambio") > 0.2, "Moderada Positiva")
            .when(F.col("correlacao_cambio") < -0.5, "Alta Negativa (Importadora)")
            .when(F.col("correlacao_cambio") < -0.2, "Moderada Negativa")
            .otherwise("Baixa Correlacao")) \
        .withColumn("alerta_desacoplamento",
            F.when(
                F.abs(F.col("variacao_media_pct")) > 5 &
                (F.abs(F.col("cambio_variacao_pct")) < 1),
                "Sim - Acao se desacoplou do cambio"
            )
            .otherwise("Nao")) \
        .withColumn("recomendacao",
            F.when(
                (F.col("correlacao_cambio") > 0.3) & (F.col("cambio_variacao_pct") > 2),
                "Comprar - Exportadora com cambio em alta"
            )
            .when(
                (F.col("correlacao_cambio") < -0.3) & (F.col("cambio_variacao_pct") > 2),
                "Vender - Importadora com cambio em alta"
            )
            .otherwise("Neutro")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .select(
            "date", "ticker", "preco_medio", "variacao_media_pct",
            "cambio", "cambio_variacao_pct",
            "correlacao_cambio", "sensibilidade_cambio",
            "alerta_desacoplamento", "recomendacao",
            "ano", "mes", "data_processamento"
        )
    # fmt: on

    # Gravar Gold
    df_resultado.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.acoes_vs_cambio")

    print("Gold acoes_vs_cambio gravado")
    return 0  # Metrics in Spark UI
