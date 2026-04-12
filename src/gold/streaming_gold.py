"""
Análises Gold derivadas de dados de streaming (silver.streaming)

Tabelas geradas:
  - gold.fraude_streaming      : deteccao de transacoes suspeitas em tempo real
  - gold.anomalias_intraday    : desvios de preco vs historico por ticker/hora
  - gold.volume_intraday       : volume negociado por ticker e hora do dia
  - gold.ranking_acoes_realtime: ranking de ativos por volume no dia atual
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def detectar_fraude_streaming(spark: SparkSession) -> int:
    """
    Detecta transacoes suspeitas no fluxo de streaming cruzando com
    benchmarks historicos de preco (gold.performance_acoes).

    Regras aplicadas:
      1. alerta_volume_suspeito  : quantidade > 9.000 unidades
      2. alerta_preco_atipico    : preco > R$90 ou < R$12
      3. alerta_valor_elevado    : valor_total > R$500.000 por transacao
      4. alerta_desvio_historico : preco desvia > 2x volatilidade historica do ativo

    Score:
      3+ alertas → Critico | 2 → Alto | 1 → Medio | 0 → Normal

    Returns: total de transacoes criticas detectadas
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Detectando fraudes em transacoes streaming...")

    df_stream = spark.sql("SELECT * FROM case_santander.silver.streaming")

    # Usa o ano mais recente de performance como referencia de preco historico
    # fmt: off
    df_perf = spark.sql("""
        SELECT ticker, preco_medio, volatilidade
        FROM case_santander.gold.performance_acoes
        WHERE ano = (SELECT MAX(ano) FROM case_santander.gold.performance_acoes)
    """)
    # fmt: on

    # df_perf tem exatamente 9 linhas (9 tickers × 1 ano) — broadcast elimina
    # o sort-merge shuffle distribuindo a tabela inteira em cada executor
    # fmt: off
    df_fraude = df_stream \
        .join(F.broadcast(df_perf), on="ticker", how="left") \
        .withColumn("alerta_volume_suspeito",
            F.when(F.col("quantidade") > 9000, True).otherwise(False)) \
        .withColumn("alerta_preco_atipico",
            F.when((F.col("preco") > 90) | (F.col("preco") < 12), True)
            .otherwise(False)) \
        .withColumn("alerta_valor_elevado",
            F.when(F.col("valor_total") > 500000, True).otherwise(False)) \
        .withColumn("alerta_desvio_historico",
            F.when(
                F.abs(F.col("preco") - F.col("preco_medio")) >
                (F.col("preco_medio") * F.col("volatilidade") / 100 * 2),
                True
            ).otherwise(False)) \
        .withColumn("total_alertas",
            F.col("alerta_volume_suspeito").cast("int") +
            F.col("alerta_preco_atipico").cast("int") +
            F.col("alerta_valor_elevado").cast("int") +
            F.col("alerta_desvio_historico").cast("int")) \
        .withColumn("score_fraude",
            F.when(F.col("total_alertas") >= 3, "Critico")
            .when(F.col("total_alertas") == 2, "Alto")
            .when(F.col("total_alertas") == 1, "Medio")
            .otherwise("Normal")) \
        .withColumn("requer_revisao",
            F.when(F.col("total_alertas") >= 2, True).otherwise(False)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .select(
            "id_transacao", "timestamp", "ticker", "tipo",
            "preco", "quantidade", "valor_total",
            "preco_medio", "volatilidade",
            "alerta_volume_suspeito", "alerta_preco_atipico",
            "alerta_valor_elevado", "alerta_desvio_historico",
            "total_alertas", "score_fraude", "requer_revisao",
            "data_processamento"
        )

    df_fraude.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.fraude_streaming")
    # fmt: on

    total_critico = df_fraude.filter(F.col("score_fraude") == "Critico").count()
    print(f"Gold fraude_streaming gravado: {df_fraude.count()} registros ({total_critico} criticos)")
    return total_critico


def detectar_anomalias_intraday(spark: SparkSession) -> int:
    """
    Detecta anomalias de preco intradiarias comparando o preco medio
    por hora de cada ativo contra seu historico (gold.performance_acoes).

    Metodo: Z-Score = (preco_medio_hora - preco_medio_historico) / desvio_historico
      Z > 2  → Alta Anormal | Z < -2 → Queda Anormal | else → Normal

    Returns: total de anomalias detectadas
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Detectando anomalias intraday...")

    df_stream = spark.sql("SELECT * FROM case_santander.silver.streaming")

    # fmt: off
    df_hora = df_stream \
        .groupBy("ticker", "hora") \
        .agg(
            F.round(F.avg("preco"), 2).alias("preco_medio_hora"),
            F.round(F.sum("valor_total"), 2).alias("valor_total_hora"),
            F.sum("quantidade").alias("volume_hora"),
            F.count("*").alias("total_transacoes_hora")
        )
    # fmt: on

    # fmt: off
    df_perf = spark.sql("""
        SELECT ticker, preco_medio, volatilidade
        FROM case_santander.gold.performance_acoes
        WHERE ano = (SELECT MAX(ano) FROM case_santander.gold.performance_acoes)
    """)
    # fmt: on

    # desvio_historico em R$: volatilidade (% stddev de variacao) * preco_medio / 100
    # df_hora tem no maximo 9 tickers × 24h = 216 linhas; df_perf = 9 linhas
    # fmt: off
    df_anomalias = df_hora \
        .join(F.broadcast(df_perf), on="ticker", how="left") \
        .withColumn("desvio_historico_rs",
            F.round(F.col("preco_medio") * F.col("volatilidade") / 100, 4)) \
        .withColumn("zscore_intraday",
            F.round(
                (F.col("preco_medio_hora") - F.col("preco_medio")) /
                F.when(F.col("desvio_historico_rs") == 0, 1)
                 .otherwise(F.col("desvio_historico_rs")),
                4)) \
        .withColumn("anomalia",
            F.when(F.abs(F.col("zscore_intraday")) > 2, True).otherwise(False)) \
        .withColumn("tipo_anomalia",
            F.when(F.col("zscore_intraday") > 2,  "Alta Anormal")
            .when(F.col("zscore_intraday") < -2, "Queda Anormal")
            .otherwise("Normal")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_anomalias.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.anomalias_intraday")
    # fmt: on

    total_anomalias = df_anomalias.filter(F.col("anomalia")).count()
    print(f"Gold anomalias_intraday gravado: {df_anomalias.count()} registros ({total_anomalias} anomalias)")
    return total_anomalias


def calcular_volume_intraday(spark: SparkSession) -> int:
    """
    Agrega o volume de transacoes streaming por ativo e hora do dia,
    comparando com o volume medio historico diario (gold.performance_acoes).

    Metricas:
      - total_transacoes, volume_hora, valor_total_hora, preco_medio_hora
      - compras / vendas separados
      - pct_volume_diario: volume_hora / volume_medio_historico * 100
      - alerta_volume: Critico (>30%), Alto (>15%), Normal

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Calculando volume intraday...")

    df_stream = spark.sql("SELECT * FROM case_santander.silver.streaming")

    # fmt: off
    df_vol = df_stream \
        .groupBy("ticker", "hora") \
        .agg(
            F.count("*").alias("total_transacoes"),
            F.sum("quantidade").alias("volume_hora"),
            F.round(F.sum("valor_total"), 2).alias("valor_total_hora"),
            F.round(F.avg("preco"), 2).alias("preco_medio_hora"),
            F.sum(F.when(F.col("tipo") == "compra", F.col("quantidade"))
                .otherwise(0)).alias("volume_compras"),
            F.sum(F.when(F.col("tipo") == "venda", F.col("quantidade"))
                .otherwise(0)).alias("volume_vendas")
        )
    # fmt: on

    # fmt: off
    df_perf = spark.sql("""
        SELECT ticker, volume_medio
        FROM case_santander.gold.performance_acoes
        WHERE ano = (SELECT MAX(ano) FROM case_santander.gold.performance_acoes)
    """)
    # fmt: on

    # fmt: off
    df_volume_intraday = df_vol \
        .join(F.broadcast(df_perf), on="ticker", how="left") \
        .withColumn("pct_volume_diario",
            F.round(
                F.col("volume_hora") /
                F.when(F.col("volume_medio") == 0, 1)
                 .otherwise(F.col("volume_medio")) * 100,
                2)) \
        .withColumn("alerta_volume_intraday",
            F.when(F.col("pct_volume_diario") > 30, "Critico")
            .when(F.col("pct_volume_diario") > 15, "Alto")
            .otherwise("Normal")) \
        .withColumn("pressao_compradora",
            F.when(F.col("volume_compras") > F.col("volume_vendas"), "Compra")
            .when(F.col("volume_vendas") > F.col("volume_compras"), "Venda")
            .otherwise("Neutro")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .orderBy("ticker", "hora")

    df_volume_intraday.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.volume_intraday")
    # fmt: on

    total = df_volume_intraday.count()
    print(f"Gold volume_intraday gravado: {total} registros")
    return total


def calcular_ranking_realtime(spark: SparkSession) -> int:
    """
    Calcula o ranking de ativos por volume financeiro nas transacoes
    de streaming, comparando preco atual com media historica.

    Metricas:
      - total_transacoes, volume_total, valor_total, preco_medio_atual
      - variacao_vs_historico_pct: desvio % do preco atual vs historico
      - rank_volume: posicao no ranking por valor total negociado

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Calculando ranking de ativos em tempo real...")

    df_stream = spark.sql("SELECT * FROM case_santander.silver.streaming")

    # fmt: off
    df_rank = df_stream \
        .groupBy("ticker") \
        .agg(
            F.count("*").alias("total_transacoes"),
            F.sum("quantidade").alias("volume_total"),
            F.round(F.sum("valor_total"), 2).alias("valor_total"),
            F.round(F.avg("preco"), 2).alias("preco_medio_atual"),
            F.sum(F.when(F.col("tipo") == "compra", 1).otherwise(0)).alias("total_compras"),
            F.sum(F.when(F.col("tipo") == "venda",  1).otherwise(0)).alias("total_vendas"),
            F.round(F.min("preco"), 2).alias("preco_minimo"),
            F.round(F.max("preco"), 2).alias("preco_maximo")
        )
    # fmt: on

    # fmt: off
    df_perf = spark.sql("""
        SELECT ticker, empresa, setor, preco_medio as preco_medio_historico
        FROM case_santander.gold.performance_acoes
        WHERE ano = (SELECT MAX(ano) FROM case_santander.gold.performance_acoes)
    """)
    # fmt: on

    # fmt: off
    df_ranking = df_rank \
        .join(F.broadcast(df_perf), on="ticker", how="left") \
        .withColumn("variacao_vs_historico_pct",
            F.round(
                (F.col("preco_medio_atual") - F.col("preco_medio_historico")) /
                F.when(F.col("preco_medio_historico") == 0, 1)
                 .otherwise(F.col("preco_medio_historico")) * 100,
                2)) \
        .withColumn("tendencia",
            F.when(F.col("variacao_vs_historico_pct") > 1,  "Alta")
            .when(F.col("variacao_vs_historico_pct") < -1, "Queda")
            .otherwise("Estavel")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .orderBy(F.desc("valor_total")) \
        .withColumn("rank_volume", (F.monotonically_increasing_id() + 1).cast("int"))

    df_ranking.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.ranking_acoes_realtime")
    # fmt: on

    total = df_ranking.count()
    print(f"Gold ranking_acoes_realtime gravado: {total} registros")
    return total
