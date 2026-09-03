"""
Deteccao de fraude em ordens de clientes
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.logging import info, warning
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def detectar_fraude(spark: SparkSession) -> int:
    """
    Detecta ordens suspeitas cruzando dados de ordens
    com score de risco dos clientes.

    Returns: total de ordens criticas detectadas
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Detectando fraudes...")

    df_ordens = spark.sql(f"SELECT * FROM {SCHEMA_SILVER}.ordens")
    df_score = spark.sql(f"""
        SELECT hash_cliente, score_risco,
               categoria_risco, limite_operacional
        FROM {SCHEMA_GOLD}.score_risco_clientes
    """)

    # Decide entre broadcast e sort-merge pelo tamanho de df_score, que cresce
    # com o SCD Type-2.
    #
    # A estimativa anterior era código morto: montava o nome da tabela a
    # partir de um RDD vazio via API privada do SparkContext, lendo um
    # atributo que DataFrame não possui, e o próprio RDD vazio sem schema
    # também falha na conversão. Levantava
    # AttributeError em toda execução, o `except` engolia, `use_broadcast`
    # ficava sempre True e o ramo sort-merge abaixo era inalcançável. Ou seja:
    # a proteção contra df_score grande nunca existiu.
    #
    # Trocado por contagem de linhas, que no Delta sai dos metadados e é
    # barata. score_risco_clientes tem uma linha por cliente, então o limiar
    # em linhas é mais legível que em bytes para quem for ajustar.
    MAX_LINHAS_BROADCAST = 5_000_000

    try:
        total_linhas = df_score.count()
        use_broadcast = total_linhas < MAX_LINHAS_BROADCAST
        info("gold_fraude", f"df_score: {total_linhas} linhas, "
                            f"broadcast={use_broadcast}")
    except Exception as e:
        # Sem a contagem não dá para decidir; sort-merge funciona em qualquer
        # tamanho, então é o fallback seguro (broadcast de tabela grande
        # estoura o driver).
        use_broadcast = False
        warning("gold_fraude", f"Falha ao medir df_score, usando sort-merge: {e}")

    if use_broadcast:
        df_join = df_ordens.join(
            F.broadcast(df_score), on="hash_cliente", how="left")
    else:
        # Sort-merge join sem broadcast
        df_join = df_ordens.join(df_score, on="hash_cliente", how="left")

    # fmt: off
    df_fraude = df_join \
        .withColumn("alerta_valor_alto",
            F.when(F.col("valor_total") > F.col("limite_operacional"), True)
            .otherwise(False)) \
        .withColumn("alerta_volume_suspeito",
            F.when(F.col("quantidade") > 9000, True).otherwise(False)) \
        .withColumn("alerta_preco_atipico",
            F.when((F.col("preco") > 90) | (F.col("preco") < 12), True)
            .otherwise(False)) \
        .withColumn("alerta_perfil_incompativel",
            F.when(
                (F.col("perfil_risco") == "Conservador") &
                (F.col("valor_total") > 200000), True)
            .otherwise(False)) \
        .withColumn("total_alertas",
            F.col("alerta_valor_alto").cast("int") +
            F.col("alerta_volume_suspeito").cast("int") +
            F.col("alerta_preco_atipico").cast("int") +
            F.col("alerta_perfil_incompativel").cast("int")) \
        .withColumn("score_fraude",
            F.when(F.col("total_alertas") >= 3, "Critico")
            .when(F.col("total_alertas") == 2, "Alto")
            .when(F.col("total_alertas") == 1, "Medio")
            .otherwise("Normal")) \
        .withColumn("requer_revisao",
            F.when(F.col("total_alertas") >= 2, True).otherwise(False)) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_fraude.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{SCHEMA_GOLD}.deteccao_fraude")
    # fmt: on

    print("Gold fraude gravado")
    return 0  # Metrics available in Spark UI
