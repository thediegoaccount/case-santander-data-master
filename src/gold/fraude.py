"""
Deteccao de fraude em ordens de clientes
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

    # Validate broadcast size: df_score pode crescer com SCD Type-2
    # Fallback para sort-merge join se > 2GB
    max_broadcast_bytes = 2_000_000_000  # 2GB default threshold

    try:
        # Estimate size sem materializar
        df_score_size = spark.sql(f"""
            SELECT SUM(LENGTH(CAST(struct(*) AS STRING))) as total_bytes
            FROM {df_score._sc.parallelize([]).toDF().name}
        """).collect()[0][0] or 0

        use_broadcast = (df_score_size < max_broadcast_bytes) if df_score_size else True
    except Exception:
        # Se erro na estimativa, usa broadcast conservativamente
        use_broadcast = True

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
