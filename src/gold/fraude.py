"""
Deteccao de fraude em ordens de clientes
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def detectar_fraude(spark: SparkSession) -> int:
    """
    Detecta ordens suspeitas cruzando dados de ordens
    com score de risco dos clientes.

    Returns: total de ordens criticas detectadas
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print("Detectando fraudes...")

    df_ordens = spark.sql("SELECT * FROM case_santander.silver.ordens")
    df_score = spark.sql(
        """
        SELECT hash_cliente, score_risco,
               categoria_risco, limite_operacional
        FROM case_santander.gold.score_risco_clientes
    """
    )

    # df_score tem ~7.900 linhas com apenas 4 colunas — cabe em broadcast
    # evitando shuffle de df_ordens (5.341 linhas) em sort-merge join
    # fmt: off
    df_fraude = df_ordens \
        .join(F.broadcast(df_score), on="hash_cliente", how="left") \
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
        .saveAsTable("case_santander.gold.deteccao_fraude")
    # fmt: on

    total_critico = df_fraude.filter(F.col("score_fraude") == "Critico").count()
    print(f"Gold fraude gravado: {df_fraude.count()} registros ({total_critico} criticos)")
    return total_critico
