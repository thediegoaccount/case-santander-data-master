"""
Transformação Silver de clientes: enriquecimento e faixas derivadas.

Lê:     <catalog>.<env>_bronze.clientes
Escreve: <catalog>.<env>_silver.clientes  (UMA tabela)
"""

from datetime import datetime

from pyspark.sql import functions as F

from src.config.logging import info
from src.config.tables import SCHEMA_BRONZE, SCHEMA_SILVER
from src.utils.delta import merge_ou_cria

CTX = "silver_clientes"


def transformar_clientes(spark) -> int:
    """Aplica faixa etária e categoria de score, e faz upsert na silver."""
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # fmt: off
    df = spark.table(f"{SCHEMA_BRONZE}.clientes") \
        .withColumn("faixa_etaria",
            F.when(F.col("idade") < 30, "Jovem")
            .when(F.col("idade") < 50, "Adulto")
            .otherwise("Senior")) \
        .withColumn("score_categoria",
            F.when(F.col("score_credito") >= 750, "Excelente")
            .when(F.col("score_credito") >= 650, "Bom")
            .when(F.col("score_credito") >= 550, "Regular")
            .otherwise("Ruim")) \
        .withColumn("data_processamento", F.lit(data_hoje))
    # fmt: on

    merge_ou_cria(spark, df, f"{SCHEMA_SILVER}.clientes", "hash_cliente", CTX)

    info(CTX, "Silver clientes gravado")
    return 0  # métricas no Spark UI
