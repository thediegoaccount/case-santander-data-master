"""
Transformação Silver de ordens: tipagem de data e colunas de partição.

Lê:     <catalog>.<env>_bronze.ordens
Escreve: <catalog>.<env>_silver.ordens  (UMA tabela)
"""

from datetime import datetime

from pyspark.sql import functions as F

from src.config.logging import info
from src.config.tables import SCHEMA_BRONZE, SCHEMA_SILVER
from src.utils.delta import merge_ou_cria

CTX = "silver_ordens"


def transformar_ordens(spark) -> int:
    """Converte data_ordem para date, deriva ano/mes e faz upsert na silver."""
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # fmt: off
    df = spark.table(f"{SCHEMA_BRONZE}.ordens") \
        .withColumn("data_ordem", F.to_date("data_ordem")) \
        .withColumn("ano",        F.year("data_ordem")) \
        .withColumn("mes",        F.month("data_ordem")) \
        .withColumn("data_processamento", F.lit(data_hoje))
    # fmt: on

    merge_ou_cria(spark, df, f"{SCHEMA_SILVER}.ordens", "id_ordem", CTX)

    info(CTX, "Silver ordens gravado")
    return 0  # métricas no Spark UI
