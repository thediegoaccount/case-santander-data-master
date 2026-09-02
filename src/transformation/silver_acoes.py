"""
Transformacao Bronze -> Silver para dados de acoes B3
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.tables import register_external_table
from src.quality.data_quality import DataQualityValidator


def transformar_acoes(spark: SparkSession, storage_account: str) -> int:
    """
    Le dados brutos de acoes do Bronze,
    aplica transformacoes e grava no Silver em Delta Lake.

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/acoes/"

    print("Transformando acoes Bronze -> Silver...")

    df = spark.read.parquet(bronze_path)

    # fmt: off
    df_silver = df \
        .withColumn("date", F.to_date("date")) \
        .withColumn("ano",       F.year("date")) \
        .withColumn("mes",       F.month("date")) \
        .withColumn("trimestre", F.quarter("date")) \
        .withColumn("open",  F.round("open",  2)) \
        .withColumn("high",  F.round("high",  2)) \
        .withColumn("low",   F.round("low",   2)) \
        .withColumn("close", F.round("close", 2)) \
        .withColumn("variacao_diaria_pct",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4)) \
        .withColumn("amplitude_diaria",
            F.round(F.col("high") - F.col("low"), 2)) \
        .withColumn("empresa",
            F.when(F.col("ticker") == "PETR4.SA",  "Petrobras")
            .when(F.col("ticker") == "VALE3.SA",  "Vale")
            .when(F.col("ticker") == "ITUB4.SA",  "Itau")
            .when(F.col("ticker") == "BBDC4.SA",  "Bradesco")
            .when(F.col("ticker") == "ABEV3.SA",  "Ambev")
            .when(F.col("ticker") == "MGLU3.SA",  "Magazine Luiza")
            .when(F.col("ticker") == "WEGE3.SA",  "WEG")
            .when(F.col("ticker") == "BBAS3.SA",  "Banco do Brasil")
            .when(F.col("ticker") == "SANB11.SA", "Santander")
            .otherwise("Desconhecido")) \
        .withColumn("setor",
            F.when(F.col("ticker").isin("PETR4.SA", "VALE3.SA"), "Commodities")
            .when(F.col("ticker").isin("ITUB4.SA", "BBDC4.SA", "BBAS3.SA", "SANB11.SA"), "Financeiro")
            .when(F.col("ticker") == "ABEV3.SA", "Consumo")
            .when(F.col("ticker") == "MGLU3.SA", "Varejo")
            .when(F.col("ticker") == "WEGE3.SA", "Industria")
            .otherwise("Outros")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("close").isNotNull()) \
        .filter(F.col("volume") > 0) \
        .drop("dividends", "stock_splits")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("ano", "mes") \
        .save(silver_path)
    # fmt: on

    # Gate de qualidade ANTES de publicar. O framework de 228 linhas em
    # src/quality/data_quality.py nao tinha um unico call site em codigo
    # executavel -- so aparecia em documentacao. Falha aqui aborta o job em
    # vez de propagar dado ruim para a camada gold.
    DataQualityValidator("silver_acoes").run_all_validations(df_silver, {
        "completeness": {"required_columns": ["date", "ticker", "close", "volume"]},
        "row_count": {"min_rows": 1},
        "nulls": {"max_null_percentage": 0.05},
    })

    # Registra o path como tabela externa no Unity Catalog.
    # Os consumidores gold leem via `FROM <catalog>.<env>_silver.acoes`, mas
    # nada criava essa tabela: as transformacoes gravavam so em path. Tres
    # tasks gold morriam com TABLE_OR_VIEW_NOT_FOUND. Quem escreve registra,
    # entao a ordem fica correta por construcao.
    register_external_table(spark, "silver", "acoes", silver_path)

    print("Silver acoes gravado")
    return 0  # Metrics in Spark UI
