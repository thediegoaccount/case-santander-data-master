from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.tables import register_external_table
from src.quality.data_quality import DataQualityValidator


def transformar_bcb(spark: SparkSession, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/bcb/"

    print("Transformando BCB Bronze -> Silver...")

    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}extracao=*/")
    print("Schema Bronze:")
    df.printSchema()
    print("Amostra:")
    df.show(3)

    # fmt: off
    df_silver = df \
        .withColumn("data",      F.to_date("data", "dd/MM/yyyy")) \
        .withColumn("ano",       F.year("data")) \
        .withColumn("mes",       F.month("data")) \
        .withColumn("trimestre", F.quarter("data")) \
        .withColumn("valor",     F.round("valor", 6)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("data").isNotNull()) \
        .dropDuplicates(["data", "indicador"]) \
        .drop("extracao")
    # fmt: on

    # fmt: off
    # Gate de qualidade ANTES de publicar. O framework de 228 linhas em
    # src/quality/data_quality.py nao tinha um unico call site em codigo
    # executavel -- so aparecia em documentacao. Falha aqui aborta o job em
    # vez de propagar dado ruim para a camada gold.
    DataQualityValidator("silver_bcb").run_all_validations(df_silver, {
        "completeness": {"required_columns": ["data", "indicador", "valor"]},
        "row_count": {"min_rows": 1},
    })

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)
    # fmt: on

    # Registra o path como tabela externa no Unity Catalog.
    # Os consumidores gold leem via `FROM <catalog>.<env>_silver.bcb`, mas
    # nada criava essa tabela: as transformacoes gravavam so em path. Tres
    # tasks gold morriam com TABLE_OR_VIEW_NOT_FOUND. Quem escreve registra,
    # entao a ordem fica correta por construcao.
    register_external_table(spark, "silver", "bcb", silver_path)

    print("Silver BCB gravado")
    return 0  # Metrics in Spark UI
