from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.tables import register_external_table
from src.quality.data_quality import DataQualityValidator


def transformar_world_bank(spark: SparkSession, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/"
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/"

    print("Transformando World Bank Bronze -> Silver...")

    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}extracao=*/")
    print("Colunas disponíveis:", df.columns)

    # Compatível com schema antigo (data) e novo (ano)
    if "ano" in df.columns:
        df = df.withColumn("ano", F.col("ano").cast("integer"))
    elif "data" in df.columns:
        df = df.withColumn("ano", F.col("data").cast("integer"))
    else:
        raise Exception("Coluna de ano não encontrada no Bronze!")

    # fmt: off
    df_silver = df \
        .withColumn("valor", F.round("valor", 4)) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .filter(F.col("valor").isNotNull()) \
        .filter(F.col("ano").isNotNull())

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)
    # fmt: on

    # Gate de qualidade ANTES de publicar. O framework de 228 linhas em
    # src/quality/data_quality.py nao tinha um unico call site em codigo
    # executavel -- so aparecia em documentacao. Falha aqui aborta o job em
    # vez de propagar dado ruim para a camada gold.
    DataQualityValidator("silver_world_bank").run_all_validations(df_silver, {
        "completeness": {"required_columns": ["ano", "indicador", "valor"]},
        "row_count": {"min_rows": 1},
    })

    # Registra o path como tabela externa no Unity Catalog.
    # Os consumidores gold leem via `FROM <catalog>.<env>_silver.world_bank`, mas
    # nada criava essa tabela: as transformacoes gravavam so em path. Tres
    # tasks gold morriam com TABLE_OR_VIEW_NOT_FOUND. Quem escreve registra,
    # entao a ordem fica correta por construcao.
    register_external_table(spark, "silver", "world_bank", silver_path)

    print("Silver World Bank gravado")
    return 0  # Metrics in Spark UI
