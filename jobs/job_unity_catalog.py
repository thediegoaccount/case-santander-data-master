"""
Job: Unity Catalog — Bronze
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret

from src.config.settings import configure_adls


def main():
    inicio = datetime.now()
    info("job_unity_catalog", f"=== JOB UNITY CATALOG INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.gold")
    info("job_unity_catalog", " Schemas verificados!")

    # Tabelas Bronze em formato Parquet
    # fmt: off
    tabelas_bronze_parquet = {
        "acoes":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/",
        "bcb":        f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/",
        "world_bank": f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/",
        "kafka":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/",
    }

    # Tabelas Bronze em formato Delta
    # tabelas_bronze_delta = {
    #    "clientes": f"abfss://bronze@{storage_account}.dfs.core.windows.net/clientes/",
    #    "ordens":   f"abfss://bronze@{storage_account}.dfs.core.windows.net/ordens/",
    # }
    # fmt: on

    for tabela, path in tabelas_bronze_parquet.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS case_santander.bronze.{tabela}")
            if tabela in {"bcb", "world_bank"}:
                df = spark.read.option("basePath", path).format("parquet").load(f"{path}extracao=*/")
            else:
                df = spark.read.format("parquet").load(path)
            # fmt: off
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"case_santander.bronze.{tabela}")
            # fmt: on
            info("job_unity_catalog", f"   case_santander.bronze.{tabela} gravado")
        except Exception as e:
            info("job_unity_catalog", f"   case_santander.bronze.{tabela} → {e}")

    # for tabela, path in tabelas_bronze_delta.items():
    #    try:
    #        spark.sql(f"DROP TABLE IF EXISTS case_santander.bronze.{tabela}")
    #        df = spark.read.format("delta").load(path)
    #        # fmt: off
    #        df.write \
    #            .format("delta") \
    #            .mode("overwrite") \
    #            .option("mergeSchema", "true") \
    #            .saveAsTable(f"case_santander.bronze.{tabela}")
    #        # fmt: on
    #        count = spark.sql(f"SELECT COUNT(*) as total FROM case_santander.bronze.{tabela}").collect()[0]["total"]
    #        info("job_unity_catalog", f"   case_santander.bronze.{tabela} → {count} registros")
    #    except Exception as e:
    #        info("job_unity_catalog", f"   case_santander.bronze.{tabela} → {e}")
    # clientes e ordens são criados posteriormente por job_clientes_ordens.py

    # como tabelas gerenciadas no Unity Catalog, sem leitura de paths ADLS aqui.
    # Tabelas Gold

    # fmt: off
    tabelas_gold = {
        "performance_acoes": f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/",
        "anomalias":         f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/",
        "acoes_vs_cambio":   f"abfss://gold@{storage_account}.dfs.core.windows.net/acoes_vs_cambio/",
    }
    # fmt: on

    for tabela, path in tabelas_gold.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS case_santander.gold.{tabela}")
            df = spark.read.format("delta").load(path)
            # fmt: off
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"case_santander.gold.{tabela}")
            # fmt: on
            info("job_unity_catalog", f"   case_santander.gold.{tabela} gravado")
        except Exception as e:
            info("job_unity_catalog", f"   case_santander.gold.{tabela} → {e}")

    # Liquid Clustering: substitui particionamento estático por clustering
    # dinâmico — o Databricks decide o layout ideal dos arquivos por coluna,
    # sem necessidade de reescrever a tabela ao mudar a estratégia.
    info("job_unity_catalog", "\nAplicando Liquid Clustering...")
    # fmt: off
    tabelas_liquid = {
        "case_santander.silver.acoes":              "ticker, ano, mes",
        "case_santander.silver.ordens":             "hash_cliente, ticker",
        "case_santander.silver.clientes":           "hash_cliente, perfil_risco",
        "case_santander.gold.anomalias":            "ticker, data_processamento",
        "case_santander.gold.performance_acoes":    "ticker, ano",
        "case_santander.gold.deteccao_fraude":      "score_fraude, data_processamento",
        "case_santander.gold.score_risco_clientes": "categoria_risco, hash_cliente",
    }
    # fmt: on

    for tabela, cols in tabelas_liquid.items():
        try:
            spark.sql(f"ALTER TABLE {tabela} CLUSTER BY ({cols})")
            info("job_unity_catalog", f"   Liquid Clustering: {tabela} → ({cols})")
        except Exception as e:
            info("job_unity_catalog", f"   {tabela}: {e}")

    # Delta Change Data Feed (CDC): habilita rastreamento de mudanças
    # a nível de linha (insert/update/delete) nas tabelas Silver críticas.
    # Permite leitura incremental por versão ou timestamp nos jobs Gold.
    info("job_unity_catalog", "\nHabilitando Delta Change Data Feed (CDC)...")
    tabelas_cdf = [
        "case_santander.silver.streaming",
        "case_santander.silver.ordens",
        "case_santander.silver.clientes",
    ]

    for tabela in tabelas_cdf:
        try:
            # fmt: off
            spark.sql(f"""
                ALTER TABLE {tabela}
                SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)
            # fmt: on
            info("job_unity_catalog", f"   CDF habilitado: {tabela}")
        except Exception as e:
            info("job_unity_catalog", f"   {tabela}: {e}")

    fim = datetime.now()
    info("job_unity_catalog", "\n=== JOB UNITY CATALOG CONCLUIDO ===")
    info("job_unity_catalog", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
