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
from src.config.tables import register_external_table
from src.config.tables import SCHEMA_BRONZE, SCHEMA_GOLD, SCHEMA_SILVER


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

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")
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
            # Sem DROP: mode("overwrite") ja substitui os dados, e o DROP
            # apagava o delta.enableChangeDataFeed habilitado adiante neste
            # mesmo job.
            if tabela in {"bcb", "world_bank"}:
                df = spark.read.option("basePath", path).format("parquet").load(f"{path}extracao=*/")
            else:
                df = spark.read.format("parquet").load(path)
            # fmt: off
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"{SCHEMA_BRONZE}.{tabela}")
            # fmt: on
            info("job_unity_catalog", f"   {SCHEMA_BRONZE}.{tabela} gravado")
        except Exception as e:
            info("job_unity_catalog", f"   {SCHEMA_BRONZE}.{tabela} → {e}")

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

    # Apenas as gold gravadas em PATH precisam ser registradas aqui.
    # acoes_vs_cambio saiu da lista: src/gold/correlacao_acoes_cambio.py
    # grava via saveAsTable (tabela gerenciada, sem path). O bloco antigo
    # fazia DROP TABLE e so entao tentava ler um path que ninguem escreve --
    # o read falhava, o except engolia, e a tabela do dia anterior ficava
    # destruida ate t3_acoes_cambio rodar no fim do pipeline.
    # fmt: off
    tabelas_gold = {
        "performance_acoes": f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/",
        "anomalias":         f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/",
    }
    # fmt: on

    for tabela, path in tabelas_gold.items():
        # Sem DROP: registra como tabela externa sobre o path, preservando
        # as propriedades da tabela (inclusive delta.enableChangeDataFeed).
        register_external_table(spark, "gold", tabela, path)
        info("job_unity_catalog", f"   {SCHEMA_GOLD}.{tabela} registrado")


    # Liquid Clustering: substitui particionamento estático por clustering
    # dinâmico — o Databricks decide o layout ideal dos arquivos por coluna,
    # sem necessidade de reescrever a tabela ao mudar a estratégia.
    info("job_unity_catalog", "\nAplicando Liquid Clustering...")
    # fmt: off
    tabelas_liquid = {
        f"{SCHEMA_SILVER}.acoes":              "ticker, ano, mes",
        f"{SCHEMA_SILVER}.ordens":             "hash_cliente, ticker",
        f"{SCHEMA_SILVER}.clientes":           "hash_cliente, perfil_risco",
        f"{SCHEMA_GOLD}.anomalias":            "ticker, data_processamento",
        f"{SCHEMA_GOLD}.performance_acoes":    "ticker, ano",
        f"{SCHEMA_GOLD}.deteccao_fraude":      "score_fraude, data_processamento",
        f"{SCHEMA_GOLD}.score_risco_clientes": "categoria_risco, hash_cliente",
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
        f"{SCHEMA_SILVER}.streaming",
        f"{SCHEMA_SILVER}.ordens",
        f"{SCHEMA_SILVER}.clientes",
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
