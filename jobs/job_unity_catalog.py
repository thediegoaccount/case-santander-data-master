"""
Job: Unity Catalog — Bronze
Registra todas as tabelas Bronze no Unity Catalog.

Ou via Databricks Workflow:
    Task: t0_unity_catalog_bronze
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime


def main():
    inicio = datetime.now()
    print(f"=== JOB UNITY CATALOG INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    # Credenciais via Key Vault
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    # Habilitar mergeSchema globalmente
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    # Criar catálogo e schemas
    spark.sql("CREATE CATALOG IF NOT EXISTS case_santander")
    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS case_santander.gold")
    print("✅ Catálogo e schemas criados!")

    # Registrar tabelas Bronze
    tabelas_bronze = {
        "acoes":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/",
        "bcb":        f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/",
        "world_bank": f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/",
        "kafka":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/",
        "clientes":   f"abfss://bronze@{storage_account}.dfs.core.windows.net/clientes/",
        "ordens":     f"abfss://bronze@{storage_account}.dfs.core.windows.net/ordens/",
    }

    for tabela, path in tabelas_bronze.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS case_santander.bronze.{tabela}")
            df = spark.read.format("parquet").load(path)
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"case_santander.bronze.{tabela}")
            count = spark.sql(f"SELECT COUNT(*) as total FROM case_santander.bronze.{tabela}").collect()[0]["total"]
            print(f"  ✅ case_santander.bronze.{tabela} → {count} registros")
        except Exception as e:
            print(f"  ⚠️ case_santander.bronze.{tabela} → {e}")

    # Registrar tabelas Gold via ADLS
    tabelas_gold = {
        "performance_acoes": f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/",
        "anomalias":         f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/",
        "acoes_vs_cambio":   f"abfss://gold@{storage_account}.dfs.core.windows.net/acoes_vs_cambio/",
    }

    for tabela, path in tabelas_gold.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS case_santander.gold.{tabela}")
            df = spark.read.format("delta").load(path)
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"case_santander.gold.{tabela}")
            count = spark.sql(f"SELECT COUNT(*) as total FROM case_santander.gold.{tabela}").collect()[0]["total"]
            print(f"  ✅ case_santander.gold.{tabela} → {count} registros")
        except Exception as e:
            print(f"  ⚠️ case_santander.gold.{tabela} → {e}")

    fim = datetime.now()
    print(f"\n=== JOB UNITY CATALOG CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
