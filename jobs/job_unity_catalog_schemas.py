"""
Job: Unity Catalog - Criação de Schemas

Cria apenas os schemas do catálogo. Sem dependência de dado nenhum, então
pode (e deve) ser a raiz do pipeline.

Existe porque job_unity_catalog.py fazia duas coisas incompatíveis na mesma
task: criar schemas (sem dependência) e registrar tabelas a partir de paths
bronze/gold (que só existem DEPOIS dos produtores rodarem). Como ele era a
raiz do DAG, na primeira execução todos os reads falhavam e as tabelas nunca
eram criadas; nas seguintes, registravam um snapshot do dia anterior.

Agora:
  t0  -> este job (schemas)
  ... produtores ...
  t8b -> job_unity_catalog (registro das tabelas + propriedades)

Produz:
  - <catalog>.<env>_bronze
  - <catalog>.<env>_silver
  - <catalog>.<env>_gold
"""

from datetime import datetime

from databricks.connect import DatabricksSession

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.tables import SCHEMA_BRONZE, SCHEMA_GOLD, SCHEMA_SILVER

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_unity_catalog_schemas", f"=== JOB UNITY CATALOG SCHEMAS INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    for schema in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        info("job_unity_catalog_schemas", f"   schema pronto: {schema}")

    fim = datetime.now()
    info("job_unity_catalog_schemas", "\n=== JOB UNITY CATALOG SCHEMAS CONCLUIDO ===")
    info("job_unity_catalog_schemas", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
