"""
Geração Automática de DAG Airflow via DynamicPipeline

Gera DAG Airflow automaticamente do databricks.yml usando DynamicPipeline.
"""

import sys
from pathlib import Path

# Configurar path
from src.config.environment import setup_python_path
setup_python_path()

from src.pipeline.dynamic_pipeline import auto_generate_dag


def main():
    """
    Gera DAG Airflow automaticamente do databricks.yml
    """
    databricks_yml_path = "databricks.yml"
    output_path = "dags/dag_pipeline_santander_auto.py"
    
    print("=== Geração Automática de DAG Airflow ===")
    print(f"Lendo: {databricks_yml_path}")
    print(f"Gerando: {output_path}")
    print()
    
    # Gerar DAG automaticamente
    auto_generate_dag(
        databricks_yml_path=databricks_yml_path,
        output_path=output_path
    )
    
    print()
    print("=== DAG Gerado com Sucesso ===")
    print(f"Arquivo: {output_path}")
    print()
    print("Próximo passo:")
    print("1. Copiar DAG para Airflow DAGs folder")
    print("2. Restart Airflow scheduler")
    print("3. Verificar DAG no Airflow UI")


if __name__ == "__main__":
    main()
