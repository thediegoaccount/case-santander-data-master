"""
DAG: Pipeline Corretora Santander (Sincronizado com Databricks Asset Bundles)
Gerado automaticamente via scripts/sync_airflow_from_databricks.py

 NÃO EDITE MANUALMENTE - Alterações devem ser feitas em databricks.yml
Este DAG reflete as dependências do workflow pai pipeline_completo
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

# Configurações do Databricks (lidas de variáveis de ambiente)
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0401-150803-wefgy1hc")
REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hk")

default_args = {
    "owner": "santander",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def databricks_task(task_id: str, job_path: str) -> DatabricksSubmitRunOperator:
    """Cria task do Airflow para executar job no Databricks"""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": CLUSTER_ID,
            "spark_python_task": {
                "python_file": f"{REPO_PATH}/{job_path}",
            },
        },
    )


with DAG(
    dag_id="pipeline_corretora_santander",
    default_args=default_args,
    description=f"Pipeline de dados financeiros — Corretora Santander (Ambiente: {ENVIRONMENT})",
    schedule_interval="0 6 * * *",  # 06:00 Brasília
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santander", "databricks", "financeiro", "synced", ENVIRONMENT],
) as dag:

    # Setup Unity Catalog e schemas
    t0_unity_catalog = databricks_task(
        task_id="t0_unity_catalog",
        job_path="jobs/job_unity_catalog_schemas.py"
    )

    # Extração Yahoo Finance
    t1_extracao_acoes = databricks_task(
        task_id="t1_extracao_acoes",
        job_path="jobs/job_extracao_acoes.py"
    )

    # Extração BCB API
    t1_extracao_bcb = databricks_task(
        task_id="t1_extracao_bcb",
        job_path="jobs/job_extracao_bcb.py"
    )

    # Extração World Bank API
    t1_extracao_world_bank = databricks_task(
        task_id="t1_extracao_world_bank",
        job_path="jobs/job_extracao_world_bank.py"
    )

    # Clientes e Ordens Kaggle
    t6_clientes_ordens = databricks_task(
        task_id="t6_clientes_ordens",
        job_path="jobs/job_clientes_ordens.py"
    )

    # Transformação Silver Ações
    t2_silver_acoes = databricks_task(
        task_id="t2_silver_acoes",
        job_path="jobs/job_silver_acoes.py"
    )

    # Transformação Silver BCB
    t2_silver_bcb = databricks_task(
        task_id="t2_silver_bcb",
        job_path="jobs/job_silver_bcb.py"
    )

    # Transformação Silver World Bank
    t2_silver_world_bank = databricks_task(
        task_id="t2_silver_world_bank",
        job_path="jobs/job_silver_world_bank.py"
    )

    # Transformação Silver Clientes

    # Gold Anomalias
    t3_anomalias = databricks_task(
        task_id="t3_anomalias",
        job_path="jobs/job_gold_anomalias.py"
    )

    # Gold Performance
    t3_performance = databricks_task(
        task_id="t3_performance",
        job_path="jobs/job_gold_performance.py"
    )

    # Gold BCB
    t3_bcb = databricks_task(
        task_id="t3_bcb",
        job_path="jobs/job_gold_bcb.py"
    )

    # Gold World Bank
    t3_world_bank = databricks_task(
        task_id="t3_world_bank",
        job_path="jobs/job_gold_world_bank.py"
    )

    # Gold Ações vs Câmbio
    t3_acoes_cambio = databricks_task(
        task_id="t3_acoes_cambio",
        job_path="jobs/job_gold_acoes_vs_cambio.py"
    )

    # Corretora Análises
    t7_corretora_analises = databricks_task(
        task_id="t7_corretora_analises",
        job_path="jobs/job_corretora_analises.py"
    )

    # SCD Type 2
    t9_scd = databricks_task(
        task_id="t9_scd",
        job_path="jobs/job_scd.py"
    )

    # Gold Fraude
    t3_fraude = databricks_task(
        task_id="t3_fraude",
        job_path="jobs/job_gold_fraude.py"
    )

    # Lakehouse Monitoring
    t8_lakehouse_monitoring = databricks_task(
        task_id="t8_lakehouse_monitoring",
        job_path="jobs/job_lakehouse_monitoring.py"
    )

    # Registro das tabelas no Unity Catalog (depois dos produtores)
    t8b_uc_registro = databricks_task(
        task_id="t8b_uc_registro",
        job_path="jobs/job_unity_catalog.py"
    )

    # Observabilidade
    t4_observabilidade = databricks_task(
        task_id="t4_observabilidade",
        job_path="jobs/job_observabilidade.py"
    )

    #  Dependências (Sincronizadas do databricks.yml) 
    [t0_unity_catalog] >> t1_extracao_acoes
    [t0_unity_catalog] >> t1_extracao_bcb
    [t0_unity_catalog] >> t1_extracao_world_bank
    [t0_unity_catalog] >> t6_clientes_ordens
    [t1_extracao_acoes] >> t2_silver_acoes
    [t1_extracao_bcb] >> t2_silver_bcb
    [t1_extracao_world_bank] >> t2_silver_world_bank
    [t2_silver_acoes] >> t3_anomalias
    [t2_silver_acoes] >> t3_performance
    [t2_silver_bcb] >> t3_bcb
    [t2_silver_world_bank] >> t3_world_bank
    [t2_silver_acoes, t2_silver_bcb] >> t3_acoes_cambio
    [t6_clientes_ordens] >> t7_corretora_analises
    [t7_corretora_analises] >> t9_scd
    [t7_corretora_analises] >> t3_fraude
    [t3_anomalias, t3_performance, t3_bcb, t3_world_bank,
     t3_acoes_cambio, t3_fraude, t9_scd] >> t8_lakehouse_monitoring
    [t8_lakehouse_monitoring] >> t8b_uc_registro
    [t8b_uc_registro] >> t4_observabilidade

# Este DAG foi gerado automaticamente a partir de databricks.yml
# Reflete as dependências do workflow pai pipeline_completo
# Para regerar: python scripts/sync_airflow_from_databricks.py
# Para modificar: Edite databricks.yml e rode o script novamente
