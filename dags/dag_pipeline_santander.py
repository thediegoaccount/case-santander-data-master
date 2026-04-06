"""
DAG: Pipeline Corretora Santander
Orquestra o pipeline completo de dados financeiros via Airflow + Databricks.
"""
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
import os

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CLUSTER_ID       = "0401-150803-wefgy1hc"
REPO_PATH        = "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master"

default_args = {
    "owner":            "santander",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

def make_task(task_id: str, job_path: str) -> dict:
    """Cria configuracao de task para Databricks"""
    return {
        "existing_cluster_id": CLUSTER_ID,
        "spark_python_task": {
            "python_file": f"{REPO_PATH}/{job_path}",
        }
    }


with DAG(
    dag_id="pipeline_corretora_santander",
    default_args=default_args,
    description="Pipeline de dados financeiros — Corretora Santander",
    schedule_interval="0 6 * * *",  # 06:00 Brasília
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santander", "databricks", "financeiro"]
) as dag:

    t0 = DatabricksSubmitRunOperator(
        task_id="t0_unity_catalog",
        databricks_conn_id="databricks_default",
        json=make_task("t0_unity_catalog", "jobs/job_unity_catalog.py")
    )

    t1 = DatabricksSubmitRunOperator(
        task_id="t1_extracao",
        databricks_conn_id="databricks_default",
        json=make_task("t1_extracao", "jobs/job_extracao.py")
    )

    t5 = DatabricksSubmitRunOperator(
        task_id="t5_streaming",
        databricks_conn_id="databricks_default",
        json=make_task("t5_streaming", "jobs/job_streaming.py")
    )

    t6 = DatabricksSubmitRunOperator(
        task_id="t6_clientes_ordens",
        databricks_conn_id="databricks_default",
        json=make_task("t6_clientes_ordens", "jobs/job_clientes_ordens.py")
    )

    t2 = DatabricksSubmitRunOperator(
        task_id="t2_silver",
        databricks_conn_id="databricks_default",
        json=make_task("t2_silver", "jobs/job_silver.py")
    )

    t7 = DatabricksSubmitRunOperator(
        task_id="t7_corretora_analises",
        databricks_conn_id="databricks_default",
        json=make_task("t7_corretora_analises", "jobs/job_corretora_analises.py")
    )

    t9 = DatabricksSubmitRunOperator(
        task_id="t9_scd",
        databricks_conn_id="databricks_default",
        json=make_task("t9_scd", "jobs/job_scd.py")
    )

    t3 = DatabricksSubmitRunOperator(
        task_id="t3_gold",
        databricks_conn_id="databricks_default",
        json=make_task("t3_gold", "jobs/job_gold.py")
    )

    t10 = DatabricksSubmitRunOperator(
        task_id="t10_streaming_gold",
        databricks_conn_id="databricks_default",
        json=make_task("t10_streaming_gold", "jobs/job_streaming_to_gold.py")
    )

    t8 = DatabricksSubmitRunOperator(
        task_id="t8_lakehouse_monitoring",
        databricks_conn_id="databricks_default",
        json=make_task("t8_lakehouse_monitoring", "jobs/job_lakehouse_monitoring.py")
    )

    t4 = DatabricksSubmitRunOperator(
        task_id="t4_observabilidade",
        databricks_conn_id="databricks_default",
        json=make_task("t4_observabilidade", "jobs/job_observabilidade.py")
    )

    t_sql = DatabricksSubmitRunOperator(
        task_id="t_carga_sql",
        databricks_conn_id="databricks_default",
        json=make_task("t_carga_sql", "jobs/job_carga_sql.py")
    )

    # Dependências
    t0 >> t1 >> [t5, t6]
    t5 >> t2
    t6 >> [t2, t7]
    t7 >> t9
    [t2, t9] >> t3
    t3 >> t10
    t10 >> [t8, t_sql]
    [t8, t_sql] >> t4
