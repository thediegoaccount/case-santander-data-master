"""
DAG: Pipeline Corretora Santander
Orquestra o pipeline completo de dados financeiros via Airflow + Databricks.

Fluxo de dados:
  [setup]
    t0_unity_catalog
  [ingestion]  — todos em paralelo após t0
    t1_extracao_acoes | t1_extracao_bcb | t1_extracao_world_bank | t5_streaming | t6_clientes_ordens
  [silver]  — cada fonte alimenta apenas sua própria Silver
    t1_acoes → t2_silver_acoes
    t1_bcb   → t2_silver_bcb
    t1_world_bank → t2_silver_world_bank
    t6       → t6_clientes_silver
  [gold]  — pipelines paralelos
    [t2_acoes, t2_bcb, t2_world_bank] → t3_gold          (anomalias, performance, fraude de mercado)
    t6_silver → t7_corretora_analises → t9_scd            (posição, score, SCD clientes)
    [t3_gold, t5_streaming] → t10_streaming_gold          (fraude/anomalias intraday)
  [finalizacao]
    [t3, t7, t9, t10] → t8_lakehouse_monitoring | t_carga_sql → t4_observabilidade
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

CLUSTER_ID = "0401-150803-wefgy1hc"
REPO_PATH  = "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master"

default_args = {
    "owner":            "santander",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def databricks_task(task_id: str, job_path: str) -> DatabricksSubmitRunOperator:
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
    description="Pipeline de dados financeiros — Corretora Santander",
    schedule_interval="0 6 * * *",  # 06:00 Brasília
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santander", "databricks", "financeiro"],
) as dag:

    # ── Setup ──────────────────────────────────────────────────────────────
    t0 = databricks_task("t0_unity_catalog", "jobs/job_unity_catalog.py")

    # ── Ingestion ──────────────────────────────────────────────────────────
    with TaskGroup("ingestion") as tg_ingestion:
        t1_acoes      = databricks_task("t1_extracao_acoes",      "jobs/job_extracao_acoes.py")
        t1_bcb        = databricks_task("t1_extracao_bcb",        "jobs/job_extracao_bcb.py")
        t1_world_bank = databricks_task("t1_extracao_world_bank", "jobs/job_extracao_world_bank.py")
        t5            = databricks_task("t5_streaming",            "jobs/job_streaming.py")
        t6            = databricks_task("t6_clientes_ordens",      "jobs/job_clientes_ordens.py")

    # ── Silver ─────────────────────────────────────────────────────────────
    with TaskGroup("silver") as tg_silver:
        t2_acoes      = databricks_task("t2_silver_acoes",       "jobs/job_silver_acoes.py")
        t2_bcb        = databricks_task("t2_silver_bcb",         "jobs/job_silver_bcb.py")
        t2_world_bank = databricks_task("t2_silver_world_bank",  "jobs/job_silver_world_bank.py")
        t6_silver     = databricks_task("t6_clientes_silver",    "jobs/job_clientes_silver.py")

    # ── Gold ───────────────────────────────────────────────────────────────
    with TaskGroup("gold") as tg_gold:
        t3  = databricks_task("t3_gold",               "jobs/job_gold.py")
        t7  = databricks_task("t7_corretora_analises", "jobs/job_corretora_analises.py")
        t9  = databricks_task("t9_scd",                "jobs/job_scd.py")
        t10 = databricks_task("t10_streaming_gold",    "jobs/job_streaming_to_gold.py")

    # ── Finalização ────────────────────────────────────────────────────────
    with TaskGroup("finalizacao") as tg_finalizacao:
        t8    = databricks_task("t8_lakehouse_monitoring", "jobs/job_lakehouse_monitoring.py")
        t_sql = databricks_task("t_carga_sql",             "jobs/job_carga_sql.py")
        t4    = databricks_task("t4_observabilidade",      "jobs/job_observabilidade.py")

    # ── Dependências ───────────────────────────────────────────────────────

    # Setup → todas as ingestões em paralelo (streaming e clientes NÃO dependem de mercado)
    t0 >> [t1_acoes, t1_bcb, t1_world_bank, t5, t6]

    # Bronze → Silver: cada fonte alimenta apenas sua própria transformação
    t1_acoes      >> t2_acoes
    t1_bcb        >> t2_bcb
    t1_world_bank >> t2_world_bank
    t6            >> t6_silver

    # Silver → Gold: pipelines de mercado e clientes correm em paralelo
    [t2_acoes, t2_bcb, t2_world_bank] >> t3   # mercado: anomalias, performance, fraude
    t6_silver >> t7                             # clientes: posição, score risco, perfil, ordens
    t7 >> t9                                   # SCD type-2 após gold.score_risco_clientes existir

    # Streaming Gold precisa de silver.streaming (t5) E gold.performance_acoes (t3)
    [t3, t5] >> t10

    # Finalização: monitora e carrega SQL após todos os Gold estarem prontos
    [t3, t7, t9, t10] >> [t8, t_sql]
    [t8, t_sql] >> t4
