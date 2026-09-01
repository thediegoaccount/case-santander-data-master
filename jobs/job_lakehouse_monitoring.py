"""
Job: Lakehouse Monitoring
Verifica e cria monitores de qualidade nas tabelas Delta.

Ou via Databricks Workflow:
    Task: t8_lakehouse_monitoring
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.sdk import WorkspaceClient


def main():
    inicio = datetime.now()
    info("job_lakehouse_monitoring", f"=== JOB LAKEHOUSE MONITORING INICIADO: {inicio} ===")

    w = WorkspaceClient()

    tabelas = [
        "case_santander.gold.anomalias",
        "case_santander.gold.posicao_clientes",
        "case_santander.gold.score_risco_clientes",
        "case_santander.gold.deteccao_fraude",
        "case_santander.gold.fraude_streaming",
        "case_santander.gold.anomalias_intraday",
        "case_santander.gold.volume_intraday",
        "case_santander.gold.ranking_acoes_realtime",
        "case_santander.silver.clientes",
        "case_santander.silver.ordens",
        "case_santander.silver.streaming",
    ]

    info("job_lakehouse_monitoring", "Verificando monitores Lakehouse Monitoring...")
    for tabela in tabelas:
        try:
            monitor = w.lakehouse_monitors.create(
                full_name=tabela,
                assets_dir=f"/Shared/monitoring/{tabela.replace('.', '/')}",
                output_schema_name="case_santander.gold",
                snapshot={},
            )
            info("job_lakehouse_monitoring", f"   Monitor criado: {tabela} → {monitor.status}")
        except Exception as e:
            if "already exists" in str(e).lower():
                monitor = w.lakehouse_monitors.get(full_name=tabela)
                info("job_lakehouse_monitoring", f"  ℹ Monitor existente: {tabela} → {monitor.status}")
            else:
                info("job_lakehouse_monitoring", f"   Erro em {tabela}: {e}")

    fim = datetime.now()
    info("job_lakehouse_monitoring", "\n=== JOB LAKEHOUSE MONITORING CONCLUIDO ===")
    info("job_lakehouse_monitoring", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
