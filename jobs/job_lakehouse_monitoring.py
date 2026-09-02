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
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_lakehouse_monitoring", f"=== JOB LAKEHOUSE MONITORING INICIADO: {inicio} ===")

    w = WorkspaceClient()

    tabelas = [
        f"{SCHEMA_GOLD}.anomalias",
        f"{SCHEMA_GOLD}.posicao_clientes",
        f"{SCHEMA_GOLD}.score_risco_clientes",
        f"{SCHEMA_GOLD}.deteccao_fraude",
        f"{SCHEMA_GOLD}.fraude_streaming",
        f"{SCHEMA_GOLD}.anomalias_intraday",
        f"{SCHEMA_GOLD}.volume_intraday",
        f"{SCHEMA_GOLD}.ranking_acoes_realtime",
        f"{SCHEMA_SILVER}.clientes",
        f"{SCHEMA_SILVER}.ordens",
        f"{SCHEMA_SILVER}.streaming",
    ]

    info("job_lakehouse_monitoring", "Verificando monitores Lakehouse Monitoring...")
    for tabela in tabelas:
        try:
            monitor = w.lakehouse_monitors.create(
                full_name=tabela,
                assets_dir=f"/Shared/monitoring/{tabela.replace('.', '/')}",
                output_schema_name=f"{SCHEMA_GOLD}",
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

if __name__ == "__main__":
    main()
