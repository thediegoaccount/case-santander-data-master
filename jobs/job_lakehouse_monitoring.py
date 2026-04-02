"""
Job: Lakehouse Monitoring
Verifica e cria monitores de qualidade nas tabelas Delta.

Ou via Databricks Workflow:
    Task: t8_lakehouse_monitoring
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from datetime import datetime


def main():
    inicio = datetime.now()
    print(f"=== JOB LAKEHOUSE MONITORING INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()
    w     = WorkspaceClient()

    tabelas = [
        "case_santander.gold.anomalias",
        "case_santander.gold.posicao_clientes",
        "case_santander.gold.score_risco_clientes",
        "case_santander.gold.deteccao_fraude",
        "case_santander.silver.clientes",
        "case_santander.silver.ordens"
    ]

    print("Verificando monitores Lakehouse Monitoring...")
    for tabela in tabelas:
        try:
            monitor = w.lakehouse_monitors.create(
                full_name=tabela,
                assets_dir=f"/Shared/monitoring/{tabela.replace('.', '/')}",
                output_schema_name="case_santander.gold",
                snapshot={}
            )
            print(f"  ✅ Monitor criado: {tabela} → {monitor.status}")
        except Exception as e:
            if "already exists" in str(e).lower():
                monitor = w.lakehouse_monitors.get(full_name=tabela)
                print(f"  ℹ️ Monitor existente: {tabela} → {monitor.status}")
            else:
                print(f"  ❌ Erro em {tabela}: {e}")

    fim = datetime.now()
    print(f"\n=== JOB LAKEHOUSE MONITORING CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
