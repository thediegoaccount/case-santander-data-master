"""
Job: Streaming to Gold
Transforma dados da camada silver.streaming em tabelas Gold analíticas.

Depende de:
  - case_santander.silver.streaming  (gerado por t5_streaming)
  - case_santander.gold.performance_acoes (gerado por t3_gold)

Tabelas geradas:
  - gold.fraude_streaming       : deteccao de transacoes suspeitas em tempo real
  - gold.anomalias_intraday     : desvios de preco vs historico por ticker/hora
  - gold.volume_intraday        : volume negociado por ticker e hora do dia
  - gold.ranking_acoes_realtime : ranking de ativos por volume no dia atual

Ou via Databricks Workflow:
    Task: t10_streaming_gold
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime
from src.config.settings import configure_adls
from src.gold.streaming_gold import (
    detectar_fraude_streaming,
    detectar_anomalias_intraday,
    calcular_volume_intraday,
    calcular_ranking_realtime,
)


def main():
    inicio = datetime.now()
    print(f"=== JOB STREAMING TO GOLD INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    # CDC via Delta Change Data Feed: lê apenas as mudanças desde a
    # última versão processada, evitando full scan da tabela a cada execução.
    # _change_type filtra somente inserções novas (insert), ignorando
    # updates/deletes que não se aplicam ao fluxo de streaming.
    try:
        # fmt: off
        ultima_versao = spark.sql("""
            SELECT COALESCE(MAX(versao_cdf), 0)
            FROM case_santander.gold.observabilidade
            WHERE tabela = 'streaming'
        """).collect()[0][0]
        # fmt: on

        # fmt: off
        df_cdf = spark.read \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", ultima_versao) \
            .table("case_santander.silver.streaming") \
            .filter("_change_type = 'insert'") \
            .drop("_change_type", "_commit_version", "_commit_timestamp")
        # fmt: on

        total_streaming = df_cdf.count()
        print(f"CDC ativo: {total_streaming} novas transacoes desde versao {ultima_versao}")

    except Exception:
        # Fallback: leitura completa se CDF ainda nao estiver habilitado
        df_cdf = None
        _row = spark.sql("SELECT COUNT(*) as total FROM case_santander.silver.streaming").collect()[0]
        total_streaming = _row["total"]
        print(f"CDC indisponivel — leitura completa: {total_streaming} transacoes")

    if total_streaming == 0:
        print("Nenhuma transacao nova encontrada. Job encerrado.")
        return

    # GOLD 1 — Fraude em transacoes streaming
    total_critico = detectar_fraude_streaming(spark)
    print(f"✅ gold.fraude_streaming → {total_critico} transacoes criticas")

    # GOLD 2 — Anomalias intradiarias de preco
    total_anomalias = detectar_anomalias_intraday(spark)
    print(f"✅ gold.anomalias_intraday → {total_anomalias} anomalias detectadas")

    # GOLD 3 — Volume intraday por ticker e hora
    total_vol = calcular_volume_intraday(spark)
    print(f"✅ gold.volume_intraday → {total_vol} registros")

    # GOLD 4 — Ranking de ativos em tempo real
    total_rank = calcular_ranking_realtime(spark)
    print(f"✅ gold.ranking_acoes_realtime → {total_rank} ativos")

    fim = datetime.now()
    print("\n=== JOB STREAMING TO GOLD CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
