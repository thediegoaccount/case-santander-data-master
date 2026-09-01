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
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret

from src.config.settings import configure_adls
from src.gold.streaming_gold import (
    calcular_ranking_realtime,
    calcular_volume_intraday,
    detectar_anomalias_intraday,
    detectar_fraude_streaming,
)


def main():
    inicio = datetime.now()
    info("job_streaming_to_gold", f"=== JOB STREAMING TO GOLD INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

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
        info("job_streaming_to_gold", f"CDC ativo: {total_streaming} novas transacoes desde versao {ultima_versao}")

    except Exception:
        # Fallback: leitura completa se CDF ainda nao estiver habilitado
        df_cdf = None
        _row = spark.sql("SELECT COUNT(*) as total FROM case_santander.silver.streaming").collect()[0]
        total_streaming = _row["total"]
        info("job_streaming_to_gold", f"CDC indisponivel — leitura completa: {total_streaming} transacoes")

    if total_streaming == 0:
        info("job_streaming_to_gold", "Nenhuma transacao nova encontrada. Job encerrado.")
        return

    # GOLD 1 — Fraude em transacoes streaming
    total_critico = detectar_fraude_streaming(spark)
    info("job_streaming_to_gold", f" gold.fraude_streaming → {total_critico} transacoes criticas")

    # GOLD 2 — Anomalias intradiarias de preco
    total_anomalias = detectar_anomalias_intraday(spark)
    info("job_streaming_to_gold", f" gold.anomalias_intraday → {total_anomalias} anomalias detectadas")

    # GOLD 3 — Volume intraday por ticker e hora
    total_vol = calcular_volume_intraday(spark)
    info("job_streaming_to_gold", " gold.volume_intraday gravado")

    # GOLD 4 — Ranking de ativos em tempo real
    total_rank = calcular_ranking_realtime(spark)
    info("job_streaming_to_gold", f" gold.ranking_acoes_realtime → {total_rank} ativos")

    fim = datetime.now()
    info("job_streaming_to_gold", "\n=== JOB STREAMING TO GOLD CONCLUIDO ===")
    info("job_streaming_to_gold", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
