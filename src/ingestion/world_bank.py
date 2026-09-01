from datetime import datetime

import pandas as pd
import requests

from src.config.environment import get_config, get_env, is_production
from src.ingestion.api_wrapper import rate_limiter
from src.utils.retry import retry_on_connection_error


def extrair_world_bank(spark, storage_account: str = None) -> int:
    """
    Extrai indicadores macroeconômicos do World Bank com isolamento por ambiente.

    Args:
        spark: Sessão Spark
        storage_account: Nome do storage account (opcional, usa config do ambiente)

    Returns: total de registros gravados
    """
    env = get_env()
    config = get_config()

    # Usar storage account do ambiente se não fornecido
    if storage_account is None:
        storage_account = config["storage_account"]

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    print(f"[{env.upper()}] Extraindo World Bank...")
    print(f"[{env.upper()}] Storage Account: {storage_account}")
    print(f"[{env.upper()}] Catalog: {config['catalog']}")

    if is_production():
        print(f"[{env.upper()}] *** PRODUÇÃO *** - Dados reais serão gravados")

    @retry_on_connection_error(max_attempts=3)
    def _requisitar(indicador):
        """
        Chamada de rede isolada: falhas de conexão/timeout disparam retry com backoff.
        Erros de dados (JSON malformado, schema inesperado) não são retentados.
        """
        # Rate limiting por ambiente
        rate_limiter.wait_if_needed("world_bank")

        url = f"https://api.worldbank.org/v2/country/BR/indicator/{indicador}?format=json&per_page=30"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    def buscar(indicador, nome):
        try:
            data = _requisitar(indicador)

            if not data or len(data) < 2 or not data[1]:
                print(f"[{env.upper()}]   VAZIO: {nome}")
                return pd.DataFrame()

            registros = data[1]
            df = pd.DataFrame([{"ano": r["date"], "valor": r["value"]} for r in registros if r["value"] is not None])

            if df.empty:
                print(f"[{env.upper()}]   VAZIO: {nome}")
                return pd.DataFrame()

            df["indicador"] = nome
            df["data_extracao"] = data_hoje
            df["fonte"] = "world_bank"
            df["ambiente"] = env  # Tag de ambiente para rastreabilidade
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

            print(f"[{env.upper()}]  OK: {nome} — {len(df)} registros")
            return df

        except Exception as e:
            # Última barreira: falha em um indicador não interrompe os demais nem o pipeline
            print(f"[{env.upper()}] ERRO: {nome} — {str(e)[:100]}")
            return pd.DataFrame()

    df_pib = buscar("NY.GDP.MKTP.KD.ZG", "pib_anual")
    df_desemprego = buscar("SL.UEM.TOTL.ZS", "desemprego")

    dfs = [df for df in [df_pib, df_desemprego] if not df.empty]

    if not dfs:
        print(f"[{env.upper()}]  World Bank sem dados disponíveis — pulando extração")
        return 0

    df_wb = pd.concat(dfs, ignore_index=True)

    # Caminho isolado por ambiente
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/extracao={data_hoje}/"
    df_spark = spark.createDataFrame(df_wb)
    df_spark.write.mode("overwrite").parquet(bronze_path)

    total = df_spark.count()
    print(f"[{env.upper()}]  Bronze World Bank gravado: {total} registros")
    return total
