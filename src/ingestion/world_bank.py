import requests
import pandas as pd
from datetime import datetime


def extrair_world_bank(spark, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    def buscar(indicador, nome):
        url = f"https://api.worldbank.org/v2/country/BR/indicator/{indicador}?format=json&per_page=30"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if not data or len(data) < 2 or not data[1]:
                print(f"  VAZIO: {nome}")
                return pd.DataFrame()
            registros = data[1]
            df = pd.DataFrame([{
                "ano":   r["date"],
                "valor": r["value"]
            } for r in registros if r["value"] is not None])
            if df.empty:
                print(f"  VAZIO: {nome}")
                return pd.DataFrame()
            df["indicador"] = nome
            df["data_extracao"] = data_hoje
            df["fonte"] = "world_bank"
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
            print(f"  OK: {nome} — {len(df)} registros")
            return df
        except Exception as e:
            print(f"  ERRO: {nome} — {e}")
            return pd.DataFrame()

    print("Extraindo World Bank...")
    df_pib = buscar("NY.GDP.MKTP.KD.ZG", "pib_anual")
    df_desemprego = buscar("SL.UEM.TOTL.ZS",    "desemprego")

    dfs = [df for df in [df_pib, df_desemprego] if not df.empty]

    if not dfs:
        print("⚠️ World Bank sem dados disponíveis — pulando extração")
        return 0

    df_wb = pd.concat(dfs, ignore_index=True)

    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/extracao={data_hoje}/"
    df_spark = spark.createDataFrame(df_wb)
    df_spark.write.mode("overwrite").parquet(bronze_path)

    total = df_spark.count()
    print(f"Bronze World Bank gravado: {total} registros")
    return total
