from datetime import datetime
import time

import pandas as pd
import requests


def extrair_bcb(spark, storage_account: str) -> int:
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    data_inicial = "01/04/2021"
    data_final = "01/04/2026"

    def buscar_serie(codigo, nome, data_inicial, data_final, max_retries=3):
        """
        Buscar série do BCB com filtro de data obrigatório (regra desde 26/03/2025).
        Inclui validação robusta e retry com backoff.
        """
        # IMPORTANTE: BCB exige filtros de data desde 26/03/2025
        # Período máximo: 10 anos
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
            f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        )

        for tentativa in range(1, max_retries + 1):
            try:
                response = requests.get(
                    url,
                    timeout=30,
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                )

                # 1. Validar status HTTP
                if response.status_code != 200:
                    print(f"  ⚠️  {nome} — HTTP {response.status_code} (tentativa {tentativa}/{max_retries})")
                    if tentativa < max_retries:
                        time.sleep(2 ** tentativa)
                        continue
                    print(f"  ERRO: {nome} — HTTP {response.status_code} após {max_retries} tentativas")
                    return pd.DataFrame()

                # 2. Validar response não vazio
                if not response.text or response.text.strip() == "":
                    print(f"  ⚠️  {nome} — Response vazio (tentativa {tentativa}/{max_retries})")
                    if tentativa < max_retries:
                        time.sleep(2 ** tentativa)
                        continue
                    print(f"  ERRO: {nome} — Response vazio após {max_retries} tentativas")
                    return pd.DataFrame()

                # 3. Validar content-type é JSON (não HTML de erro)
                content_type = response.headers.get("Content-Type", "")
                if "json" not in content_type.lower() and "javascript" not in content_type.lower():
                    print(f"  ⚠️  {nome} — Content-Type inválido: {content_type} (tentativa {tentativa}/{max_retries})")
                    print(f"     Primeiros 100 chars: {response.text[:100]}")
                    if tentativa < max_retries:
                        time.sleep(2 ** tentativa)
                        continue
                    print(f"  ERRO: {nome} — API não retornou JSON")
                    return pd.DataFrame()

                # 4. Parse JSON com tratamento de erro
                try:
                    json_data = response.json()
                except ValueError as json_err:
                    print(f"  ⚠️  {nome} — JSON inválido: {str(json_err)[:80]} (tentativa {tentativa}/{max_retries})")
                    print(f"     Primeiros 100 chars: {response.text[:100]}")
                    if tentativa < max_retries:
                        time.sleep(2 ** tentativa)
                        continue
                    print(f"  ERRO: {nome} — JSON inválido após {max_retries} tentativas")
                    return pd.DataFrame()

                # 5. Validar estrutura
                if not isinstance(json_data, list) or len(json_data) == 0:
                    print(f"  ⚠️  {nome} — Dados vazios")
                    return pd.DataFrame()

                # 6. Construir DataFrame
                df = pd.DataFrame(json_data)

                # 7. Validar colunas
                if "data" not in df.columns or "valor" not in df.columns:
                    print(f"  ⚠️  {nome} — Colunas inesperadas: {df.columns.tolist()}")
                    return pd.DataFrame()

                # 8. Enriquecer
                df["indicador"] = nome
                df["data_extracao"] = data_hoje
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
                df = df.dropna(subset=["valor"])

                print(f"  ✅ OK: {nome} — {len(df)} registros")
                return df

            except requests.exceptions.Timeout:
                print(f"  ⚠️  {nome} — Timeout (tentativa {tentativa}/{max_retries})")
                if tentativa < max_retries:
                    time.sleep(2 ** tentativa)
                    continue
                print(f"  ERRO: {nome} — Timeout após {max_retries} tentativas")
                return pd.DataFrame()

            except requests.exceptions.ConnectionError as e:
                print(f"  ⚠️  {nome} — Erro de conexão (tentativa {tentativa}/{max_retries})")
                if tentativa < max_retries:
                    time.sleep(2 ** tentativa)
                    continue
                print(f"  ERRO: {nome} — {str(e)[:80]}")
                return pd.DataFrame()

            except Exception as e:
                print(f"  ERRO: {nome} — Inesperado: {str(e)[:100]}")
                return pd.DataFrame()

        return pd.DataFrame()

    print("Extraindo BCB...")

    # IMPORTANTE: Todas as séries agora usam filtros de data (regra BCB 26/03/2025)
    # IPCA é mensal mas API exige filtro mesmo assim
    df_selic = buscar_serie(11, "selic", data_inicial, data_final)
    time.sleep(1)  # Pequena pausa entre chamadas para evitar rate limit
    
    df_cambio = buscar_serie(1, "cambio_usd_brl", data_inicial, data_final)
    time.sleep(1)
    
    df_ipca = buscar_serie(433, "ipca", data_inicial, data_final)

    # Filtrar DataFrames vazios antes de concatenar
    dfs_validos = [df for df in [df_selic, df_cambio, df_ipca] if len(df) > 0]

    if not dfs_validos:
        print("❌ Nenhuma série BCB foi extraída com sucesso")
        return 0

    df_bcb = pd.concat(dfs_validos, ignore_index=True)

    if len(df_bcb) == 0:
        print("❌ Nenhum dado para gravar no Bronze")
        return 0

    # Salvar no Bronze
    try:
        bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/extracao={data_hoje}/"
        df_spark = spark.createDataFrame(df_bcb)
        df_spark.write.mode("overwrite").parquet(bronze_path)

        total = df_spark.count()
        print(f"✅ Bronze BCB gravado: {total} registros")
        return total

    except Exception as e:
        print(f"❌ Erro ao gravar Bronze: {str(e)}")
        return 0
