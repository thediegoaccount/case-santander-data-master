"""
Exporta uma amostra de hash_cliente de bronze.clientes para um arquivo JSON
em ADLS.

O producer de streaming (scripts/eventhub_producer*.py) roda FORA do
Databricks -- sem Spark, sem dbutils, sem acesso a Unity Catalog. Este e o
unico ponto da cadeia onde a identidade de um cliente REAL sai do mundo
Databricks e vira algo que um processo externo consegue ler, para atribuir
hash_cliente a transacoes de streaming simuladas.
"""

import json
from datetime import datetime

from src.config.logging import info
from src.config.tables import SCHEMA_BRONZE

CTX = "exportar_amostra_streaming"
TAMANHO_AMOSTRA = 200
SEED = 42


def exportar_amostra_clientes(spark, dbutils, storage_account: str) -> int:
    """
    Amostra ate TAMANHO_AMOSTRA hash_cliente de bronze.clientes e grava em
    abfss://gold@<storage>/amostra_clientes_streaming/amostra.json.

    Retorna a quantidade de clientes exportados.
    """
    # Mesmo padrao de src/ingestion/ordens_simuladas.py: orderBy antes de
    # amostrar, porque Delta nao garante ordem de leitura -- sem isso a
    # mesma seed sortearia clientes diferentes a cada execucao.
    df_clientes = (
        spark.table(f"{SCHEMA_BRONZE}.clientes")
        .select("hash_cliente")
        .orderBy("hash_cliente")
        .toPandas()
    )

    total = len(df_clientes)
    if total == 0:
        info(CTX, "bronze.clientes vazia -- nenhuma amostra exportada")
        return 0

    n = min(TAMANHO_AMOSTRA, total)
    amostra = df_clientes.sample(n, random_state=SEED)["hash_cliente"].tolist()

    conteudo = json.dumps({
        "gerado_em": datetime.now().isoformat(),
        "hash_cliente": amostra,
    })

    path = f"abfss://gold@{storage_account}.dfs.core.windows.net/amostra_clientes_streaming/amostra.json"
    dbutils.fs.put(path, conteudo, overwrite=True)

    info(CTX, f"Amostra exportada: {len(amostra)} clientes -> {path}")
    return len(amostra)
