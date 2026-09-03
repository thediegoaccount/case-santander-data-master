"""
Geração de ordens simuladas de compra e venda, a partir dos clientes já
ingeridos na bronze.

Escreve UMA tabela: <catalog>.<env>_bronze.ordens
Lê: <catalog>.<env>_bronze.clientes
"""

import random
from datetime import datetime, timedelta

import pandas as pd

from src.config.logging import info
from src.config.tables import SCHEMA_BRONZE
from src.utils.delta import merge_ou_cria

CTX = "ordens_simuladas"

# Seed fixa: mesmo conjunto de clientes, mesmas ordens, entre execuções.
# df.sample() do pandas usa gerador próprio — random.seed() sozinho não basta.
ORDENS_SEED = 42
QTD_CLIENTES_AMOSTRA = 1000

ACOES = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "ABEV3.SA",
    "MGLU3.SA", "WEGE3.SA", "BBAS3.SA", "SANB11.SA",
]


def gerar_ordens(spark) -> int:
    """
    Lê bronze.clientes, simula ordens e grava bronze.ordens.

    Retorna a quantidade de ordens geradas.
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    random.seed(ORDENS_SEED)

    # ORDENAÇÃO OBRIGATÓRIA ANTES DE AMOSTRAR.
    # Antes, as ordens eram geradas a partir do DataFrame pandas ainda em
    # memória, na ordem do CSV. Agora a origem é a tabela Delta, e Delta NÃO
    # garante ordem de leitura: sem o orderBy, a mesma seed sortearia clientes
    # diferentes a cada execução e a simulação deixaria de ser reproduzível.
    df_clientes = (
        spark.table(f"{SCHEMA_BRONZE}.clientes")
        .select("hash_cliente", "perfil_risco", "faixa_saldo")
        .orderBy("hash_cliente")
        .toPandas()
    )

    if df_clientes.empty:
        info(CTX, "bronze.clientes está vazia — nenhuma ordem gerada")
        return 0

    n = min(QTD_CLIENTES_AMOSTRA, len(df_clientes))
    amostra = df_clientes.sample(n, random_state=ORDENS_SEED).to_dict("records")

    ordens = []
    contador = 0
    for cliente in amostra:
        for _ in range(random.randint(1, 10)):
            preco = round(random.uniform(10, 100), 2)
            qtd = random.randint(100, 10000)
            data_ord = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 457))
            contador += 1
            # ID determinístico: hash do cliente + data + sequencial.
            # É a chave do MERGE, então precisa ser estável entre execuções.
            id_ordem = f"ORD{cliente['hash_cliente']}-{data_ord.strftime('%Y%m%d')}-{contador:04d}"

            ordens.append({
                "id_ordem":      id_ordem,
                "hash_cliente":  cliente["hash_cliente"],
                "perfil_risco":  cliente["perfil_risco"],
                "faixa_saldo":   cliente["faixa_saldo"],
                "ticker":        random.choice(ACOES),
                "preco":         preco,
                "quantidade":    qtd,
                "valor_total":   round(preco * qtd, 2),
                "tipo":          random.choice(["compra", "venda"]),
                "corretora":     "Santander Corretora",
                "status":        random.choice(["executada", "cancelada", "pendente"]),
                "data_ordem":    data_ord.strftime("%Y-%m-%d"),
                "data_extracao": data_hoje,
            })

    merge_ou_cria(spark, spark.createDataFrame(pd.DataFrame(ordens)),
                  f"{SCHEMA_BRONZE}.ordens", "id_ordem", CTX)

    info(CTX, f"Bronze ordens gravado: {len(ordens)} registros "
              f"({n} clientes amostrados)")
    return len(ordens)
