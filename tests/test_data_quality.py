"""
Testes de Qualidade de Dados

Valida qualidade de dados em cada camada (Bronze, Silver, Gold).

Contrato destes testes: a leitura da tabela pode pular o teste (a tabela
pode não existir no ambiente), mas TODA verificação de qualidade roda fora
do try/except e reprova de verdade quando o dado está ruim.
"""

from pyspark.sql import functions as F

from tests.conftest import read_table_or_skip
from src.config.tables import SCHEMA_BRONZE, SCHEMA_GOLD, SCHEMA_SILVER

# A tabela de fraude é gravada por src/gold/fraude.py como deteccao_fraude.
# O teste antigo lia "gold.fraude", que nunca existiu — e o skip escondia isso.
TB_BRONZE_CLIENTES = f"{SCHEMA_BRONZE}.clientes"
TB_SILVER_CLIENTES = f"{SCHEMA_SILVER}.clientes"
TB_GOLD_FRAUDE = f"{SCHEMA_GOLD}.deteccao_fraude"


def test_bronze_clientes_completeness(spark_session):
    """Testa se bronze.clientes tem todas as colunas obrigatórias"""
    # Alinhado ao que src/ingestion/clientes_kaggle.py realmente produz.
    # Antes exigia "nome" e "score_risco": o primeiro NAO e gravado de
    # proposito (e PII em claro; o desenho so persiste sobrenome_masked
    # hasheado), e o segundo nao existe -- a coluna e "score_credito".
    # Como o teste so pulava quando a tabela NAO existe, ele falharia em
    # qualquer ambiente onde ela exista.
    expected_columns = [
        "id_cliente",
        "hash_cliente",
        "sobrenome_masked",
        "score_credito",
        "saldo",
        "ativo",
    ]

    df = read_table_or_skip(spark_session, TB_BRONZE_CLIENTES)

    faltando = [c for c in expected_columns if c not in df.columns]
    assert not faltando, f"Colunas faltando em {TB_BRONZE_CLIENTES}: {faltando}"


def test_bronze_clientes_uniqueness(spark_session):
    """Testa se hash_cliente é único em bronze.clientes"""
    df = read_table_or_skip(spark_session, TB_BRONZE_CLIENTES)

    duplicados = df.groupBy("hash_cliente").count().filter(F.col("count") > 1)
    total_dup = duplicados.count()
    assert total_dup == 0, f"hash_cliente tem {total_dup} valores duplicados"


def test_silver_clientes_no_nulls(spark_session):
    """Testa se silver.clientes não tem nulls acima de 5% em colunas críticas"""
    df = read_table_or_skip(spark_session, TB_SILVER_CLIENTES)

    total = df.count()
    assert total > 0, f"{TB_SILVER_CLIENTES} está vazia"

    critical_columns = ["id_cliente", "hash_cliente", "nome"]
    violacoes = []
    for col in critical_columns:
        nulos = df.filter(F.col(col).isNull()).count()
        pct = nulos / total
        if pct >= 0.05:
            violacoes.append(f"{col}: {pct:.2%}")

    assert not violacoes, f"Nulos acima de 5% (max permitido): {violacoes}"


def test_gold_fraude_not_empty(spark_session):
    """Testa se a tabela de detecção de fraude tem dados e as colunas de score"""
    df = read_table_or_skip(spark_session, TB_GOLD_FRAUDE)

    assert df.count() > 0, f"{TB_GOLD_FRAUDE} está vazia"

    # Regressão: src/gold/fraude.py já gravou esta tabela sem nenhuma coluna
    # de fraude, porque a cadeia .withColumn estava presa ao ramo else do
    # join. Este assert pega essa classe de erro.
    esperadas = ["score_fraude", "total_alertas", "requer_revisao", "data_processamento"]
    faltando = [c for c in esperadas if c not in df.columns]
    assert not faltando, f"Colunas de fraude ausentes: {faltando}"


def test_schema_drift_detection(spark_session):
    """Testa se o schema de bronze.clientes não mudou inesperadamente"""
    expected_schema = {
        "id_cliente": "string",
        "hash_cliente": "string",
        "sobrenome_masked": "string",
        "score_credito": "long",
        "saldo": "double",
        "ativo": "boolean",
    }

    df = read_table_or_skip(spark_session, TB_BRONZE_CLIENTES)
    actual_schema = {f.name: f.dataType.typeName() for f in df.schema.fields}

    problemas = []
    for col, dtype in expected_schema.items():
        if col not in actual_schema:
            problemas.append(f"{col}: ausente")
        elif actual_schema[col] != dtype:
            problemas.append(f"{col}: esperado {dtype}, atual {actual_schema[col]}")

    assert not problemas, f"Schema drift em {TB_BRONZE_CLIENTES}: {problemas}"
