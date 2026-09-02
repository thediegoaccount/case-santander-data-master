"""
Configuração compartilhada da suíte de testes.

A fixture spark_session vivia dentro de tests/test_data_quality.py, o que a
tornava indisponível para os demais arquivos de teste. Aqui ela é visível
para toda a suíte.
"""

import pytest


@pytest.fixture(scope="session")
def spark_session():
    """
    SparkSession local para os testes.

    Requer JDK instalado e JAVA_HOME configurado. Sem isso o pyspark falha
    com JAVA_GATEWAY_EXITED — a fixture transforma isso num skip explicativo
    em vez de um erro cru repetido em cada teste.
    """
    pyspark = pytest.importorskip("pyspark", reason="pyspark não instalado")
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder
            .appName("tests_case_santander")
            .master("local[1]")
            .getOrCreate()
        )
    except Exception as exc:  # JAVA_GATEWAY_EXITED e afins
        pytest.skip(f"SparkSession indisponível (JDK/JAVA_HOME ausente?): {exc}")

    yield spark

    spark.stop()


def read_table_or_skip(spark, table: str):
    """
    Lê uma tabela ou pula o teste se ela não existir.

    IMPORTANTE: use esta função para o acesso à tabela e deixe os asserts
    FORA de qualquer try/except. O padrão anterior envolvia leitura E asserts
    no mesmo `try: ... except Exception: pytest.skip(...)`; como AssertionError
    é subclasse de Exception, toda falha real de qualidade virava SKIP e o CI
    reportava verde. Estes testes eram incapazes de reprovar.
    """
    try:
        return spark.table(table)
    except Exception:
        pytest.skip(f"Tabela {table} não existe neste ambiente")
