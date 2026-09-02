"""
Testes de src/quality/data_quality.py.

O framework tinha 228 linhas, zero call sites e zero cobertura — só aparecia
em documentação. Agora ele é o gate das transformações silver, então precisa
de teste que garanta que ele REPROVA quando deve.

Usa dublês em vez de Spark: as validações só dependem de `columns`, `count()`
e `filter()`, então não é preciso JVM.
"""

import pytest

from src.quality.data_quality import DataQualityError, DataQualityValidator


class FakeDF:
    """Dublê mínimo de DataFrame Spark para as validações."""

    def __init__(self, columns, n=10, nulos=0, duplicatas=0):
        self.columns = list(columns)
        self._n = n
        self._nulos = nulos
        self._dups = duplicatas

    def count(self):
        return self._n

    def filter(self, *_a, **_k):
        return FakeDF(self.columns, n=self._nulos)

    def select(self, *_a, **_k):
        return self

    def groupBy(self, *_a, **_k):
        return self

    def agg(self, *_a, **_k):
        return self

    def dropDuplicates(self, *_a, **_k):
        return FakeDF(self.columns, n=self._n - self._dups)


@pytest.fixture
def v():
    return DataQualityValidator("teste")


def test_completeness_aprova_com_todas_as_colunas(v):
    df = FakeDF(["id", "nome", "valor"])
    v.validate_completeness(df, ["id", "nome"])


def test_completeness_reprova_com_coluna_faltando(v):
    df = FakeDF(["id"])
    with pytest.raises(DataQualityError) as exc:
        v.validate_completeness(df, ["id", "nome", "valor"])
    assert "nome" in str(exc.value)


def test_row_count_reprova_com_tabela_vazia(v):
    with pytest.raises(DataQualityError):
        v.validate_row_count(FakeDF(["id"], n=0), min_rows=1)


def test_row_count_aprova_acima_do_minimo(v):
    v.validate_row_count(FakeDF(["id"], n=10), min_rows=1)


def test_run_all_validations_propaga_a_falha(v):
    """
    O gate precisa ABORTAR o job. Se run_all_validations engolisse a
    exceção, dado ruim seguiria para a camada gold silenciosamente — que é
    exatamente o defeito que os testes antigos de qualidade tinham.
    """
    df = FakeDF(["id"], n=0)
    with pytest.raises(DataQualityError):
        v.run_all_validations(df, {
            "completeness": {"required_columns": ["id", "nome"]},
            "row_count": {"min_rows": 1},
        })


def test_run_all_validations_aprova_dado_bom(v):
    df = FakeDF(["id", "nome"], n=100)
    v.run_all_validations(df, {
        "completeness": {"required_columns": ["id", "nome"]},
        "row_count": {"min_rows": 1},
    })
