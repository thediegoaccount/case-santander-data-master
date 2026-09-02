"""
Testes de src/utils/retry.py — backoff exponencial.

Usado em produção por src/ingestion/world_bank.py e yahoo_finance.py.
Estava com 0% de cobertura. O delay é mockado para o teste ser instantâneo.
"""

import pytest

from src.utils.retry import RetryError, retry, retry_on_connection_error


@pytest.fixture(autouse=True)
def sem_sleep(monkeypatch):
    """Neutraliza o time.sleep para os testes não gastarem o backoff real."""
    import src.utils.retry as mod

    dormidas = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: dormidas.append(s))
    return dormidas


def test_sucesso_de_primeira_nao_faz_retry(sem_sleep):
    chamadas = []

    @retry(max_attempts=3, delay=1)
    def ok():
        chamadas.append(1)
        return "resultado"

    assert ok() == "resultado"
    assert len(chamadas) == 1
    assert sem_sleep == [], "não deveria ter dormido"


def test_tenta_de_novo_e_sucede(sem_sleep):
    chamadas = []

    @retry(max_attempts=3, delay=1)
    def falha_duas_vezes():
        chamadas.append(1)
        if len(chamadas) < 3:
            raise ConnectionError("boom")
        return "ok"

    assert falha_duas_vezes() == "ok"
    assert len(chamadas) == 3


def test_backoff_e_exponencial(sem_sleep):
    @retry(max_attempts=4, delay=1.0, backoff=2.0)
    def sempre_falha():
        raise ConnectionError("boom")

    with pytest.raises((RetryError, ConnectionError)):
        sempre_falha()

    # 3 esperas entre 4 tentativas, dobrando a cada vez
    assert sem_sleep == [1.0, 2.0, 4.0], f"backoff inesperado: {sem_sleep}"


def test_esgota_tentativas_e_propaga(sem_sleep):
    chamadas = []

    @retry(max_attempts=3, delay=0.1)
    def sempre_falha():
        chamadas.append(1)
        raise ConnectionError("boom")

    with pytest.raises((RetryError, ConnectionError)):
        sempre_falha()

    assert len(chamadas) == 3, "deveria ter tentado exatamente max_attempts vezes"


def test_excecao_fora_da_lista_nao_faz_retry(sem_sleep):
    """Só as exceções declaradas disparam retry; as demais sobem na hora."""
    chamadas = []

    @retry(max_attempts=3, delay=0.1, exceptions=(ConnectionError,))
    def erro_de_tipo():
        chamadas.append(1)
        raise ValueError("não é de conexão")

    with pytest.raises(ValueError):
        erro_de_tipo()

    assert len(chamadas) == 1, "ValueError não deveria disparar retry"


def test_retry_on_connection_error_preserva_metadados(sem_sleep):
    @retry_on_connection_error(max_attempts=2)
    def minha_funcao():
        """Docstring original."""
        return 42

    assert minha_funcao() == 42
    # @wraps deve preservar nome e docstring
    assert minha_funcao.__name__ == "minha_funcao"
    assert minha_funcao.__doc__ == "Docstring original."
