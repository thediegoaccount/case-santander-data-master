import sys

sys.path.append(".")
from src.config.settings import ACOES, get_paths


def test_acoes_lista():
    assert len(ACOES) == 9
    assert "PETR4.SA" in ACOES
    assert "VALE3.SA" in ACOES
    assert "SANB11.SA" in ACOES


def test_acoes_formato():
    for acao in ACOES:
        assert acao.endswith(".SA"), f"{acao} nao tem sufixo .SA"


def test_paths_bronze():
    paths = get_paths("stcasesantander")
    assert "bronze" in paths["bronze_acoes"]
    assert "bronze" in paths["bronze_bcb"]
    assert "bronze" in paths["bronze_clientes"]


def test_paths_silver():
    paths = get_paths("stcasesantander")
    assert "silver" in paths["silver_acoes"]
    assert "silver" in paths["silver_bcb"]


def test_paths_gold():
    paths = get_paths("stcasesantander")
    assert "gold" in paths["gold_anomalias"]
    assert "gold" in paths["gold_performance"]


def test_storage_account():
    paths = get_paths("stcasesantander")
    assert "stcasesantander" in paths["bronze_acoes"]
