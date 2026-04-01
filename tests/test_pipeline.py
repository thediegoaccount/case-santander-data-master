import pytest
import sys
sys.path.append(".")
from config.config import ACOES, BRONZE, SILVER, GOLD

def test_acoes_lista():
    assert len(ACOES) == 8
    assert "PETR4.SA" in ACOES
    assert "VALE3.SA" in ACOES

def test_camadas_adls():
    assert "bronze" in BRONZE
    assert "silver" in SILVER
    assert "gold" in GOLD

def test_storage_account():
    assert "stcasesantander" in BRONZE

def test_acoes_formato():
    for acao in ACOES:
        assert acao.endswith(".SA"), f"{acao} nao tem sufixo .SA"
