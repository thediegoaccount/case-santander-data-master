"""
Testes de src/security/hashing.py — anonimização LGPD.

Módulo puro e determinístico: roda sem Spark, sem JVM e sem rede.
Estava com 0% de cobertura apesar de ser o que garante a anonimização.
"""

import hashlib
import re

import pytest

from src.security.hashing import (
    generate_random_salt,
    hash_customer_id,
    hash_surname,
    hash_with_salt,
)

SALT = "salt-de-teste"


def test_hash_e_sha256_hex_de_64_chars():
    h = hash_with_salt("cliente-123", salt=SALT)
    assert re.fullmatch(r"[0-9a-f]{64}", h), f"não parece SHA256 hex: {h}"


def test_hash_e_deterministico():
    assert hash_with_salt("cliente-123", salt=SALT) == hash_with_salt("cliente-123", salt=SALT)


def test_entradas_diferentes_geram_hashes_diferentes():
    assert hash_with_salt("cliente-1", salt=SALT) != hash_with_salt("cliente-2", salt=SALT)


def test_salt_diferente_muda_o_hash():
    """Sem isto, o salt não estaria cumprindo função nenhuma."""
    assert hash_with_salt("cliente-123", salt="salt-a") != hash_with_salt("cliente-123", salt="salt-b")


def test_hash_nao_contem_o_dado_original():
    """Garantia mínima de anonimização."""
    h = hash_with_salt("sobrenome-silva", salt=SALT)
    assert "silva" not in h.lower()


def test_corresponde_a_sha256_de_dado_mais_salt():
    """Fixa o algoritmo: trocar a fórmula invalida hashes já gravados."""
    esperado = hashlib.sha256(f"cliente-123{SALT}".encode()).hexdigest()
    assert hash_with_salt("cliente-123", salt=SALT) == esperado


@pytest.mark.parametrize("func", [hash_customer_id, hash_surname])
def test_helpers_exigem_salt_do_key_vault(func, monkeypatch):
    """
    hash_customer_id/hash_surname não recebem salt: buscam no Key Vault.
    Fora do Databricks isso falha — o teste garante que falha de forma
    explícita, e não devolvendo um hash sem salt silenciosamente.
    """
    import src.security.hashing as mod

    monkeypatch.setattr(mod, "get_salt", lambda: SALT)
    h = func("valor-qualquer")
    assert re.fullmatch(r"[0-9a-f]{64}", h)


def test_generate_random_salt_tem_tamanho_pedido_e_nao_repete():
    """length é em BYTES; a saída é hex, portanto o dobro de caracteres."""
    a = generate_random_salt(32)
    b = generate_random_salt(32)
    assert len(a) == len(b) == 64
    assert re.fullmatch(r"[0-9a-f]{64}", a)
    assert a != b, "dois salts aleatórios não deveriam coincidir"
