"""
Security: Hashing with Salt for Data Anonymization

Usa SHA256 com salt armazenado no Key Vault para mascarar dados sensíveis.
Impossível reverter (one-way hash) mesmo com acesso ao salt.
"""

import hashlib
from src.config.secrets import get_secret


def get_salt():
    """Recupera salt do Key Vault via get_secret"""
    try:
        return get_secret("salt")
    except Exception as e:
        raise ValueError(f"Erro ao recuperar salt do Key Vault: {str(e)}")


def hash_with_salt(data: str, salt: str = None) -> str:
    """
    Gera hash SHA256 com salt (one-way, não reversível)

    Args:
        data: Dado a ser mascarado
        salt: Salt para hashing (opcional, usa Key Vault se não fornecido)

    Returns:
        Hash SHA256 em formato hexadecimal (64 caracteres)

    Nota:
        Mesmo com acesso ao salt, não é possível reverter o hash original.
        O salt apenas previne ataques de rainbow table.
    """
    if salt is None:
        salt = get_salt()

    # Concatenar dado + salt
    salted_data = f"{data}{salt}"

    # Gerar hash SHA256
    hash_obj = hashlib.sha256(salted_data.encode("utf-8"))
    hash_hex = hash_obj.hexdigest()

    return hash_hex


def hash_customer_id(customer_id: str) -> str:
    """
    Mascara CustomerId usando SHA256 + salt
    Retorna hash de 64 caracteres (hexadecimal)
    """
    return hash_with_salt(customer_id)


def hash_surname(surname: str) -> str:
    """
    Mascara sobrenome usando SHA256 + salt
    Útil para anonimizar nomes enquanto mantém consistência
    """
    return hash_with_salt(surname)


def hash_email(email: str) -> str:
    """
    Mascara email usando SHA256 + salt
    """
    return hash_with_salt(email)


def anonymize_customer_row(row, salt=None):
    """
    Anonimiza uma linha de dados do cliente

    Args:
        row: Dicionário com dados do cliente
        salt: Salt opcional

    Returns:
        Dicionário com dados mascarados
    """
    anonymized = {}

    for key, value in row.items():
        if key in ["CustomerId", "Surname", "Email"]:
            anonymized[key] = hash_with_salt(str(value), salt)
        else:
            anonymized[key] = value

    return anonymized


def generate_random_salt(length=32):
    """
    Gera um salt aleatório criptograficamente seguro

    Args:
        length: Comprimento do salt em bytes

    Returns:
        Salt em formato hexadecimal
    """
    import secrets

    return secrets.token_hex(length)
