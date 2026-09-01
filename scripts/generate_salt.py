"""
Script para gerar salt criptograficamente seguro
Usa secrets.token_hex para gerar 64 caracteres hexadecimais
"""

import secrets
import sys


def generate_salt(length=32):
    """
    Gera um salt aleatório criptograficamente seguro
    
    Args:
        length: Comprimento em bytes (padrão: 32 = 64 caracteres hex)
    
    Returns:
        Salt em formato hexadecimal (64 caracteres)
    """
    return secrets.token_hex(length)


def main():
    print("=== Gerador de Salt para Anonimização de Dados ===")
    print()
    
    # Gerar salt
    salt = generate_salt()
    
    print(f"Salt gerado (64 caracteres hex):")
    print(f"{salt}")
    print()
    print("⚠️  Armazene este salt no Key Vault:")
    print(f"   az keyvault secret set --vault-name kv-case-santander --name salt --value {salt}")
    print()
    print("⚠️  NÃO compartilhe este salt. Se perdido, hashes não podem ser reutilizados.")
    print()
    
    # Output para fácil cópia
    print(f"SALT={salt}")


if __name__ == "__main__":
    main()
