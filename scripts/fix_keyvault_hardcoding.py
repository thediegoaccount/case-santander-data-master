"""
Script para corrigir hardcoding de Key Vault nos jobs

Substitui dbutils.secrets.get(scope="kv-case-santander", key=...)
por get_secret(key) que usa key_vault dinâmico do ambiente.
"""

import os
import re
from pathlib import Path


def fix_job_file(file_path: str):
    """
    Corrige hardcoding de Key Vault em um arquivo de job
    
    Substitui:
        dbutils.secrets.get(scope="kv-case-santander", key="...")
    
    Por:
        from src.config.secrets import get_secret
        get_secret("...")
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Verificar se já tem import
    if "from src.config.secrets import get_secret" in content:
        print(f"  JÁ CORRIGIDO: {file_path}")
        return
    
    # Adicionar import
    imports_pattern = r'(from databricks\.sdk\.runtime import dbutils)'
    if re.search(imports_pattern, content):
        content = re.sub(
            imports_pattern,
            r'\1\nfrom src.config.secrets import get_secret',
            content
        )
    
    # Substituir dbutils.secrets.get(scope="kv-case-santander", key=...)
    # por get_secret("...")
    pattern = r'dbutils\.secrets\.get\(scope="kv-case-santander", key="([^"]+)"\)'
    replacement = r'get_secret("\1")'
    
    new_content = re.sub(pattern, replacement, content)
    
    # Substituir dbutils.secrets.get(scope="kv-case-santander-hk", key=...)
    pattern_hk = r'dbutils\.secrets\.get\(scope="kv-case-santander-hk", key="([^"]+)"\)'
    replacement_hk = r'get_secret("\1")'
    
    new_content = re.sub(pattern_hk, replacement_hk, new_content)
    
    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  CORRIGIDO: {file_path}")
    else:
        print(f"  SEM MUDANÇAS: {file_path}")


def main():
    print("=== Corrigindo Hardcoding de Key Vault ===")
    print()
    
    # Diretório de jobs
    jobs_dir = Path("jobs")
    
    if not jobs_dir.exists():
        print("ERRO: Diretório 'jobs' não encontrado")
        return
    
    # Arquivos de job Python
    job_files = list(jobs_dir.glob("job_*.py"))
    
    print(f"Encontrados {len(job_files)} arquivos de job")
    print()
    
    for job_file in job_files:
        print(f"Processando: {job_file.name}")
        fix_job_file(str(job_file))
    
    print()
    print("=== Correção Concluída ===")
    print()
    print("Próximo passo:")
    print("1. Verificar arquivos modificados")
    print("2. Commitar mudanças")
    print("3. Deploy para Databricks")


if __name__ == "__main__":
    main()
