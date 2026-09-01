"""
Script para substituir sys.path.insert hard-coded por setup_python_path()

Substitui:
    sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
Por:
    from src.config.environment import setup_python_path
    setup_python_path()
"""

import os
import re
from pathlib import Path


def fix_syspath_in_file(file_path: str):
    """
    Substitui sys.path.insert hard-coded em um arquivo
    
    Args:
        file_path: Caminho do arquivo Python
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Verificar se já tem import de setup_python_path
    if "from src.config.environment import setup_python_path" in content:
        print(f"  JÁ CORRIGIDO: {file_path}")
        return
    
    # Substituir sys.path.insert por setup_python_path
    # Padrão: sys.path.insert(0, "/Workspace/Users/...")
    pattern = r'sys\.path\.insert\(0, "/Workspace/Users/[^"]+"\)'
    
    # Verificar se tem o padrão
    if not re.search(pattern, content):
        print(f"  SEM sys.path.insert: {file_path}")
        return
    
    # Adicionar import após import sys
    import_sys_pattern = r'(import sys\n)'
    if re.search(import_sys_pattern, content):
        content = re.sub(
            import_sys_pattern,
            r'\1from src.config.environment import setup_python_path\n',
            content
        )
    
    # Substituir sys.path.insert por setup_python_path()
    new_content = re.sub(pattern, 'setup_python_path()', content)
    
    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  CORRIGIDO: {file_path}")
    else:
        print(f"  SEM MUDANÇAS: {file_path}")


def main():
    print("=== Corrigindo sys.path.insert hard-coded ===")
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
        fix_syspath_in_file(str(job_file))
    
    print()
    print("=== Correção Concluída ===")
    print()
    print("Próximo passo:")
    print("1. Verificar arquivos modificados")
    print("2. Commitar mudanças")
    print("3. Deploy para Databricks")


if __name__ == "__main__":
    main()
