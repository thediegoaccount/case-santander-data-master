"""
Script para substituir print() por logging em jobs Python

Substitui:
    print("mensagem")
Por:
    from src.config.logging import info
    info("job_name", "mensagem")
"""

import os
import re
from pathlib import Path


def get_job_name_from_file(file_path: str) -> str:
    """Extrai nome do job do nome do arquivo"""
    filename = Path(file_path).name
    # job_clientes_ordens.py -> job_clientes_ordens
    return filename.replace(".py", "")


def replace_prints_in_file(file_path: str):
    """
    Substitui print() por logging em um arquivo
    
    Args:
        file_path: Caminho do arquivo Python
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    job_name = get_job_name_from_file(file_path)
    
    # Verificar se já tem import de logging
    if "from src.config.logging import" in content:
        print(f"  JÁ TEM LOGGING: {file_path}")
        return
    
    # Adicionar import após sys.path.insert
    import_pattern = r'(sys\.path\.insert\(0, "[^"]+"\)\n)'
    if re.search(import_pattern, content):
        content = re.sub(
            import_pattern,
            r'\1from src.config.logging import info, error, warning\n',
            content
        )
    
    # Substituir print("mensagem") por info(job_name, "mensagem")
    # Padrão: print("texto") ou print(f"texto{var}")
    print_pattern = r'print\(["\']([^"\']+)["\']\)'
    
    def replace_print(match):
        message = match.group(1)
        return f'info("{job_name}", "{message}")'
    
    new_content = re.sub(print_pattern, replace_print, content)
    
    # Substituir print(f"...") por info(job_name, f"...")
    print_f_pattern = r'print\(f"([^"]+)"\)'
    
    def replace_print_f(match):
        message = match.group(1)
        return f'info("{job_name}", f"{message}")'
    
    new_content = re.sub(print_f_pattern, replace_print_f, new_content)
    
    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  CORRIGIDO: {file_path}")
    else:
        print(f"  SEM MUDANÇAS: {file_path}")


def main():
    print("=== Substituindo print() por logging ===")
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
        replace_prints_in_file(str(job_file))
    
    print()
    print("=== Substituição Concluída ===")
    print()
    print("Próximo passo:")
    print("1. Verificar arquivos modificados")
    print("2. Commitar mudanças")
    print("3. Deploy para Databricks")


if __name__ == "__main__":
    main()
