"""
Script de Configuração do Airflow - Setup de Variáveis de Ambiente

Configura o Airflow com as variáveis corretas baseadas no ambiente (HK ou PROD).
"""
import os
import sys
from pathlib import Path


def load_env_file(env_file: str = ".env") -> dict:
    """Carrega variáveis do arquivo .env"""
    env_vars = {}
    
    if not os.path.exists(env_file):
        print(f" Arquivo {env_file} não encontrado")
        return env_vars
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    
    return env_vars


def get_env_config(env: str, env_vars: dict) -> dict:
    """Retorna configuração específica do ambiente"""
    config = {}
    
    # Variáveis comuns
    config['ENVIRONMENT'] = env
    config['CONFIRM_PRODUCTION'] = env_vars.get('CONFIRM_PRODUCTION', 'false')
    
    # Databricks - seleciona baseado no ambiente
    if env == 'hk':
        config['DATABRICKS_HOST'] = env_vars.get('DATABRICKS_HOST_HK', '')
        config['DATABRICKS_TOKEN'] = env_vars.get('DATABRICKS_TOKEN_HK', '')
        config['DATABRICKS_CLUSTER_ID'] = env_vars.get('DATABRICKS_CLUSTER_ID_HK', '')
        config['STORAGE_ACCOUNT'] = env_vars.get('STORAGE_ACCOUNT_HK', '')
        config['KEY_VAULT'] = env_vars.get('KEY_VAULT_HK', '')
    elif env == 'prod':
        config['DATABRICKS_HOST'] = env_vars.get('DATABRICKS_HOST_PROD', '')
        config['DATABRICKS_TOKEN'] = env_vars.get('DATABRICKS_TOKEN_PROD', '')
        config['DATABRICKS_CLUSTER_ID'] = env_vars.get('DATABRICKS_CLUSTER_ID_PROD', '')
        config['STORAGE_ACCOUNT'] = env_vars.get('STORAGE_ACCOUNT_PROD', '')
        config['KEY_VAULT'] = env_vars.get('KEY_VAULT_PROD', '')
    
    # Variáveis comuns
    config['DATABRICKS_REPO_PATH'] = env_vars.get('DATABRICKS_REPO_PATH', 
        '/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master')
    config['KAGGLE_API_TOKEN'] = env_vars.get('KAGGLE_API_TOKEN', '')
    
    return config


def generate_docker_env_file(env: str, output_file: str = "docker/.env"):
    """Gera arquivo .env para o Docker Compose"""
    env_vars = load_env_file()
    config = get_env_config(env, env_vars)
    
    with open(output_file, 'w') as f:
        for key, value in config.items():
            f.write(f"{key}={value}\n")
    
    print(f" Arquivo {output_file} gerado para ambiente {env.upper()}")
    return config


def generate_airflow_env_script(env: str, output_file: str = "docker/airflow_env.sh"):
    """Gera script de setup para o Airflow"""
    env_vars = load_env_file()
    config = get_env_config(env, env_vars)
    
    script_content = f"""#!/bin/bash
# Airflow Environment Setup - {env.upper()}
# Gerado automaticamente via scripts/setup_airflow_env.py

export ENVIRONMENT="{config['ENVIRONMENT']}"
export CONFIRM_PRODUCTION="{config['CONFIRM_PRODUCTION']}"
export DATABRICKS_HOST="{config['DATABRICKS_HOST']}"
export DATABRICKS_TOKEN="{config['DATABRICKS_TOKEN']}"
export DATABRICKS_CLUSTER_ID="{config['DATABRICKS_CLUSTER_ID']}"
export DATABRICKS_REPO_PATH="{config['DATABRICKS_REPO_PATH']}"
export STORAGE_ACCOUNT="{config['STORAGE_ACCOUNT']}"
export KEY_VAULT="{config['KEY_VAULT']}"
export KAGGLE_API_TOKEN="{config['KAGGLE_API_TOKEN']}"

echo " Airflow configurado para ambiente {env.upper()}"
echo "   Databricks Host: $DATABRICKS_HOST"
echo "   Cluster ID: $DATABRICKS_CLUSTER_ID"
echo "   Storage Account: $STORAGE_ACCOUNT"
"""
    
    with open(output_file, 'w') as f:
        f.write(script_content)
    
    # Tornar executável
    os.chmod(output_file, 0o755)
    
    print(f" Script {output_file} gerado para ambiente {env.upper()}")
    return config


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Configura Airflow para ambiente específico")
    parser.add_argument(
        "--env",
        choices=["hk", "prod"],
        default="hk",
        help="Ambiente de destino (hk ou prod)"
    )
    parser.add_argument(
        "--output-dir",
        default="docker",
        help="Diretório de saída para arquivos de configuração"
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Arquivo .env de origem"
    )
    
    args = parser.parse_args()
    
    # Verificar se arquivo .env existe
    if not os.path.exists(args.env_file):
        print(f" Arquivo {args.env_file} não encontrado")
        print(f" Copie .env.example para {args.env_file} e configure os valores")
        sys.exit(1)
    
    # Criar diretório de saída se não existir
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Gerar arquivos de configuração
    output_dir = Path(args.output_dir)
    
    print(f" Configurando Airflow para ambiente {args.env.upper()}...")
    
    # Gerar .env para Docker Compose
    docker_env = output_dir / ".env"
    config = generate_docker_env_file(args.env, str(docker_env))
    
    # Gerar script de setup
    airflow_script = output_dir / "airflow_env.sh"
    generate_airflow_env_script(args.env, str(airflow_script))
    
    # Exibir configuração
    print("\n Configuração aplicada:")
    print(f"   Ambiente: {args.env.upper()}")
    print(f"   Databricks Host: {config['DATABRICKS_HOST']}")
    print(f"   Cluster ID: {config['DATABRICKS_CLUSTER_ID']}")
    print(f"   Storage Account: {config['STORAGE_ACCOUNT']}")
    print(f"   Key Vault: {config['KEY_VAULT']}")
    
    print(f"\n Configuração concluída!")
    print(f" Para usar:")
    print(f"   docker compose -f docker/docker-compose.yml --env-file docker/.env up -d")


if __name__ == "__main__":
    main()
