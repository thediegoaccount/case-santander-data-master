# Configuração do Airflow com Sistema de Ambientes

## Visão Geral

O Airflow agora está integrado com o sistema de configuração de ambientes (HK/PROD) definido em `src/config/environment.py`.

## Estrutura de Configuração

```

                  .env (Configuração Principal)                
                                                             
  ENVIRONMENT=hk                                           
  DATABRICKS_HOST_HK=...                                   
  DATABRICKS_TOKEN_HK=...                                  
  STORAGE_ACCOUNT_HK=...                                   

                     
                     

           scripts/setup_airflow_env.py                      
                                                             
  - Lê .env                                                 
  - Seleciona variáveis do ambiente (HK ou PROD)           
  - Gera docker/.env para Docker Compose                   

                     
                     

              docker/docker-compose.yml                      
                                                             
  - Lê docker/.env                                          
  - Passa variáveis para containers Airflow                
  - Disponibiliza para DAGs e Jobs                         

                     
                     

              Airflow DAG & Databricks Jobs                 
                                                             
  - DAG lê variáveis de ambiente                          
  - Jobs usam src/config/environment.py                    
  - Configuração consistente em toda pipeline              

```

## Passo a Passo de Configuração

### 1. Configurar Arquivo .env Principal

Copie o template e configure:

```bash
# Copiar template
cp .env.example .env

# Editar com suas credenciais
nano .env  # ou use seu editor preferido
```

**Variáveis obrigatórias:**

```bash
# Ambiente
ENVIRONMENT=hk  # ou prod

# Databricks
DATABRICKS_HOST_HK=https://adb-xxx-hk.azuredatabricks.net
DATABRICKS_TOKEN_HK=seu_token_hk
DATABRICKS_CLUSTER_ID_HK=seu_cluster_id_hk

# Para produção (se usar)
DATABRICKS_HOST_PROD=https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN_PROD=seu_token_prod
DATABRICKS_CLUSTER_ID_PROD=seu_cluster_id_prod

# Azure Resources
STORAGE_ACCOUNT_HK=stcasesantander-hk
KEY_VAULT_HK=kv-case-santander-hk
```

### 2. Gerar Configuração do Airflow

Execute o script de setup:

```bash
# Para homologação (padrão)
python scripts/setup_airflow_env.py --env hk

# Para produção
python scripts/setup_airflow_env.py --env prod
```

Isso gera:
- `docker/.env` - Arquivo de configuração para Docker Compose
- `docker/airflow_env.sh` - Script de setup (opcional)

### 3. Iniciar Airflow com Configuração

```bash
# Usar o arquivo .env gerado
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

### 4. Verificar Configuração

Acesse o Airflow UI e verifique:

```bash
# Acessar Airflow
http://localhost:8080

# Login: admin / admin

# Verificar variáveis de ambiente no DAG
# - Admin → Variables (ou ver nos logs do DAG)
```

## Como o DAG Lê a Configuração

### No Arquivo do DAG

O DAG gerado automaticamente lê as variáveis:

```python
# dags/dag_pipeline_santander.py (gerado via sync script)
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0401-150803-wefgy1hc")
REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hk")
```

### Nos Jobs Databricks

Os jobs usam o sistema de configuração:

```python
# jobs/job_extracao_acoes.py
from src.config.environment import get_config, get_env

env = get_env()  # "hk" ou "prod"
config = get_config()  # Configuração do ambiente atual
```

## Trocar de Ambiente

### De HK para PROD

1. **Editar .env principal**
```bash
# .env
ENVIRONMENT=prod
CONFIRM_PRODUCTION=true
```

2. **Regenerar configuração do Airflow**
```bash
python scripts/setup_airflow_env.py --env prod
```

3. **Reiniciar Airflow**
```bash
docker compose -f docker/docker-compose.yml --env-file docker/.env down
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

### De PROD para HK

1. **Editar .env principal**
```bash
# .env
ENVIRONMENT=hk
CONFIRM_PRODUCTION=false
```

2. **Regenerar configuração do Airflow**
```bash
python scripts/setup_airflow_env.py --env hk
```

3. **Reiniciar Airflow**
```bash
docker compose -f docker/docker-compose.yml --env-file docker/.env down
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

## Validação de Configuração

### Verificar Variáveis no Container

```bash
# Entrar no container Airflow
docker compose -f docker/docker-compose.yml --env-file docker/.env exec airflow-webserver bash

# Verificar variáveis de ambiente
env | grep DATABRICKS
env | grep ENVIRONMENT
env | grep STORAGE_ACCOUNT
```

### Verificar no DAG

O DAG exibe o ambiente nos logs:

```python
# No DAG gerado
print(f"Ambiente: {ENVIRONMENT}")
print(f"Databricks Host: {os.getenv('DATABRICKS_HOST')}")
print(f"Cluster ID: {CLUSTER_ID}")
```

## Troubleshooting

### Erro: "DATABRICKS_HOST não definido"

**Causa:** Variável de ambiente não configurada

**Solução:**
```bash
# Verificar se docker/.env foi gerado
cat docker/.env

# Regenerar configuração
python scripts/setup_airflow_env.py --env hk
```

### Erro: "Ambiente inválido"

**Causa:** ENVIRONMENT não é "hk" ou "prod"

**Solução:**
```bash
# Verificar .env
grep ENVIRONMENT .env

# Corrigir para hk ou prod
sed -i 's/ENVIRONMENT=.*/ENVIRONMENT=hk/' .env
```

### Erro: "Produção requer confirmação"

**Causa:** Tentando usar prod sem CONFIRM_PRODUCTION=true

**Solução:**
```bash
# Adicionar confirmação
echo "CONFIRM_PRODUCTION=true" >> .env

# Ou usar hk para testes
```

### Airflow não conecta no Databricks

**Causa:** Credenciais incorretas ou cluster ID errado

**Solução:**
```bash
# Verificar variáveis no container
docker compose exec airflow-webserver env | grep DATABRICKS

# Testar conexão manual
databricks configure --token
```

## Boas Práticas

###  DO

- **Sempre** use `setup_airflow_env.py` para gerar configuração
- **Sempre** verifique `docker/.env` antes de iniciar Airflow
- **Sempre** teste em HK antes de usar PROD
- **Mantenha** `.env` no `.gitignore` (não commitar credenciais)
- **Use** confirmação explícita para produção

###  DON'T

- **Nunca** edite `docker/.env` manualmente (use o script)
- **Nunca** commit `.env` com credenciais reais
- **Nunca** misture ambientes (HK apontando para prod)
- **Nunca** ignore avisos de ambiente inválido
- **Nunca** use credenciais de prod em ambiente de testes

## Integração CI/CD

O script de setup pode ser integrado no CI/CD:

```yaml
# .github/workflows/deploy-airflow.yml
- name: Setup Airflow Environment
  run: |
    python scripts/setup_airflow_env.py --env ${{ env.ENVIRONMENT }}
    
- name: Start Airflow
  run: |
    docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

## Monitoramento

### Logs de Configuração

O script de setup gera logs detalhados:

```bash
python scripts/setup_airflow_env.py --env hk
```

Saída:
```
 Configurando Airflow para ambiente HK...
 Arquivo docker/.env gerado para ambiente HK
 Script docker/airflow_env.sh gerado para ambiente HK

 Configuração aplicada:
   Ambiente: HK
   Databricks Host: https://adb-xxx-hk.azuredatabricks.net
   Cluster ID: 0401-150803-wefgy1hc
   Storage Account: stcasesantander-hk
   Key Vault: kv-case-santander-hk

 Configuração concluída!
 Para usar:
   docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

## Resumo

Fluxo completo de configuração:

1. **Configurar** `.env` principal com credenciais
2. **Executar** `python scripts/setup_airflow_env.py --env hk/prod`
3. **Iniciar** Airflow com `docker compose --env-file docker/.env up -d`
4. **Verificar** variáveis no container Airflow
5. **Usar** DAG e Jobs com configuração consistente

O sistema garante que Airflow, DAG e Jobs estejam sempre sincronizados com o ambiente correto (HK ou PROD).
