# Pontos Pendentes - Configuração de Ambientes no Databricks

##  Concluído (Airflow)
- [x] Sistema de configuração HK/PROD em `src/config/environment.py`
- [x] Integração do Airflow com variáveis de ambiente
- [x] Script de setup `scripts/setup_airflow_env.py`
- [x] Atualização do `docker/docker-compose.yml`
- [x] Documentação de configuração do Airflow

## ⏳ Pendentes (Databricks)

### 1. Atualizar `databricks.yml` para Ambientes HK/PROD

**Status:**  PENDENTE

**Atual:** 
```yaml
environments:
  dev:  #  Remover - não existe mais
    workspace:
      host: ${env.DATABRICKS_HOST_DEV}
      root_path: /Workspace/Users/diego.silva0001@gmail.com/bundles/case-santander-dev
    variables:
      cluster_id: ${env.DEV_CLUSTER_ID}
      environment: dev

  prod:
    workspace:
      host: ${env.DATABRICKS_HOST_PROD}
      root_path: /Workspace/Shared/bundles/case-santander-prod
    variables:
      cluster_id: ${env.PROD_CLUSTER_ID}
      environment: prod
```

**Deveria ser:**
```yaml
environments:
  hk:  #  Adicionar - homologação reduzido
    workspace:
      host: ${env.DATABRICKS_HOST_HK}
      root_path: /Workspace/Shared/bundles/case-santander-hk
    variables:
      cluster_id: ${env.DATABRICKS_CLUSTER_ID_HK}
      environment: hk
      enable_streaming: false  # HK sem streaming

  prod:
    workspace:
      host: ${env.DATABRICKS_HOST_PROD}
      root_path: /Workspace/Shared/bundles/case-santander-prod
    variables:
      cluster_id: ${env.DATABRICKS_CLUSTER_ID_PROD}
      environment: prod
      enable_streaming: true   # PROD com streaming
```

**Ação Necessária:**
- [ ] Remover ambiente `dev` do `databricks.yml`
- [ ] Adicionar ambiente `hk` ao `databricks.yml`
- [ ] Atualizar variáveis para HK (host, cluster_id, etc.)
- [ ] Adicionar variável `enable_streaming` por ambiente

---

### 2. Atualizar Jobs Python para Usar `src/config/environment.py`

**Status:**  PENDENTE

**Jobs que precisam ser atualizados:**

#### Ingestão
- [ ] `jobs/job_extracao_acoes.py` - Já atualizado 
- [ ] `jobs/job_extracao_bcb.py` - Já atualizado 
- [ ] `jobs/job_extracao_world_bank.py` - Precisa atualizar
- [ ] `jobs/job_clientes_ordens.py` - Precisa atualizar

#### Transformação
- [ ] `jobs/job_silver_acoes.py` - Precisa atualizar
- [ ] `jobs/job_silver_bcb.py` - Precisa atualizar
- [ ] `jobs/job_silver_world_bank.py` - Precisa atualizar
- [ ] `jobs/job_clientes_silver.py` - Precisa atualizar

#### Gold
- [ ] `jobs/job_gold_anomalias.py` - Precisa atualizar
- [ ] `jobs/job_gold_performance.py` - Precisa atualizar
- [ ] `jobs/job_gold_bcb.py` - Precisa atualizar
- [ ] `jobs/job_gold_world_bank.py` - Precisa atualizar
- [ ] `jobs/job_gold_fraude.py` - Precisa atualizar
- [ ] `jobs/job_gold_acoes_vs_cambio.py` - Precisa atualizar

#### Outros
- [ ] `jobs/job_streaming.py` - Precisa atualizar (usar `enable_streaming`)

**Padrão de atualização:**

**Antes:**
```python
from databricks.sdk.runtime import dbutils

client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")
```

**Depois:**
```python
from src.config.environment import get_config, get_env

env = get_env()  # "hk" ou "prod"
config = get_config()

storage_account = config["storage_account"]  # Já selecionado por ambiente
catalog = config["catalog"]
schema_prefix = config["schema_prefix"]
```

**Ação Necessária:**
- [ ] Atualizar cada job para usar `src/config/environment.py`
- [ ] Remover chamadas diretas ao `dbutils.secrets` para recursos Azure
- [ ] Usar schemas separados (`hk_bronze` vs `prod_bronze`)
- [ ] Adicionar tags de ambiente nos dados
- [ ] Adicionar logs com ambiente atual

---

### 3. Atualizar Unity Catalog Setup

**Status:**  PENDENTE

**Atual:**
```python
# jobs/job_unity_catalog.py
CATALOG = "case_santander"
SCHEMAS = ["bronze", "silver", "gold"]
```

**Deveria ser:**
```python
# jobs/job_unity_catalog.py
from src.config.environment import get_config, get_env

env = get_env()
config = get_config()

CATALOG = config["catalog"]  # "case_santander" (mesmo para ambos)
SCHEMA_PREFIX = config["schema_prefix"]  # "hk_" ou "prod_"

# Criar schemas separados por ambiente
SCHEMAS = [
    f"{SCHEMA_PREFIX}bronze",  # hk_bronze ou prod_bronze
    f"{SCHEMA_PREFIX}silver",  # hk_silver ou prod_silver
    f"{SCHEMA_PREFIX}gold"     # hk_gold ou prod_gold
]
```

**Ação Necessária:**
- [ ] Atualizar `jobs/job_unity_catalog.py` para usar `src/config/environment.py`
- [ ] Criar schemas separados: `hk_bronze`, `hk_silver`, `hk_gold`
- [ ] Criar schemas separados: `prod_bronze`, `prod_silver`, `prod_gold`
- [ ] Atualizar todos os jobs para usar schemas separados
- [ ] Atualizar Unity Catalog tables paths

---

### 4. Atualizar Workflows de CI/CD

**Status:**  PENDENTE

**Arquivo:** `.github/workflows/deploy-databricks.yml`

**Atual:**
```yaml
deploy-dev:
  if: github.ref == 'refs/heads/develop'
  environment:
    name: development
  steps:
    - name: Deploy Asset Bundle
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_DEV }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        DEV_CLUSTER_ID: ${{ secrets.DEV_CLUSTER_ID }}
```

**Deveria ser:**
```yaml
deploy-hk:
  if: github.ref == 'refs/heads/develop'
  environment:
    name: homologation
  steps:
    - name: Deploy Asset Bundle
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_HK }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_HK }}
        DATABRICKS_CLUSTER_ID_HK: ${{ secrets.DATABRICKS_CLUSTER_ID_HK }}
        ENVIRONMENT: hk
```

**Ação Necessária:**
- [ ] Renomear `deploy-dev` para `deploy-hk`
- [ ] Atualizar secrets de `DEV_*` para `HK_*`
- [ ] Atualizar trigger para branch `develop` → HK
- [ ] Adicionar variável `ENVIRONMENT=hk`
- [ ] Atualizar `deploy-prod` para usar variáveis `PROD_*`
- [ ] Adicionar variável `ENVIRONMENT=prod`

---

### 5. Configurar Secrets no GitHub Actions

**Status:**  PENDENTE

**Secrets necessários no GitHub:**

**HK (Homologação):**
- [ ] `DATABRICKS_HOST_HK`
- [ ] `DATABRICKS_TOKEN_HK`
- [ ] `DATABRICKS_CLUSTER_ID_HK`
- [ ] `STORAGE_ACCOUNT_HK`
- [ ] `KEY_VAULT_HK`

**PROD (Produção):**
- [ ] `DATABRICKS_HOST_PROD`
- [ ] `DATABRICKS_TOKEN_PROD`
- [ ] `DATABRICKS_CLUSTER_ID_PROD`
- [ ] `STORAGE_ACCOUNT_PROD`
- [ ] `KEY_VAULT_PROD`

**Ação Necessária:**
- [ ] Configurar secrets no GitHub (Settings → Secrets)
- [ ] Remover secrets `DEV_*` (não usados mais)
- [ ] Testar conexão com cada ambiente

---

### 6. Atualizar Key Vault no Databricks

**Status:**  PENDENTE

**Atual:**
```python
# src/config/settings.py
def get_credentials(dbutils):
    return {
        "client_id": dbutils.secrets.get(scope="kv-case-santander", key="client-id"),
        "storage_account": dbutils.secrets.get(scope="kv-case-santander", key="storage-account"),
        # ...
    }
```

**Deveria ser:**
```python
# src/config/settings.py (ou usar environment.py)
from src.config.environment import get_config

def get_credentials(dbutils):
    config = get_config()
    key_vault = config["key_vault"]  # kv-case-santander-hk ou kv-case-santander
    
    return {
        "client_id": dbutils.secrets.get(scope=key_vault, key="client-id"),
        "storage_account": dbutils.secrets.get(scope=key_vault, key="storage-account"),
        # ...
    }
```

**Ação Necessária:**
- [ ] Criar Key Vault separado para HK: `kv-case-santander-hk`
- [ ] Mover segredos do HK para o novo Key Vault
- [ ] Atualizar `src/config/settings.py` para usar Key Vault dinâmico
- [ ] Criar secret scope no Databricks para HK

---

### 7. Testar Isolamento de Ambientes

**Status:**  PENDENTE

**Testes necessários:**

**HK (Homologação):**
- [ ] Deploy para HK via `databricks bundle deploy --target hk`
- [ ] Verificar se jobs usam `stcasesantander-hk`
- [ ] Verificar se jobs usam schemas `hk_bronze`, `hk_silver`, `hk_gold`
- [ ] Verificar se streaming está desabilitado
- [ ] Verificar rate limits reduzidos
- [ ] Testar retenção de 30 dias

**PROD (Produção):**
- [ ] Deploy para PROD via `databricks bundle deploy --target prod`
- [ ] Verificar se jobs usam `stcasesantander`
- [ ] Verificar se jobs usam schemas `prod_bronze`, `prod_silver`, `prod_gold`
- [ ] Verificar se streaming está habilitado
- [ ] Verificar rate limits completos
- [ ] Testar retenção de 90 dias

**Isolamento:**
- [ ] Verificar se dados HK não aparecem em PROD
- [ ] Verificar se dados PROD não aparecem em HK
- [ ] Testar falha em um ambiente não afeta o outro

---

### 8. Atualizar Documentação

**Status:**  PENDENTE

**Arquivos a atualizar:**

- [ ] `README.md` - Atualizar ambientes de "dev, hk, prod" para "hk, prod"
- [ ] `docs/technical-reference.md` - Atualizar arquitetura de ambientes
- [ ] `docs/cicd.md` - Atualizar fluxo de CI/CD
- [ ] `README.md` - Atualizar seção de orquestração

**Ação Necessária:**
- [ ] Remover referências a ambiente "dev"
- [ ] Adicionar documentação sobre HK reduzido
- [ ] Atualizar diagramas de arquitetura
- [ ] Documentar economia de custo HK vs PROD

---

##  Checklist de Implementação

### Fase 1: Configuração Básica
- [ ] Atualizar `databricks.yml` (remover dev, adicionar hk)
- [ ] Configurar secrets GitHub Actions (HK_* e PROD_*)
- [ ] Atualizar CI/CD workflows (deploy-hk)

### Fase 2: Jobs Python
- [ ] Atualizar jobs de ingestão para usar `environment.py`
- [ ] Atualizar jobs de transformação para usar `environment.py`
- [ ] Atualizar jobs de gold para usar `environment.py`
- [ ] Atualizar jobs de carga SQL para usar `environment.py`

### Fase 3: Unity Catalog
- [ ] Atualizar `job_unity_catalog.py`
- [ ] Criar schemas separados (hk_* e prod_*)
- [ ] Atualizar paths de tabelas em todos os jobs

### Fase 4: Key Vault
- [ ] Criar Key Vault para HK
- [ ] Mover segredos HK para novo Key Vault
- [ ] Atualizar secret scopes no Databricks

### Fase 5: Testes
- [ ] Testar deploy HK
- [ ] Testar deploy PROD
- [ ] Verificar isolamento entre ambientes
- [ ] Validar economia de custo HK

### Fase 6: Documentação
- [ ] Atualizar README.md
- [ ] Atualizar documentação técnica
- [ ] Documentar processo de deploy

---

##  Priorização

### Alta Prioridade (Bloqueio)
1. **Atualizar `databricks.yml`** - Sem isso, deploy não funciona
2. **Configurar secrets GitHub** - Sem isso, CI/CD não funciona
3. **Atualizar CI/CD workflows** - Sem isso, deploy automático não funciona

### Média Prioridade (Funcionalidade)
4. **Atualizar jobs Python** - Necessário para isolamento funcionar
5. **Atualizar Unity Catalog** - Necessário para schemas separados
6. **Criar Key Vault HK** - Necessário para isolamento de segredos

### Baixa Prioridade (Otimização)
7. **Testes de isolamento** - Validação final
8. **Atualizar documentação** - Manutenção

---

##  Notas

- **Custo:** Criar Key Vault HK tem custo mínimo (~R$ 30/mês)
- **Tempo estimado:** Fase 1-2 (1 dia), Fase 3-4 (2 dias), Fase 5-6 (1 dia)
- **Risco:** Médio - alterações em jobs podem quebrar pipeline existente
- **Recomendação:** Implementar em fases, testando cada uma antes de próxima

---

##  Documentação Relacionada

- `docs/environment-isolation.md` - Detalhes do sistema de ambientes
- `docs/api-security-governance.md` - Boas práticas de APIs
- `docs/airflow-configuration.md` - Configuração do Airflow (já concluído)
- `docs/airflow-databricks-sync.md` - Sincronização Airflow ↔ Databricks
