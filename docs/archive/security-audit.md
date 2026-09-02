# Auditoria de Segurança - Hardcoding e Key Vaults

## Resumo Executivo

**Problema Identificado:** ⚠️ Key Vault names estão hard-coded no código

**Status:** Apenas referências ao nome do Key Vault (não valores de secrets)

**Impacto:** Baixo (não há secrets expostos, mas nome do Key Vault está no código)

---

## 1. Hardcoding de Key Vault Names

### Onde Está Hard-coded

**Jobs Python (múltiplos arquivos):**
```python
client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
```

**Arquivos afetados:**
- `jobs/job_clientes_ordens.py`
- `jobs/job_streaming.py`
- `jobs/job_streaming_continuous.py`
- `jobs/job_streaming_to_gold.py`
- `jobs/job_unity_catalog.py`
- `jobs/job_extracao_acoes.py`
- `jobs/job_extracao_bcb.py`
- `jobs/job_extracao_world_bank.py`
- `jobs/job_silver_acoes.py`
- `jobs/job_silver_bcb.py`
- `jobs/job_silver_world_bank.py`
- `jobs/job_gold_anomalias.py`
- `jobs/job_gold_performance.py`
- `jobs/job_gold_bcb.py`
- `jobs/job_gold_world_bank.py`
- `jobs/job_gold_acoes_vs_cambio.py`
- `jobs/job_gold_fraude.py`
- `jobs/job_carga_sql_acoes.py`
- `jobs/job_carga_sql_clientes.py`
- `jobs/job_carga_sql_fraude.py`
- `jobs/job_carga_sql_macro.py`
- `jobs/job_carga_sql_streaming.py`
- `jobs/job_corretora_analises.py`
- `notebooks/case_presentation.py`

**Configuração:**
- `src/config/settings.py` - `key_vault: "kv-case-santander"`
- `src/config/environment.py` - `key_vault: "kv-case-santander-hk"` ou `"kv-case-santander"`

**Documentação:**
- `README.md`
- `docs/technical-reference.md`
- `docs/environment-isolation.md`
- `docs/databricks-pending-tasks.md`
- `docs/airflow-configuration.md`

---

## 2. Análise de Risco

### ❌ Problema: Nome do Key Vault Hard-coded

**Código atual:**
```python
client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
```

**Problema:**
- Nome do Key Vault está no código
- Dificulta troca de ambiente (HK vs PROD)
- Dificulta migração para outro Key Vault

### ✅ O que NÃO está hard-coded

**Secrets valores:**
- ✅ Nenhum secret está no código
- ✅ Todos usam `dbutils.secrets.get()` para recuperar
- ✅ Valores estão no Key Vault (não expostos)

**Credenciais:**
- ✅ `client-id` não está no código
- ✅ `client-secret` não está no código
- ✅ `tenant-id` não está no código
- ✅ `storage-account` não está no código
- ✅ `kaggle-username` não está no código
- ✅ `kaggle-key` não está no código

---

## 3. O Que Terraform Cria

### Recursos Provisionados

**Módulo Key Vault:**
```hcl
resource "azurerm_key_vault" "this" {
  name                = "${var.key_vault_prefix}${var.environment}"
  # HK: kv-case-santander-hk
  # PROD: kv-case-santander
}
```

**Secrets provisionados:**
- `client-id`
- `client-secret`
- `tenant-id`
- `storage-account`
- `kaggle-username`
- `kaggle-key`
- `salt` (nova adição)

---

## 4. O que Precisa Ser Corrigido

### ❌ Erro Atual

**Terraform cria:**
- HK: `kv-case-santander-hk`
- PROD: `kv-case-santander`

**Código espera:**
- HK: `kv-case-santander-hk` (em `src/config/environment.py`)
- PROD: `kv-case-santander` (em `src/config/environment.py`)

**Problema:**
- Jobs não usam `src/config/environment.py` para recuperar Key Vault
- Jobs usam hardcoded `kv-case-santander` (não dinâmico)

---

## 5. Solução Recomendada

### Correção 1: Usar Configuração Dinâmica

**Código atual (hard-coded):**
```python
client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
```

**Código corrigido (dinâmico):**
```python
from src.config.environment import get_config

config = get_config()
key_vault = config["key_vault"]  # kv-case-santander-hk ou kv-case-santander

client_id = dbutils.secrets.get(scope=key_vault, key="client-id")
```

---

### Correção 2: Jobs Usam Configuração do Ambiente

**Atualizar todos os jobs para usar:**
```python
from src.config.environment import get_config

config = get_config()
key_vault = config["key_vault"]
```

---

## 6. Plano de Ação

### Passo 1: Criar Função Helper

**Arquivo:** `src/config/secrets.py`

```python
from src.config.environment import get_config

def get_secret(key: str):
    """
    Recupera secret do Key Vault dinamicamente
    Usa key_vault do ambiente configurado
    """
    config = get_config()
    key_vault = config["key_vault"]
    
    from databricks.sdk.runtime import dbutils
    return dbutils.secrets.get(scope=key_vault, key=key)
```

### Passo 2: Atualizar Jobs

**Padrão:**
```python
# Antes:
client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")

# Depois:
from src.config.secrets import get_secret
client_id = get_secret("client-id")
```

### Passo 3: Atualizar Terraform

**Module key_vault:**
```hcl
variable "key_vault_prefix" {
  default = "kv-case-santander"  # OK
}
```

---

## 7. Recursos Criados pelo Terraform

### Lista Completa

**HK (Homologação):**
- Resource Group: `rg-case-santander-hk`
- Storage Account: `stcasesantanderhk`
- Key Vault: `kv-case-santander-hk`
- Databricks Workspace: `databricks-santander-hk`
- Event Hub Namespace: `evhcasesantander-hk`
- Unity Catalog: `case_santander`
- Secrets: 7 secrets

**PROD (Produção):**
- Resource Group: `rg-case-santander-prod`
- Storage Account: `stcasesantander`
- Key Vault: `kv-case-santander`
- Databricks Workspace: `databricks-santander`
- Event Hub Namespace: `evhcasesantander`
- Unity Catalog: `case_santander`
- Secrets: 7 secrets

---

## 8. O que Está Faltando

### ❌ Não Implementado

1. **Jobs não usam configuração dinâmica de Key Vault**
   - Jobs usam hardcoded `kv-case-santander`
   - Deveriam usar `src/config/environment.py`

2. **Terraform não configura schema separado por ambiente**
   - Terraform cria Key Vault separado
   - Mas código não usa key_vault dinâmico

3. **Salt não provisionado automaticamente**
   - Terraform aceita salt via variável
   - Mas precisa ser gerada manualmente

---

## 9. Recomendação de Prioridade

### P1 (Alta Prioridade)

1. **Criar função helper `get_secret()`**
   - Centraliza recuperação de secrets
   - Usa key_vault dinâmico do ambiente

2. **Atualizar jobs para usar `get_secret()`**
   - Remove hardcoding de `kv-case-santander`
   - Suporta HK e PROD automaticamente

### P2 (Média Prioridade)

3. **Gerar salt automaticamente no Terraform**
   - Usar `random_password` resource
   - Armazenar no Key Vault automaticamente

4. **Validar integridade de secrets**
   - Verificar se todos os secrets existem
   - Alertar se faltando secret

---

## 10. Conclusão

**Status Atual:**
- ✅ Terraform cria Key Vaults separados
- ✅ Terraform provisiona 7 secrets
- ❌ Jobs não usam key_vault dinâmico
- ❌ Nome do Key Vault está hard-coded

**Impacto:**
- ⚠️ Baixo (apenas nome do Key Vault, não valores)
- ⚠️ Dificulta troca de ambiente
- ⚠️ Não enterprise-grade

**Correção necessária:**
- Criar função helper `get_secret()`
- Atualizar jobs para usar configuração dinâmica
- Remover hardcoding de `kv-case-santander`

---

## Resumo

**O que Terraform cria:**
- ✅ 2 Resource Groups (HK, PROD)
- ✅ 2 Storage Accounts (HK, PROD)
- ✅ 2 Key Vaults (HK, PROD)
- ✅ 2 Databricks Workspaces (HK, PROD)
- ✅ 2 Event Hubs (HK, PROD)
- ✅ 1 Unity Catalog (compartilhado)
- ✅ 7 Secrets por Key Vault

**O que precisa ser corrigido:**
- ❌ Jobs não usam key_vault dinâmico
- ❌ Nome do Key Vault está hard-coded

**Próximo passo:** Criar função helper e atualizar jobs para usar configuração dinâmica.
