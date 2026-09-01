# CI/CD Multi-Ambiente - Guia Completo

## Visão Geral

CI/CD completo para múltiplos ambientes (HK e PROD) com validações e atualização automática do Airflow DAG.

---

## Arquitetura do CI/CD

```
┌─────────────────────────────────────────────────────────────┐
│ CI/CD PIPELINE (GitHub Actions)                             │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 1. VALIDAÇÃO E TESTES                               │  │
│  │    - Lint                                          │  │
│  │    - Testes de qualidade de dados                 │  │
│  │    - Validação de sintaxe                          │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 2. BUILD HK (Homologação)                         │  │
│  │    - Validate Databricks Bundle                   │  │
│  │    - Deploy to HK                                  │  │
│  │    - Executar jobs teste                          │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 3. BUILD PROD (Produção)                          │  │
│  │    - Validate Databricks Bundle                   │  │
│  │    - Deploy to PROD                                │  │
│  │    - Executar jobs produção                       │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 4. ATUALIZAÇÃO AUTOMÁTICA DO AIRFLOW DAG          │  │
│  │    - Gerar DAG do databricks.yml                  │  │
│  │    - Validar DAG                                  │  │
│  │    - Commit DAG no repositório                     │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 5. DEPLOY TERRAFORM (OPCIONAL)                    │  │
│  │    - Plan Terraform (HK)                          │  │
│  │    - Apply Terraform (HK)                         │  │
│  │    - Plan Terraform (PROD)                        │  │
│  │    - Apply Terraform (PROD)                       │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 6. HEALTH CHECKS                                   │  │
│  │    - Verificar conexão Databricks HK              │  │
│  │    - Verificar conexão Databricks PROD            │  │
│  │    - Verificar saúde do sistema                  │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                      │
│                     ▼                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 7. NOTIFICAÇÃO                                    │  │
│  │    - Sucesso                                      │  │
│  │    - Falha                                        │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Workflows GitHub Actions

### 1. CI/CD Pipeline Principal

**Arquivo:** `.github/workflows/ci-cd.yml`

**Triggers:**
- Push para `main` ou `develop`
- Pull request para `main`
- Manual (workflow_dispatch)

**Jobs:**
1. **validate** - Validação e testes
2. **build-hk** - Build e deploy HK
3. **build-prod** - Build e deploy PROD
4. **update-airflow-dag** - Atualização automática do DAG
5. **deploy-terraform-hk** - Deploy Terraform HK
6. **deploy-terraform-prod** - Deploy Terraform PROD
7. **health-check** - Health checks pós-deploy
8. **notify** - Notificação

---

### 2. Atualização Automática do Airflow DAG

**Arquivo:** `.github/workflows/update-airflow-dag.yml`

**Triggers:**
- Push para `main` ou `develop` (quando databricks.yml muda)
- Manual (workflow_dispatch)

**Jobs:**
1. **validate-databricks-yml** - Valida YAML
2. **generate-dag** - Gera DAG automaticamente
3. **validate-dag** - Valida DAG gerado
4. **update-repo** - Commit DAG no repositório
5. **notify** - Notificação

---

## Configuração de Secrets

### Secrets GitHub Necessários

**Databricks:**
- `DATABRICKS_HOST_HK` - Host do Databricks HK
- `DATABRICKS_TOKEN_HK` - Token do Databricks HK
- `DATABRICKS_HOST_PROD` - Host do Databricks PROD
- `DATABRICKS_TOKEN_PROD` - Token do Databricks PROD

**Azure (para Terraform):**
- `AZURE_CREDENTIALS_HK` - Credenciais Azure HK
- `AZURE_CREDENTIALS_PROD` - Credenciais Azure PROD

**GitHub:**
- `GITHUB_TOKEN` - Token para commit automático do DAG

---

## Fluxo de Deploy por Ambiente

### Ambiente HK (Homologação)

**Trigger:** Push para `develop` ou `main`

**Fluxo:**
1. ✅ Validação e testes
2. ✅ Build HK
3. ✅ Deploy HK (Databricks Bundle)
4. ✅ Deploy Terraform HK (opcional)
5. ✅ Health Check HK

**Comando manual:**
```bash
git push origin develop
```

---

### Ambiente PROD (Produção)

**Trigger:** Push para `main` (proteção via branch protection)

**Fluxo:**
1. ✅ Validação e testes
2. ✅ Build PROD
3. ✅ Deploy PROD (Databricks Bundle)
4. ✅ Deploy Terraform PROD (opcional)
5. ✅ Health Check PROD
6. ✅ Atualização automática do Airflow DAG

**Comando manual:**
```bash
git push origin main
```

---

## Atualização Automática do Airflow DAG

### Fluxo Automático

**Trigger:** Quando `databricks.yml` muda

**Fluxo:**
1. ✅ Valida `databricks.yml`
2. ✅ Gera DAG via `auto_generate_dag.py`
3. ✅ Valida DAG gerado
4. ✅ Commit DAG no repositório
5. ✅ Push DAG
6. ✅ Airflow carrega DAG automaticamente

**Arquivo gerado:** `dags/dag_pipeline_santander_auto.py`

---

### Fluxo Manual

**Comando:**
```bash
python scripts/auto_generate_dag.py
```

**Ou via GitHub Actions:**
```bash
gh workflow run update-airflow-dag.yml
```

---

## Validações

### 1. Validação de Código

**Lint:**
```bash
flake8 src/ --max-line-length=120
```

**Testes:**
```bash
pytest tests/test_data_quality.py -v
```

**Sintaxe:**
```bash
python -m py_compile jobs/*.py
```

---

### 2. Validação Databricks Bundle

**HK:**
```bash
databricks bundle validate -e hk
```

**PROD:**
```bash
databricks bundle validate -e prod
```

---

### 3. Validação DAG

**Sintaxe:**
```python
from airflow import DAG
exec(open('dags/dag_pipeline_santander_auto.py').read())
```

---

## Deploy Estratégico

### Branch Protection

**Branch `main`:**
- ✅ Protegido contra push direto
- ✅ Requer pull request
- ✅ Requer aprovação
- ✅ Status checks obrigatórios

**Branch `develop`:**
- ✅ Deploy automático para HK
- ✅ Não requer aprovação

---

### Status Checks

**Antes de merge para `main`:**
- ✅ validate passed
- ✅ build-hk passed
- ✅ build-prod passed
- ✅ health-check passed

---

## Health Checks

### Pós-Deploy

**HK:**
```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql('SELECT 1').collect()
```

**PROD:**
```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql('SELECT 1').collect()
```

---

## Notificações

### Sucesso
- ✅ "Deploy realizado com sucesso!"
- Pode adicionar Slack, Email, etc.

### Falha
- ❌ "Deploy falhou!"
- Pode adicionar Slack, Email, PagerDuty, etc.

---

## Resumo

### CI/CD Principal (ci-cd.yml)

**Ambientes:**
- HK (homologação)
- PROD (produção)

**Jobs:**
1. Validação e testes
2. Build HK
3. Build PROD
4. Atualização automática do DAG
5. Deploy Terraform (opcional)
6. Health checks
7. Notificação

---

### Atualização do DAG (update-airflow-dag.yml)

**Trigger:**
- databricks.yml muda
- Manual

**Jobs:**
1. Valida databricks.yml
2. Gera DAG automaticamente
3. Valida DAG
4. Commit DAG
5. Notificação

---

### Próximo Passo

1. **Configurar secrets no GitHub**
   - DATABRICKS_HOST_HK
   - DATABRICKS_TOKEN_HK
   - DATABRICKS_HOST_PROD
   - DATABRICKS_TOKEN_PROD
   - AZURE_CREDENTIALS_HK
   - AZURE_CREDENTIALS_PROD
   - GITHUB_TOKEN

2. **Configurar branch protection**
   - Proteger branch `main`
   - Requer pull request
   - Requer aprovação

3. **Testar workflow**
   - Push para `develop` (deploy HK)
   - Criar PR para `main` (deploy PROD)

**Documentação completa:** `.github/workflows/ci-cd.yml` e `.github/workflows/update-airflow-dag.yml`
