#  Deployment Guide - Asset Bundles + GitHub Actions

**Guia completo para sincronizar Airflow ↔ Databricks com Asset Bundles**

---

##  Pré-requisitos

 **Ferramentas Necessárias:**
- Databricks CLI (`pip install databricks-cli`)
- Python 3.9+
- Git configurado
- Conta no GitHub com permissões de repository settings

 **Credenciais Necessárias:**
- Token Databricks (DEV)
- Token Databricks (PROD)
- GitHub Personal Access Token (PAT)
- Databricks Host URLs (DEV e PROD)
- Cluster IDs (DEV e PROD)

---

##  1. Configurar Databricks CLI Localmente

### Passo 1.1: Instalar CLI

```bash
pip install databricks-cli
```

### Passo 1.2: Autenticar com Databricks

```bash
# Para DEV
export DATABRICKS_HOST=https://dev.cloud.databricks.com
export DATABRICKS_TOKEN=dapi... # Token DEV

# Testar conexão
databricks --version
databricks workspace list /
```

### Passo 1.3: Criar arquivo de config local

```bash
# ~/.databricks/config
[DEFAULT]
host = https://dev.cloud.databricks.com
token = dapi...

[PROD]
host = https://prod.cloud.databricks.com
token = dapi...
```

---

##  2. Configurar GitHub Secrets

### Passo 2.1: Acessar Settings do Repository

```
GitHub → Repository Settings → Secrets and variables → Actions
```

### Passo 2.2: Adicionar Secrets

**Para Development:**
```
DATABRICKS_HOST_DEV = https://dev.cloud.databricks.com
DATABRICKS_TOKEN_DEV = dapi...
DEV_CLUSTER_ID = 0401-150803-wefgy1hc
```

**Para Production:**
```
DATABRICKS_HOST_PROD = https://prod.cloud.databricks.com
DATABRICKS_TOKEN_PROD = dapi...
PROD_CLUSTER_ID = 0401-150803-xxxyyyzzz
```

**Opcional (Notificações):**
```
SLACK_WEBHOOK_URL = https://hooks.slack.com/services/...
```

###  IMPORTANTE: Nunca commite secrets!

```bash
# Verificar .gitignore
echo ".env*" >> .gitignore
echo "secrets/*" >> .gitignore
```

---

##  3. Deploy Local (Testing)

### Passo 3.1: Validar configuração

```bash
# Validar YAML
python -c "
import yaml
with open('databricks.yml', 'r') as f:
    config = yaml.safe_load(f)
    print(' Valid configuration')
"

# Validar Python
find jobs -name '*.py' -exec python -m py_compile {} \;
find src -name '*.py' -exec python -m py_compile {} \;
```

### Passo 3.2: Deploy em DEV

```bash
# Configurar variáveis de ambiente
export DATABRICKS_HOST_DEV=https://dev.cloud.databricks.com
export DATABRICKS_TOKEN_DEV=dapi...
export DEV_CLUSTER_ID=0401-150803-wefgy1hc

# Deploy
databricks bundle deploy --target dev

# Verificar
databricks jobs list
```

### Passo 3.3: Verificar Jobs Criados

```bash
# Listar jobs
databricks jobs list --output json | python -m json.tool

# Ver detalhes de um job específico
databricks jobs get --job-id 123
```

---

##  4. Workflow CI/CD

### Passo 4.1: Push para develop (testa em DEV)

```bash
git checkout develop
git add databricks.yml .github/workflows/
git commit -m "feat: add Asset Bundles deployment"
git push origin develop
```

**O que acontece:**
1.  Validates YAML syntax
2.  Validates Python syntax
3.  Validates Airflow DAG
4.  Deploys to DEV
5.  Verifies jobs created
6.  (Optional) Slack notification

### Passo 4.2: Merge para main (deploy em PROD)

```bash
# Criar tag/release
git tag -a v1.0.0 -m "Release 1.0.0"
git push origin v1.0.0
```

**O que acontece:**
1.  Todas as validações
2.  Backup do estado PROD
3.  Deploy em PROD
4.  Create GitHub Release
5.  Slack notification

---

##  5. Monitorar Deployments

### GitHub Actions

```
Repository → Actions → Deploy to Databricks
```

**Ver logs:**
- Cada workflow execution tem logs detalhados
- Artifacts disponíveis (logs, backups)

### Databricks Workspace

```
Workspace → Workflows → Monitor
```

**Ver jobs:**
- Todos os jobs criados via Asset Bundle
- Status de execução
- Logs de cada run

### Airflow

```
Airflow UI → DAGs → pipeline_corretora_santander
```

**Referencia jobs:**
```python
t3_anomalias = DatabricksRunNowOperator(
    task_id="t3_anomalias",
    databricks_conn_id="databricks_default",
    job_id=123,  # ID criado pelo Asset Bundle
    wait_for_termination=True,
)
```

---

##  6. Atualizar Jobs

### Workflow para Atualizar um Job

```bash
# 1. Editar job Python
vim jobs/job_gold_anomalias.py

# 2. Commit e push para develop
git add jobs/job_gold_anomalias.py
git commit -m "fix: optimize gold anomalias query"
git push origin develop

# 3. GitHub Actions testa em DEV
# 4. Se OK, merge para main
git checkout main
git merge develop
git push origin main

# 5. Criar release
git tag -a v1.0.1 -m "Fix anomalias query"
git push origin v1.0.1

# 6. GitHub Actions deploys em PROD
```

---

##  7. Troubleshooting

### Erro: "Invalid token"

```bash
# Verificar token
echo $DATABRICKS_TOKEN
echo $DATABRICKS_HOST

# Testar conexão
databricks workspace list /
```

### Erro: "Cluster not found"

```bash
# Listar clusters disponíveis
databricks clusters list

# Atualizar CLUSTER_ID em databricks.yml
```

### Erro: "Module not found"

```bash
# Verificar imports
python -c "from case_santander.jobs import job_gold_anomalias"

# Instalar package
pip install -e .
```

### Erro: "DAG validation failed"

```bash
# Validar DAG localmente
export AIRFLOW_HOME=/tmp/airflow
python -c "
from dags.dag_pipeline_santander import dag
print('DAG:', dag.dag_id)
print('Tasks:', len(dag.tasks))
"
```

---

##  8. Monitorar Custo

### Jobs estão rodando demais?

```bash
# Ver histórico de runs
databricks jobs get-run --run-id 123 --output json | jq '.execution_duration'

# Listar últimas execuções
databricks jobs runs list --limit 10
```

### Otimizar clusters

```yaml
# databricks.yml - aumentar timeout para reduzir restarts
task:
  timeout_seconds: 3600  # 1 hora
  max_retries: 2
```

---

##  9. Checklist de Deploy

- [ ] Tokens configurados no GitHub Secrets
- [ ] databricks.yml validado
- [ ] Todos os jobs Python compiláveis
- [ ] Airflow DAG válido
- [ ] Deploy em DEV bem-sucedido
- [ ] Jobs appearing no Databricks DEV
- [ ] Merge para main
- [ ] Tag release criada
- [ ] Deploy em PROD bem-sucedido
- [ ] Jobs appearing no Databricks PROD
- [ ] Airflow DAG referenciando job IDs corretos

---

##  10. Comandos Úteis

```bash
# Validar localmente
databricks bundle validate --target dev

# Deploy DEV
databricks bundle deploy --target dev

# Deploy PROD
databricks bundle deploy --target prod

# Ver estado atual
databricks bundle status

# Reverter (desfazer)
git revert HEAD
git push origin main

# Listar jobs criados
databricks jobs list --output json

# Ver logs de um job
databricks runs get-output --run-id 123
```

---

##  Suporte

**Problemas?**

1. Verificar logs do GitHub Actions
2. Verificar logs do Databricks
3. Validar credentials
4. Testar localmente com `databricks bundle deploy --target dev`

**Documentação:**
- https://docs.databricks.com/en/dev-tools/bundles/index.html
- https://docs.databricks.com/en/dev-tools/cli/index.html
- https://docs.databricks.com/en/workflows/index.html

---

##  Notas Importantes

 **Asset Bundles:**
- Definem jobs como código
- Versionam com git
- Deploy repetível e idempotente

 **Databricks Repos:**
- Sincroniza código via git
- Atualiza automaticamente
- Não reescreve cada vez

 **Airflow DAG:**
- Referencia jobs pelo ID
- Não precisa de mudanças
- Executa jobs no Databricks

 **CI/CD Pipeline:**
- Valida em cada push
- Testa em DEV
- Deploy automático em PROD

---

**Status:**  PRONTO PARA USAR

Próximos passos:
1. Configurar secrets no GitHub
2. Fazer primeiro deploy em DEV
3. Verificar jobs criados
4. Atualizar Airflow DAG com job IDs
5. Deploy em PROD
