# Terraform - Infrastructure as Code Enterprise

## Por que Terraform?

### Benefícios Enterprise

1. **Infrastructure as Code (IaC)**
   - Todo o ambiente provisionado via código
   - Versionamento Git completo
   - Rollback fácil
   - Auditoria de mudanças

2. **Consistência entre Ambientes**
   - HK e PROD idênticos
   - Zero configuração manual
   - Erros humanos minimizados

3. **Custo Controlado**
   - Estimativa de custo antes do deploy
   - Eliminação de recursos não utilizados
   - Otimização automática

4. **Segurança**
   - Secrets no Key Vault (não em código)
   - RBAC automático
   - Policy enforcement

5. **CI/CD Integrado**
   - Deploy automatizado via GitHub Actions
   - Pull requests com terraform plan
   - Approval para produção

## Arquitetura Terraform

```
┌─────────────────────────────────────────────────────────────┐
│ TERRAFORM CONFIGURATION                                    │
│                                                             │
│  main.tf                                                    │
│  ├─ resource_group module                                 │
│  ├─ storage_account module                                 │
│  ├─ key_vault module                                       │
│  ├─ databricks_workspace module                           │
│  ├─ event_hub module                                       │
│  ├─ unity_catalog module                                   │
│  └─ secrets module                                        │
└─────────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────────┐
│ AZURE RESOURCES (Provisionados)                            │
│                                                             │
│  Resource Groups                                           │
│  ├─ rg-case-santander-hk                                   │
│  └─ rg-case-santander-prod                                 │
│                                                             │
│  Storage Accounts (ADLS Gen2)                              │
│  ├─ stcasesantanderhk (HK)                                 │
│  └─ stcasesantander (PROD)                                 │
│     ├─ bronze/                                             │
│     ├─ silver/                                             │
│     ├─ gold/                                               │
│     └─ checkpoints/                                        │
│                                                             │
│  Key Vaults                                                │
│  ├─ kv-case-santander-hk                                  │
│  └─ kv-case-santander                                     │
│     ├─ client-id                                          │
│     ├─ client-secret                                      │
│     ├─ tenant-id                                          │
│     ├─ storage-account                                    │
│     ├─ kaggle-username                                    │
│     └─ kaggle-key                                         │
│                                                             │
│  Databricks Workspaces                                     │
│  ├─ databricks-santander-hk (Premium)                     │
│  └─ databricks-santander (Premium)                        │
│                                                             │
│  Event Hubs                                                │
│  ├─ evhcasesantander-hk                                   │
│  └─ evhcasesantander                                      │
│     └─ transacoes-financeiras                             │
│                                                             │
│  Unity Catalog                                            │
│  └─ case_santander                                        │
│     ├─ bronze                                             │
│     ├─ silver                                             │
│     └─ gold                                               │
└─────────────────────────────────────────────────────────────┘
```

## Módulos Reutilizáveis

### 1. Resource Group
- Cria resource group
- Aplica tags

### 2. Storage Account
- Cria ADLS Gen2
- Cria containers (bronze, silver, gold, checkpoints)
- Configura access tier

### 3. Key Vault
- Cria Key Vault
- Configura access policies
- Gerencia secrets

### 4. Databricks Workspace
- Cria workspace Premium
- Configura managed resource group
- Habilita Unity Catalog

### 5. Event Hub
- Cria namespace
- Cria event hub
- Configura authorization rules

### 6. Unity Catalog
- Cria metastore
- Cria catalog
- Cria schemas (bronze, silver, gold)

### 7. Secrets
- Armazena secrets no Key Vault
- Não expõe secrets no código

## Integração com Pipeline

### Fluxo Completo

```
┌─────────────────────────────────────────────────────────────┐
│ 1. TERRAFORM DEPLOY                                        │
│                                                             │
│  terraform apply -var-file=hk.tfvars                       │
│  ↓                                                         │
│  Resource Groups criados                                    │
│  Storage Accounts criados                                  │
│  Key Vault criado                                           │
│  Databricks Workspace criado                                │
│  Event Hub criado                                          │
│  Unity Catalog criado                                     │
│  Secrets armazenados                                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. DATABRICKS ASSET BUNDLE DEPLOY                          │
│                                                             │
│  databricks bundle deploy --target hk                      │
│  ↓                                                         │
│  Jobs criados                                               │
│  Workflows criados                                          │
│  Pipelines configurados                                     │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. AIRFLOW DAG SYNC                                         │
│                                                             │
│  python scripts/sync_airflow_from_databricks.py            │
│  ↓                                                         │
│  DAG gerado automaticamente                                 │
│  Dependências sincronizadas                                 │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. PIPELINE EXECUTION                                       │
│                                                             │
│  Airflow scheduler                                         │
│  ↓                                                         │
│  Databricks jobs executados                                │
│  Dados processados                                         │
│  Grafana monitora                                          │
└─────────────────────────────────────────────────────────────┘
```

## Como "Vender" com Terraform

### Ponto 1: Enterprise-Grade Infrastructure

**Fale:**
- "Todo o ambiente provisionado via Infrastructure as Code"
- "Zero configuração manual"
- "Consistência garantida entre HK e PROD"
- "Versionamento completo em Git"

**Demonstre:**
```bash
terraform plan -var-file=hk.tfvars
# Mostra o que será criado/alterado
```

---

### Ponto 2: Cost Optimization

**Fale:**
- "Estimativa de custo antes do deploy"
- "Eliminação de recursos não utilizados"
- "Monitoramento de custos em tempo real"

**Demonstre:**
```bash
terraform plan -out=tfplan
terraform show tfplan
# Mostra custo estimado
```

---

### Ponto 3: Security & Compliance

**Fale:**
- "Secrets no Key Vault (não em código)"
- "RBAC automático"
- "Audit trail completo"

**Demonstre:**
```bash
terraform output key_vault_uri
# Key Vault protegido
```

---

### Ponto 4: CI/CD Automation

**Fale:**
- "Deploy automatizado via GitHub Actions"
- "Pull requests com terraform plan"
- "Approval para produção"

**Demonstre:**
```yaml
# .github/workflows/terraform.yml
name: Terraform Deploy
on: [push]
jobs:
  deploy:
    steps:
      - terraform apply -auto
```

---

### Ponto 5: Disaster Recovery

**Fale:**
- "Recriação completa do ambiente em minutos"
- "Backup automático do state"
- "Rollback fácil"

**Demonstre:**
```bash
terraform destroy -var-file=hk.tfvars
terraform apply -var-file=hk.tfvars
# Recria tudo do zero
```

## Comparação: Manual vs Terraform

| Aspecto | Manual | Terraform |
|---------|--------|------------|
| **Tempo de setup** | Dias | Minutos |
| **Erros humanos** | Alto | Zero |
| **Consistência** | Baixa | Alta |
| **Versionamento** | Nenhum | Git completo |
| **Rollback** | Manual | Automático |
| **Custo** | Não controlado | Otimizado |
| **Segurança** | Risco alto | Garantida |
| **Auditoria** | Manual | Automática |

## Resumo

**Terraform adiciona:**
- ✅ Infrastructure as Code
- ✅ Consistência entre ambientes
- ✅ Versionamento Git
- ✅ CI/CD automatizado
- ✅ Cost optimization
- ✅ Security garantida
- ✅ Disaster recovery

**Valor para o cliente:**
- Redução de 90% no tempo de setup
- Eliminação de erros humanos
- Compliance e segurança garantidos
- Custo controlado e otimizado
- Recuperação rápida de desastres

**Próximo passo:** Deploy Terraform para HK e mostrar o plano completo antes da execução.
