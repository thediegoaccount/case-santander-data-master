# Terraform - Infrastructure as Code

## Visão Geral

Terraform provisiona toda a infraestrutura Azure para o case Santander Data Engineering:

- Resource Groups
- Storage Accounts (ADLS Gen2)
- Key Vault (secrets management)
- Databricks Workspace
- Event Hub (streaming)
- Unity Catalog
- Secrets (client-id, client-secret, etc.)

## Estrutura

```
terraform/
├── main.tf                 # Configuração principal
├── variables.tf            # Variáveis
├── outputs.tf              # Outputs
├── environments/
│   ├── hk.tfvars          # Variáveis HK
│   └── prod.tfvars        # Variáveis PROD
└── modules/
    ├── resource_group/    # Resource Group
    ├── storage_account/   # ADLS Gen2
    ├── key_vault/         # Key Vault
    ├── databricks_workspace/  # Databricks
    ├── event_hub/         # Event Hub
    ├── unity_catalog/     # Unity Catalog
    └── secrets/           # Secrets management
```

## Pré-requisitos

1. **Azure CLI instalado:**
```bash
az login
az account set --subscription YOUR_SUBSCRIPTION_ID
```

2. **Terraform instalado:**
```bash
terraform --version
```

3. **Service Principal criado:**
```bash
az ad sp create-for-rbac --name "terraform-santander" --role="Contributor" --scopes="/subscriptions/YOUR_SUBSCRIPTION_ID"
```

4. **Variáveis de ambiente:**
```bash
export ARM_CLIENT_ID="YOUR_CLIENT_ID"
export ARM_CLIENT_SECRET="YOUR_CLIENT_SECRET"
export ARM_SUBSCRIPTION_ID="YOUR_SUBSCRIPTION_ID"
export ARM_TENANT_ID="YOUR_TENANT_ID"
```

## Deploy

### 1. Inicializar Terraform

```bash
cd terraform
terraform init
```

### 2. Validar Configuração

```bash
terraform validate
terraform plan -var-file=environments/hk.tfvars
```

### 3. Deploy para HK (Homologação)

```bash
terraform apply -var-file=environments/hk.tfvars
```

### 4. Deploy para PROD (Produção)

```bash
terraform apply -var-file=environments/prod.tfvars
```

## Destruição

```bash
# Destruir HK
terraform destroy -var-file=environments/hk.tfvars

# Destruir PROD
terraform destroy -var-file=environments/prod.tfvars
```

## Outputs

Após o deploy, os outputs estão disponíveis:

```bash
terraform output
```

**Outputs principais:**
- `resource_group_name`
- `storage_account_name`
- `storage_account_primary_endpoint`
- `key_vault_name`
- `key_vault_uri`
- `databricks_workspace_url`
- `databricks_workspace_id`
- `event_hub_namespace_name`
- `event_hub_name`
- `event_hub_connection_string`
- `unity_catalog_name`

## Integração com Databricks Asset Bundles

Após o deploy Terraform, atualize suas variáveis de ambiente:

```bash
# Para HK
export DATABRICKS_HOST_HK=$(terraform output -raw databricks_workspace_url)
export STORAGE_ACCOUNT_HK=$(terraform output -raw storage_account_name)
export KEY_VAULT_HK=$(terraform output -raw key_vault_name)

# Para PROD
export DATABRICKS_HOST_PROD=$(terraform output -raw databricks_workspace_url)
export STORAGE_ACCOUNT_PROD=$(terraform output -raw storage_account_name)
export KEY_VAULT_PROD=$(terraform output -raw key_vault_name)
```

## Segurança

### Secrets

Os secrets são armazenados no Key Vault via Terraform:

- `client-id`: Azure AD Client ID
- `client-secret`: Azure AD Client Secret
- `tenant-id`: Azure AD Tenant ID
- `storage-account`: Storage Account name
- `kaggle-username`: Kaggle API username
- `kaggle-key`: Kaggle API key
- `salt`: Salt para anonimização de dados (SHA256 hashing)

**Importante:** Não commitar secrets no repositório. Use variáveis de ambiente ou Azure Key Vault.

### Backend

O state do Terraform é armazenado no Azure Storage:

```hcl
backend "azurerm" {
  resource_group_name  = "rg-terraform-state"
  storage_account_name = "stterrasantander"
  container_name       = "tfstate"
  key                  = "case-santander-data.tfstate"
}
```

## CI/CD

### GitHub Actions

```yaml
name: Terraform Deploy

on:
  push:
    branches: [main]

jobs:
  deploy-hk:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to HK
        run: |
          cd terraform
          terraform init
          terraform apply -var-file=environments/hk.tfvars -auto

  deploy-prod:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to PROD
        run: |
          cd terraform
          terraform init
          terraform apply -var-file=environments/prod.tfvars -auto
```

## Troubleshooting

### Erro: "Service Principal não tem permissão"

```bash
# Adicionar permissão Contributor
az role assignment create --assignee $ARM_CLIENT_ID --role Contributor --scope /subscriptions/$ARM_SUBSCRIPTION_ID
```

### Erro: "Storage account name already exists"

```bash
# Alterar prefixo no variables.tf
storage_account_prefix = "stcasesantander2"
```

### Erro: "Databricks workspace already exists"

```bash
# Importar workspace existente
terraform import azurerm_databricks_workspace.this /subscriptions/SUB_ID/resourceGroups/RG_NAME/providers/Microsoft.Databricks/workspaces/WORKSPACE_NAME
```

## Recursos Criados

### Resource Group
- `rg-case-santander-hk` (HK)
- `rg-case-santander-prod` (PROD)

### Storage Account
- `stcasesantanderhk` (HK)
- `stcasesantander` (PROD)
- Containers: bronze, silver, gold, checkpoints

### Key Vault
- `kv-case-santander-hk` (HK)
- `kv-case-santander` (PROD)

### Databricks Workspace
- `databricks-santander-hk` (HK)
- `databricks-santander` (PROD)

### Event Hub
- `evhcasesantander-hk` (HK)
- `evhcasesantander` (PROD)
- Event Hub: `transacoes-financeiras`

### Unity Catalog
- `case_santander` (catalog)
- Schemas: bronze, silver, gold

## Links Úteis

- Terraform Docs: https://www.terraform.io/docs
- Azure Provider: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs
- Databricks Provider: https://registry.terraform.io/providers/databricks/databricks/latest/docs
