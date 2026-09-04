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
├── bootstrap/              # Cria o backend do state (rodar 1x)
└── modules/
    ├── resource_group/    # Resource Group
    ├── storage_account/   # ADLS Gen2
    ├── key_vault/         # Key Vault
    ├── databricks_workspace/  # Databricks
    ├── event_hub/         # Event Hub
    ├── unity_catalog/     # Metastore, catalog, schemas
    ├── secret_scope/      # Secret scope Key Vault-backed
    └── secrets/           # Secrets management
```

## Convenção de nomes

Os nomes são derivados de `environment`. `src/config/environment.py` segue a
**mesma** convenção, então os dois lados não podem divergir:

| Recurso      | Padrão                         | hk                          |
|--------------|--------------------------------|-----------------------------|
| Storage      | `stcasesantander<env>`         | `stcasesantanderhk`         |
| Key Vault    | `kv-case-santander-<env>`      | `kv-case-santander-hk`      |
| Event Hub NS | `evhcasesantander-<env>`       | `evhcasesantander-hk`       |
| Event Hub    | `transacoes-financeiras-<env>` | `transacoes-financeiras-hk` |
| Schemas      | `<env>_bronze/silver/gold`     | `hk_bronze`, ...            |

Storage account não aceita hífen (só minúsculas e dígitos), por isso é o único
sem separador. Para conferir depois do apply:

```bash
terraform output resource_names
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

3. **Databricks Account ID** (console em accounts.azuredatabricks.net):
necessário porque o metastore do Unity Catalog é recurso *account-level*.

4. **Service Principal criado:**
```bash
az ad sp create-for-rbac --name "terraform-santander" --role="Contributor" --scopes="/subscriptions/YOUR_SUBSCRIPTION_ID"
```

5. **Variáveis de ambiente:**
```bash
export ARM_CLIENT_ID="YOUR_CLIENT_ID"
export ARM_CLIENT_SECRET="YOUR_CLIENT_SECRET"
export ARM_SUBSCRIPTION_ID="YOUR_SUBSCRIPTION_ID"
export ARM_TENANT_ID="YOUR_TENANT_ID"
```

## Deploy

O deploy tem **duas etapas**. O provider `databricks` precisa de `host`, que só
existe depois do workspace criado; Terraform avalia configuração de provider no
plan, e um valor desconhecido nessa posição aborta. Então a etapa 1 cria o Azure
e a etapa 2 cria o que vive dentro do Databricks.

O script `scripts/deploy_infra.sh` encadeia tudo:

```bash
./scripts/deploy_infra.sh bootstrap   # só na primeira vez (backend do state)
./scripts/deploy_infra.sh all -var-file=terraform/environments/hk.tfvars
```

### Manualmente

**0. Backend do state** (uma vez por subscription):

```bash
cd terraform/bootstrap && terraform init && terraform apply
```

**1. Infraestrutura Azure:**

```bash
cd terraform
# O -backend-config e OBRIGATORIO: o `key` do bloco backend em main.tf
# e fixo, entao um `terraform init` puro faz hk e prod compartilharem o
# mesmo state -- aplicar um planeja destruir o outro. Use a chave do
# ambiente alvo (case-santander-data-prod.tfstate para prod).
terraform init -backend-config="key=case-santander-data-hk.tfstate"
terraform validate
terraform apply -var-file=environments/hk.tfvars \
  -target=module.resource_group \
  -target=module.storage_account \
  -target=module.key_vault \
  -target=module.databricks_workspace \
  -target=module.event_hub \
  -target=module.secrets
```

**2. Recursos Databricks** (secret scope, Unity Catalog):

```bash
terraform apply -var-file=environments/hk.tfvars \
  -var="databricks_host=$(terraform output -raw databricks_workspace_host)"
```

Para PROD, troque `hk.tfvars` por `prod.tfvars`.

### Secret scope: limitação do Azure

O scope Key Vault-backed **só pode ser criado com credencial de usuário AAD**,
não de service principal. Se o apply roda como SP, use
`create_secret_scope = false` e crie o scope uma vez à mão:

```bash
databricks secrets create-scope --scope $(terraform output -raw key_vault_name) \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id $(terraform output -raw key_vault_id) \
  --dns-name $(terraform output -raw key_vault_uri)
```

O nome do scope precisa ser igual ao do Key Vault: é o que
`src/config/secrets.py` usa em `dbutils.secrets.get(scope=...)`.

### Unity Catalog: um metastore por região

O Azure permite **um metastore por região por account**. Se já existir um em
`eastus2`, passe `existing_metastore_id = "..."` e nenhum novo é criado — só o
assignment ao workspace.

## Destruição

`terraform destroy` sozinho **não** funciona aqui: o provider `databricks`
exige `databricks_host`, que é uma variável sem default, e o backend usa uma
`key` por ambiente (ver Deploy). Use o script, que resolve as duas coisas
automaticamente a partir do próprio state:

```bash
./scripts/destroy_infra.sh all
ENVIRONMENT=prod ./scripts/destroy_infra.sh all -var-file=terraform/environments/prod.tfvars

# sem confirmação interativa (CI):
./scripts/destroy_infra.sh all -auto-approve
```

Um único `terraform destroy` cobre tudo — Terraform computa a ordem reversa
de dependência a partir do state, não precisa das duas etapas do apply. Isso
inclui o Unity Catalog (`force_destroy = true` no catalog e nos schemas,
então funciona mesmo que o pipeline já tenha gravado tabelas) e o Key Vault
(o provider `azurerm` purga de verdade por padrão — `features {}` vazio já
ativa `purge_soft_delete_on_destroy`, então recriar com o mesmo nome depois
não esbarra em "vault soft-deleted").

**Metastore compartilhado entre ambientes**: o Azure permite um metastore
Unity Catalog por região/account. Se um segundo ambiente aponta para o
metastore deste via `existing_metastore_id`, destruir este ambiente destrói
o metastore de que o outro depende — são estados diferentes, Terraform não
enxerga essa dependência. Nesse caso, destrua primeiro quem usa
`existing_metastore_id`, por último quem criou o metastore.

O backend do state (`terraform/bootstrap/`) não é afetado por nada acima —
ele existe para sobreviver a vários ciclos de subir/derrubar. Só destrua com
`./scripts/destroy_infra.sh bootstrap` se quiser apagar o histórico de todos
os ambientes.

## Outputs

Após o deploy, os outputs estão disponíveis:

```bash
terraform output
```

**Outputs principais:**
- `resource_group_name`
- `storage_account_name`
- `storage_account_dfs_endpoint`
- `key_vault_name`
- `key_vault_uri`
- `databricks_workspace_url` (hostname) / `databricks_workspace_host` (com `https://`)
- `databricks_workspace_id` (ID numérico do control plane, não o ID ARM)
- `databricks_secret_scope`
- `event_hub_namespace_name`
- `event_hub_name`
- `event_hub_connection_string`
- `unity_catalog_name` / `unity_catalog_schemas`
- `resource_names` (confira contra `src/config/environment.py`)

## Integração com Databricks Asset Bundles

Após o deploy Terraform, atualize suas variáveis de ambiente:

O CLI do Databricks exige o host **com** `https://`, por isso
`databricks_workspace_host` e não `databricks_workspace_url`:

```bash
# Para HK
export DATABRICKS_HOST_HK=$(terraform output -raw databricks_workspace_host)

# Para PROD
export DATABRICKS_HOST_PROD=$(terraform output -raw databricks_workspace_host)
```

`STORAGE_ACCOUNT` / `KEY_VAULT` não precisam ser exportados: `environment.py`
deriva os nomes pela convenção acima. As variáveis `STORAGE_ACCOUNT`,
`KEY_VAULT_NAME`, `EVENTHUB_NAMESPACE` e `EVENTHUB_NAME` existem apenas como
override, caso os defaults do Terraform tenham sido alterados no tfvars.

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
          terraform apply -var-file=environments/hk.tfvars -auto-approve

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
          terraform apply -var-file=environments/prod.tfvars -auto-approve
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
- `stcasesantanderhk` (HK) / `stcasesantanderprod` (PROD)
- ADLS Gen2 (`is_hns_enabled = true`)
- Containers: bronze, silver, gold, checkpoints, catalog

Os subdiretórios (`kafka/`, `streaming/`, `checkpoints/...`) **não** são criados
pelo Terraform: o Spark os cria no primeiro write. Em object storage o prefixo
passa a existir quando o primeiro objeto é gravado.

### Key Vault
- `kv-case-santander-hk` (HK) / `kv-case-santander-prod` (PROD)
- Access policy para o operador e para o SP `AzureDatabricks` (leitura)

### Databricks Workspace
- `databricks-santanderhk` (HK) / `databricks-santanderprod` (PROD)

### Event Hub
- `evhcasesantander-hk` (HK) / `evhcasesantander-prod` (PROD)
- Event Hub: `transacoes-financeiras-<env>`
- Authorization rule: `pipeline-access`

Namespace de Event Hub é DNS **global**: se der conflito com outra conta Azure,
mude `event_hub_namespace_prefix` no tfvars.

### Unity Catalog
- `case_santander` (catalog)
- Schemas: `hk_bronze`, `hk_silver`, `hk_gold` (ou `prod_*`)

## Links Úteis

- Terraform Docs: https://www.terraform.io/docs
- Azure Provider: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs
- Databricks Provider: https://registry.terraform.io/providers/databricks/databricks/latest/docs
