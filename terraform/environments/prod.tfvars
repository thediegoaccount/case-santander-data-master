# Environment: PROD (Production)

resource_group_name = "rg-case-santander-prod"
location            = "eastus2"
environment         = "prod"

tags = {
  Project     = "case-santander-data"
  Environment = "production"
  ManagedBy   = "terraform"
}

databricks_sku             = "premium"
databricks_managed_rg_name = "rg-databricks-prod-managed"

event_hub_namespace_prefix = "evhcasesantander"
event_hub_prefix           = "transacoes-financeiras"

unity_catalog_name = "case_santander"

# Preencher (ou passar por -var / TF_VAR_*):
#   databricks_account_id  - account-level, exigido pelo metastore
#   databricks_host        - output databricks_workspace_host da etapa 1
#   client_id / client_secret / kaggle_username / kaggle_key / salt
# Ver terraform.tfvars.example. Nao coloque secrets neste arquivo: ele e versionado.
