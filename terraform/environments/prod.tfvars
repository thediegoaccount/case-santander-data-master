# Environment: PROD (Production)

resource_group_name = "rg-case-santander-prod"
location            = "eastus2"
environment         = "prod"

tags = {
  Project     = "case-santander-data"
  Environment = "production"
  ManagedBy   = "terraform"
}

databricks_sku = "premium"
databricks_managed_rg = "rg-databricks-prod-managed"

event_hub_namespace = "evhcasesantander"
event_hub_name      = "transacoes-financeiras"

unity_catalog_name = "case_santander"
