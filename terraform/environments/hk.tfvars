# Environment: HK (Homologation)

resource_group_name = "rg-case-santander-hk"
location            = "eastus2"
environment         = "hk"

tags = {
  Project     = "case-santander-data"
  Environment = "homologation"
  ManagedBy   = "terraform"
}

databricks_sku = "premium"
databricks_managed_rg = "rg-databricks-hk-managed"

event_hub_namespace = "evhcasesantander-hk"
event_hub_name      = "transacoes-financeiras"

unity_catalog_name = "case_santander"
