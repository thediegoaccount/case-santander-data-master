# Unity Catalog Module
#
# Metastore e um recurso ACCOUNT-level: exige o provider apontando para
# accounts.azuredatabricks.net. Catalog/schema sao WORKSPACE-level e so
# funcionam depois do metastore estar atribuido ao workspace.

terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.account, databricks.workspace]
    }
  }
}

# Azure permite apenas UM metastore por regiao por account.
# Se ja existir um, passe existing_metastore_id e nenhum e criado.
resource "databricks_metastore" "this" {
  count    = var.existing_metastore_id == null ? 1 : 0
  provider = databricks.account

  name          = var.catalog_name
  storage_root  = "abfss://catalog@${var.storage_account}.dfs.core.windows.net/"
  region        = var.location
  force_destroy = true
}

locals {
  metastore_id = var.existing_metastore_id != null ? var.existing_metastore_id : databricks_metastore.this[0].id
}

# Credencial de acesso do metastore ao storage_root. Sem isto o metastore
# existe mas nao consegue ler/escrever em abfss://catalog@..., e todo
# saveAsTable (tabela gerenciada) falha.
resource "databricks_metastore_data_access" "this" {
  provider = databricks.account

  metastore_id = local.metastore_id
  name         = "dac-case-santander-${var.environment}"
  is_default   = true

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_metastore_assignment" "this" {
  provider = databricks.account

  metastore_id = local.metastore_id
  workspace_id = var.databricks_workspace_id
}

resource "databricks_catalog" "this" {
  provider   = databricks.workspace
  depends_on = [databricks_metastore_assignment.this, databricks_metastore_data_access.this]

  name         = var.catalog_name
  metastore_id = local.metastore_id
}

# Schemas por ambiente: hk_bronze / prod_bronze, etc.
# Mesmo catalog, isolamento por schema (ver src/config/environment.py).
resource "databricks_schema" "layers" {
  provider = databricks.workspace
  for_each = toset(["bronze", "silver", "gold"])

  catalog_name = databricks_catalog.this.name
  name         = "${var.schema_prefix}${each.key}"
  comment      = "${title(each.key)} layer - ${var.environment}"
}
