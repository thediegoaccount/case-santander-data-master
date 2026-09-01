# Unity Catalog Module

provider "databricks" {
  host = var.databricks_host
}

resource "databricks_metastore" "this" {
  name         = var.catalog_name
  storage_root = "abfss://catalog@${var.storage_account}.dfs.core.windows.net/"
  force_destroy = true
}

resource "databricks_catalog" "this" {
  name = var.catalog_name
  metastore_id = databricks_metastore.this.id
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.this.name
  name         = "bronze"
  comment      = "Bronze layer - raw data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.this.name
  name         = "silver"
  comment      = "Silver layer - processed data"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.this.name
  name         = "gold"
  comment      = "Gold layer - analytical data"
}
