output "catalog_name" {
  description = "Unity Catalog name"
  value       = databricks_catalog.this.name
}

output "metastore_id" {
  description = "Metastore ID"
  value       = databricks_metastore.this.id
}
