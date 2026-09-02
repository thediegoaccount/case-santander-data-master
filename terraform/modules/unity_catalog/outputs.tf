output "catalog_name" {
  description = "Unity Catalog name"
  value       = databricks_catalog.this.name
}

output "metastore_id" {
  description = "Metastore ID"
  value       = local.metastore_id
}

output "schemas" {
  description = "Schemas criados no catalog"
  value       = [for s in databricks_schema.layers : s.name]
}
