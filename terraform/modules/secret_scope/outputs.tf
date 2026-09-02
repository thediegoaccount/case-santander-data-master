output "scope_name" {
  description = "Nome do secret scope criado"
  value       = databricks_secret_scope.this.name
}
