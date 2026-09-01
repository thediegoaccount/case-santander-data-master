output "namespace_name" {
  description = "Event Hub namespace name"
  value       = azurerm_eventhub_namespace.this.name
}

output "event_hub_name" {
  description = "Event Hub name"
  value       = azurerm_eventhub.this.name
}

output "connection_string" {
  description = "Event Hub connection string"
  value       = azurerm_eventhub_authorization_rule.this.primary_connection_string
  sensitive   = true
}
