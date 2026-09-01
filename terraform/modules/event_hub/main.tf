# Event Hub Module

resource "azurerm_eventhub_namespace" "this" {
  name                = var.namespace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Standard"
  capacity            = 1

  tags = var.tags
}

resource "azurerm_eventhub" "this" {
  name                = var.event_hub_name
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = var.resource_group_name
  message_retention  = 7
  partition_count     = 2
}

resource "azurerm_eventhub_authorization_rule" "this" {
  name                = "RootManageSharedAccessKey"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = var.resource_group_name
  listen              = true
  send                = true
  manage              = true
}
