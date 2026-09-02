# Event Hub Module

resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.namespace_prefix}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Standard"
  capacity            = 1

  tags = var.tags
}

resource "azurerm_eventhub" "this" {
  name                = "${var.event_hub_prefix}-${var.environment}"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = var.resource_group_name
  message_retention   = 7
  partition_count     = 2

  # ORIGEM DA CADEIA DE STREAMING.
  # Sem Capture, ninguem escrevia em bronze/kafka/: os produtores mandam
  # para o Event Hub e os jobs leem do ADLS com Auto Loader. O elo nao
  # existia -- toda a cadeia streaming -> silver -> 4 tabelas gold nunca
  # recebia um registro.
  #
  # Capture grava Avro (unico formato suportado), com envelope
  # {SequenceNumber, Offset, EnqueuedTimeUtc, SystemProperties,
  # Properties, Body}. O payload original fica em Body, decodificado em
  # jobs/job_streaming*.py.
  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 60
    size_limit_in_bytes = 10485760
    skip_empty_archives = true

    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "kafka/{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = var.capture_container
      storage_account_id  = var.storage_account_id
    }
  }
}

resource "azurerm_eventhub_authorization_rule" "this" {
  name                = "pipeline-access"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.this.name
  resource_group_name = var.resource_group_name
  listen              = true
  send                = true
  manage              = true
}
