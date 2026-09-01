# Streaming Contínuo - Implementação

## Status: IMPLEMENTADO

Streaming contínuo foi implementado e integrado ao pipeline com suporte a ambientes HK/PROD.

## Situação Atual

**Trigger Mode:** `trigger(processingTime='1 minute')`

**Comportamento:**
- Processa dados a cada 1 minuto
- Execução contínua 24/7
- Job roda como serviço independente
- Fica aguardando atualizações

## Jobs Criados

### 1. Job de Streaming Contínuo

**Arquivo:** `jobs/job_streaming_continuous.py`

**Características:**
- Fica aguardando atualizações 24/7
- Não depende de trigger agendado
- Executa como serviço contínuo
- Não limpa checkpoint (mantém estado)
- Não limpa destino (mantém histórico)
- Trigger: `processingTime='1 minute'`

**Configuração principal:**
```python
query = df_processado.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .queryName("streaming_continuous_query") \
    .trigger(processingTime='1 minute') \
    .start(silver_streaming_path)

query.awaitTermination()  # Fica aguardando 24/7
```

### 2. Job de Streaming to Gold (Agendado)

**Arquivo:** `jobs/job_streaming_to_gold_continuous.py`

**Características:**
- Executa a cada 5 minutos (agendado separado)
- Processa apenas dados novos desde última execução
- Não depende do batch daily
- Usa CDC (Change Data Feed)
- Schedule: `*/5 * * * ?` (a cada 5 minutos)

**Configuração no Databricks:**
```yaml
streaming_to_gold_continuous:
  schedule:
    quartz_cron_expression: "0 */5 * * ?"
    timezone_id: "America/Sao_Paulo"
```

## Databricks.yml Atualizado

**Jobs adicionados:**

```yaml
# STREAMING CONTÍNUO (Serviço 24/7)
streaming_continuous:
  name: "[${var.environment}] Streaming Contínuo"
  description: "Serviço de streaming 24/7 - Aguarda atualizações"
  tags:
    layer: streaming
    type: continuous-service

  tasks:
    - task_key: streaming_continuous
      python_wheel_task:
        entry_point: jobs.job_streaming_continuous
      timeout_seconds: 0  # Sem timeout (serviço contínuo)
      max_retries: 0  # Sem retries (serviço contínuo)

  # Sem schedule - roda como serviço contínuo

# STREAMING TO GOLD (Agendado)
streaming_to_gold_continuous:
  name: "[${var.environment}] Streaming to Gold (Agendado)"
  description: "Processa dados de streaming para Gold a cada 5 minutos"
  tags:
    layer: gold
    source: streaming

  tasks:
    - task_key: streaming_to_gold
      python_wheel_task:
        entry_point: jobs.job_streaming_to_gold_continuous

  schedule:
    quartz_cron_expression: "0 */5 * * ?"
    timezone_id: "America/Sao_Paulo"
```

**Workflow pai atualizado:**
- Removido `t5_streaming` (agora é serviço contínuo)
- Removido `t10_streaming_gold` (agora é agendado separado)
- Removido `t_sql_streaming` (dependia do anterior)
- Pipeline diário agora apenas batch (sem streaming)

## Comportamento por Ambiente

### HK (Homologação)
```yaml
enable_streaming: false
```
- Serviço `streaming_continuous` não roda
- Agendamento `streaming_to_gold_continuous` não roda
- Economia de custo

### PROD (Produção)
```yaml
enable_streaming: true
```
- Serviço `streaming_continuous` roda 24/7
- Agendamento `streaming_to_gold_continuous` roda a cada 5 min
- Detecção de fraudes em tempo real

## Arquitetura Final

```
┌─────────────────────────────────────────────────────────────┐
│ BATCH LAYER (Pipeline Diário)                              │
│                                                             │
│  Fontes Externas (Yahoo, BCB, WB, Kaggle)                 │
│  ↓                                                         │
│  Bronze → Silver → Gold (Batch)                           │
│  ↓                                                         │
│  Execução: 06:00 diariamente                              │
│  Agendado: Airflow / Databricks Workflow                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ SPEED LAYER (Streaming Contínuo)                           │
│                                                             │
│  Event Hub (tempo real)                                    │
│  ↓                                                         │
│  Bronze → Silver → Gold (Streaming)                       │
│  ↓                                                         │
│  Execução: 24/7 (serviço contínuo)                        │
│  Trigger: processingTime='1 minute'                       │
│  Job: streaming_continuous (independente)                │
└──────────────────┬──────────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        ▼                       ▼
┌──────────────────┐  ┌──────────────────┐
│ Análises Atuais  │  │ Análises        │
│ (Tempo Real)     │  │ Históricas      │
│  fraudes,        │  │  performance,   │
│  anomalias,      │  indicadores      │
│  volume, ranking │                  │
└──────────────────┘  └──────────────────┘
```

## Resumo

**Implementação concluída:**
- Jobs de streaming contínuo criados
- Databricks.yml atualizado
- Workflow pai ajustado (removido streaming do batch)
- Airflow DAG sincronizado
- Suporte a ambientes HK/PROD

**Próximo:** Deploy e testes em Databricks
