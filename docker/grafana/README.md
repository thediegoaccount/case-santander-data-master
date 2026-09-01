# Grafana - Airflow Monitoring

## Configuração

Grafana foi configurado automaticamente para monitorar o Airflow via PostgreSQL.

## Acesso

**URL:** http://localhost:3000

**Login:**
- User: admin
- Password: admin

## Data Source

**Nome:** Airflow Postgres
**Tipo:** PostgreSQL
**Host:** postgres:5432
**Database:** airflow
**User:** airflow
**Password:** airflow

## Dashboards

### 1. Airflow Pipeline Monitoring

**Panels:**
1. **DAG Status (Last 24h)** - Contagem de DAGs com sucesso
2. **Failed DAGs (Last 24h)** - Contagem de DAGs com falha
3. **DAG Duration (Minutes)** - Duração média das DAGs
4. **Task Failures (Last 24h)** - Tasks com mais falhas

### 2. Databricks Jobs Performance

**Panels:**
1. **Databricks Jobs - Avg Duration (Minutes)** - Duração média dos jobs Databricks
2. **Databricks Jobs - Total Runs (Last 24h)** - Total de execuções
3. **Databricks Jobs - Success Rate (%)** - Taxa de sucesso
4. **Databricks Jobs - Failures by Task** - Tasks com mais falhas
5. **Databricks Jobs - Max Duration (Minutes)** - Duração máxima

### 3. Santander Streaming Monitor

**Panels:**
1. **Streaming Jobs - Running Status** - Jobs rodando agora
2. **Streaming Jobs - Failures (Last 24h)** - Falhas de streaming
3. **Streaming Jobs - Success Rate (%)** - Taxa de sucesso
4. **Streaming Jobs - Latency (Minutes)** - Latência dos jobs
5. **Streaming Jobs - Runs per Hour** - Execuções por hora

### 4. Santander Pipeline Overview

**Panels:**
1. **Pipeline Execution Status** - Status da pipeline principal
2. **Pipeline Duration (Minutes)** - Duração ao longo do tempo
3. **Pipeline Success Rate (Last 7d)** - Taxa de sucesso (7 dias)
4. **Tasks by Layer (Last Run)** - Distribuição por camada (Setup, Ingestion, Silver, Gold, SQL)
5. **Failed Tasks by Layer (Last 7d)** - Falhas por camada
6. **Pipeline Runs per Day (Last 30d)** - Execuções por dia

### 5. Santander Data Metrics

**Panels:**
1. **Ingestion Sources - Success Rate (Last 24h)** - Taxa de sucesso da ingestão
2. **Ingestion Sources - Last Run Duration** - Duração da última ingestão
3. **Silver Transformations - Success Rate (Last 24h)** - Taxa de sucesso do Silver
4. **Gold Analytics - Success Rate (Last 24h)** - Taxa de sucesso do Gold
5. **Data Quality - Row Counts by Layer (Last Run)** - Tasks processadas por camada
6. **SQL Load - Success Rate (Last 24h)** - Taxa de sucesso da carga SQL
7. **Pipeline End-to-End Duration (Last 7 Runs)** - Duração completa end-to-end

### 6. System Health & Alerts

**Panels:**
1. **Total Failed Tasks (Last 24h)** - Total de tasks falhadas
2. **Total Failed DAGs (Last 24h)** - Total de DAGs falhadas
3. **Running Tasks (Now)** - Tasks rodando agora
4. **Queued Tasks (Now)** - Tasks na fila
5. **Failed Tasks Over Time (Last 7d)** - Falhas ao longo do tempo
6. **Most Failed DAGs (Last 7d)** - DAGs com mais falhas
7. **Longest Running Tasks (Last 24h)** - Tasks mais longas
8. **Overall Success Rate (Last 24h)** - Taxa de sucesso geral

## Queries SQL

### Status das DAGs (Last 24h)
```sql
SELECT
    dag_id,
    COUNT(*) as total_runs,
    SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as success,
    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END) as running
FROM dag_run
WHERE execution_date >= NOW() - INTERVAL '24 hours'
GROUP BY dag_id;
```

### Latência das DAGs
```sql
SELECT
    dag_id,
    AVG(EXTRACT(EPOCH FROM (end_date - start_date))) / 60 as avg_duration_minutes
FROM dag_run
WHERE execution_date >= NOW() - INTERVAL '24 hours'
GROUP BY dag_id;
```

### Tasks com Mais Falhas
```sql
SELECT
    task_id,
    COUNT(*) as failures
FROM task_instance
WHERE state = 'failed'
AND execution_date >= NOW() - INTERVAL '24 hours'
GROUP BY task_id
ORDER BY failures DESC
LIMIT 10;
```

### Jobs Databricks - Duração
```sql
SELECT
    ti.task_id,
    AVG(EXTRACT(EPOCH FROM (ti.end_date - ti.start_date))) / 60 as avg_duration_minutes
FROM task_instance ti
JOIN dag_run dr ON ti.dag_id = dr.dag_id AND ti.execution_date = dr.execution_date
WHERE ti.task_id LIKE '%databricks%'
AND ti.execution_date >= NOW() - INTERVAL '24 hours'
GROUP BY ti.task_id;
```

## Como Usar

### 1. Iniciar Grafana

```bash
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

### 2. Acessar Grafana

http://localhost:3000

### 3. Explorar Dashboard

- Go to Dashboards → Airflow Pipeline Monitoring
- Visualizar panels de monitoramento
- Ajustar time range (Last 24h, Last 7d, etc.)

### 4. Criar Novos Dashboards

- Go to Dashboards → New
- Add Query (SQL PostgreSQL)
- Choose Visualization
- Save Dashboard

### 5. Configurar Alertas

**Alertas Recomendados:**

#### Alerta 1: DAG com Falha
- **Dashboard:** System Health & Alerts
- **Panel:** Total Failed DAGs (Last 24h)
- **Condition:** `failed_dags > 0`
- **Notification:** Email, Slack, PagerDuty

#### Alerta 2: Pipeline Principal Falhou
- **Dashboard:** Santander Pipeline Overview
- **Panel:** Pipeline Execution Status
- **Condition:** `state = 'failed'`
- **Notification:** Email, Slack

#### Alerta 3: Streaming Job Down
- **Dashboard:** Santander Streaming Monitor
- **Panel:** Streaming Jobs - Running Status
- **Condition:** `running_jobs = 0` (se deveria estar rodando)
- **Notification:** Email, Slack

#### Alerta 4: Taxa de Sucesso Baixa
- **Dashboard:** System Health & Alerts
- **Panel:** Overall Success Rate (Last 24h)
- **Condition:** `success_rate < 95%`
- **Notification:** Email

**Como Configurar:**
1. Go to Dashboards → [Nome do Dashboard]
2. Click no panel → Configure Alert
3. Set condition (SQL query)
4. Add notification channel
5. Save alert

## Resumo dos Dashboards

| Dashboard | Foco | Principais Métricas |
|-----------|------|-------------------|
| **Airflow Pipeline Monitoring** | Geral do Airflow | Status DAGs, Falhas, Duração |
| **Databricks Jobs Performance** | Jobs Databricks | Duração, Success Rate, Latência |
| **Santander Streaming Monitor** | Streaming em tempo real | Status, Latência, Runs/hora |
| **Santander Pipeline Overview** | Pipeline principal | Status, Falhas por camada, Runs/dia |
| **Santander Data Metrics** | Métricas de dados | Ingestão, Silver, Gold, SQL |
| **System Health & Alerts** | Saúde do sistema | Falhas totais, Running tasks, Success rate |

## Estrutura de Diretórios

```
docker/grafana/
├── provisioning/
│   ├── datasources/
│   │   └── airflow-postgres.yml  # Data source configurado
│   └── dashboards/
│       ├── airflow-dashboard.yml  # Provider de dashboards
│       └── airflow-monitoring.json  # Dashboard automático
└── README.md
```

## Troubleshooting

### Grafana não conecta ao PostgreSQL

```bash
# Verificar se postgres está rodando
docker compose ps postgres

# Verificar logs
docker compose logs grafana
```

### Dashboard não aparece

```bash
# Reiniciar Grafana
docker compose restart grafana

# Verificar logs
docker compose logs grafana
```

### Queries retornam erro

```bash
# Verificar se banco existe
docker compose exec postgres psql -U airflow -d airflow -c "\dt"

# Verificar se há dados
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM dag_run;"
```

## Links Úteis

- Grafana Docs: https://grafana.com/docs/
- Airflow Metrics: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/metrics.html
- PostgreSQL Datasource: https://grafana.com/docs/grafana/latest/datasources/postgres/
