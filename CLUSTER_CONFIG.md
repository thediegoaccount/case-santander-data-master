# 🔧 Configurar Job Cluster - Guia Prático

**Passo a passo para substituir always-on por job clusters**

---

## 1️⃣ Entender Configurações

### Tamanho de Cluster (node_type_id)

```yaml
# SMALL (exploração/testes)
node_type_id: "i3.xlarge"        # 1 vCPU, 30 GB RAM
num_workers: 1                    # Total: 1 node = ~$0.10/h
# Uso: testes, debugging

# MEDIUM (análises normais)
node_type_id: "i3.xlarge"
num_workers: 4                    # Total: 5 nodes = $0.50/h
# Uso: gold, silver transformações

# LARGE (processamento pesado)
node_type_id: "i3.2xlarge"
num_workers: 8                    # Total: 9 nodes = $1.00/h
# Uso: big data, machine learning

# CHEAP (on-demand spots)
aws_attributes:
  availability: "SPOT"            # 70% desconto vs on-demand
# Risco: pode ser interrompido (OK para jobs idempotentes)
```

---

## 2️⃣ Configuração Mínima

### Template Básico

```yaml
resources:
  jobs:
    job_gold_anomalias:
      name: "Gold Anomalias"
      
      tasks:
        - task_key: anomalias
          python_wheel_task:
            package_name: case_santander
            entry_point: jobs.job_gold_anomalias
          
          # ← JOB CLUSTER (NOVO)
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 4
          
          timeout_seconds: 3600
          max_retries: 2
```

**Pronto! Isso é suficiente para começar.**

---

## 3️⃣ Configuração Completa (Recomendada)

### Com Otimizações

```yaml
resources:
  jobs:
    # ═══════════════════════════════════════════════════════
    # GOLD ANALYSIS JOBS (executam análises complexas)
    # ═══════════════════════════════════════════════════════
    
    t3_gold_anomalias:
      name: "[${var.environment}] Gold Anomalias"
      description: "Detecta anomalias em movimentação de ações"
      
      tasks:
        - task_key: anomalias
          python_wheel_task:
            package_name: case_santander
            entry_point: jobs.job_gold_anomalias
          
          # JOB CLUSTER: Médio (análise normal)
          new_cluster:
            # Versão Spark
            spark_version: "14.3.x-scala2.12"  # Versão estável
            
            # Tipo de node
            node_type_id: "i3.xlarge"          # 30GB RAM, fast CPU
            num_workers: 4                      # 5 nodes total
            
            # AWS Config (se Databricks no AWS)
            aws_attributes:
              availability: "SPOT"              # 70% desconto
              zone_id: "us-west-2a"             # Zona preferida
            
            # Auto-terminate (economiza $)
            idle_timeout_minutes: 15            # Kill se inativo 15 min
            
            # Spark Config
            spark_conf:
              "spark.databricks.delta.schema.autoMerge.enabled": "true"
              "spark.sql.shuffle.partitions": "200"
          
          # Job config
          timeout_seconds: 3600                 # 1 hora timeout
          max_retries: 2                        # Retry 2× se falhar
          retry_on_timeout: false               # Não retry em timeout

      schedule:
        quartz_cron_expression: "0 10 * * ?"   # 10:00 toda manhã
        timezone_id: "America/Sao_Paulo"
        pause_status: UNPAUSED

    # ═══════════════════════════════════════════════════════
    # SQL LOAD JOBS (executam cargas rápidas)
    # ═══════════════════════════════════════════════════════
    
    t_carga_sql_acoes:
      name: "[${var.environment}] Carga SQL - Ações"
      description: "Carrega 2 tabelas no SQL Database"
      
      tasks:
        - task_key: sql_acoes
          python_wheel_task:
            package_name: case_santander
            entry_point: jobs.job_carga_sql_acoes
          
          # JOB CLUSTER: Pequeno (cargas são rápidas)
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2                      # Apenas 3 nodes
            
            aws_attributes:
              availability: "SPOT"              # Super barato
              zone_id: "us-west-2a"
            
            idle_timeout_minutes: 10            # Kill mais rápido
            
            spark_conf:
              "spark.sql.shuffle.partitions": "100"  # Menos partições
          
          timeout_seconds: 1800                 # 30 min (já é rápido)
          max_retries: 2

      schedule:
        quartz_cron_expression: "0 15 * * ?"   # 15:00
        timezone_id: "America/Sao_Paulo"
        pause_status: UNPAUSED

    # ═══════════════════════════════════════════════════════
    # HEAVY JOBS (processamento pesado)
    # ═══════════════════════════════════════════════════════
    
    t0_unity_catalog:
      name: "[${var.environment}] Setup - Unity Catalog"
      
      tasks:
        - task_key: catalog
          python_wheel_task:
            package_name: case_santander
            entry_point: jobs.job_unity_catalog
          
          # JOB CLUSTER: Grande (setup pesado)
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.2xlarge"         # Mais potência
            num_workers: 6                      # 7 nodes total
            
            aws_attributes:
              availability: "SPOT"
              zone_id: "us-west-2a"
            
            idle_timeout_minutes: 20
            
            spark_conf:
              "spark.sql.shuffle.partitions": "300"
          
          timeout_seconds: 7200                 # 2 horas
          max_retries: 1

      schedule:
        quartz_cron_expression: "0 6 * * ?"    # 6:00 (primeiro)
        timezone_id: "America/Sao_Paulo"
        pause_status: UNPAUSED
```

---

## 4️⃣ Variação por Ambiente

### Prod vs Dev (diferentes tamanhos)

```yaml
bundle:
  name: case-santander-data
  environments:
    # DESENVOLVIMENTO: Clusters menores (economizar)
    dev:
      resources:
        jobs:
          t3_gold_anomalias:
            tasks:
              - task_key: anomalias
                new_cluster:
                  node_type_id: "i3.xlarge"
                  num_workers: 2              # MENOR (dev)
          
          t_carga_sql_acoes:
            tasks:
              - task_key: sql_acoes
                new_cluster:
                  node_type_id: "i3.xlarge"
                  num_workers: 1              # MÍNIMO (dev)

    # PRODUÇÃO: Clusters maiores (confiabilidade)
    prod:
      resources:
        jobs:
          t3_gold_anomalias:
            tasks:
              - task_key: anomalias
                new_cluster:
                  node_type_id: "i3.xlarge"
                  num_workers: 4              # MAIOR (prod)
          
          t_carga_sql_acoes:
            tasks:
              - task_key: sql_acoes
                new_cluster:
                  node_type_id: "i3.xlarge"
                  num_workers: 2              # NORMAL (prod)
```

---

## 5️⃣ Opções Avançadas

### Para Casos Especiais

```yaml
# ╔══════════════════════════════════════════════════════════╗
# ║ OPÇÃO 1: Auto-scaling (escalona conforme carga)        ║
# ╚══════════════════════════════════════════════════════════╝

new_cluster:
  spark_version: "14.3.x-scala2.12"
  node_type_id: "i3.xlarge"
  num_workers: 2                          # Mínimo
  
  # Auto-scaling: 2-8 workers
  autoscale:
    min_workers: 2
    max_workers: 8                        # Sobe até 8 se necessário

# ╔══════════════════════════════════════════════════════════╗
# ║ OPÇÃO 2: Drivers maiores (para jobs memory-intensive)  ║
# ╚══════════════════════════════════════════════════════════╝

new_cluster:
  spark_version: "14.3.x-scala2.12"
  driver_node_type_id: "i3.2xlarge"      # Driver bigger
  node_type_id: "i3.xlarge"
  num_workers: 4
  
  spark_conf:
    "spark.driver.memory": "16g"          # Driver tem mais RAM
    "spark.executor.memory": "8g"

# ╔══════════════════════════════════════════════════════════╗
# ║ OPÇÃO 3: On-Demand (confiabilidade máxima)             ║
# ╚══════════════════════════════════════════════════════════╝

new_cluster:
  spark_version: "14.3.x-scala2.12"
  node_type_id: "i3.xlarge"
  num_workers: 4
  
  aws_attributes:
    availability: "ON_DEMAND"             # Nunca desligado
    # Mais caro: $0.50/h vs $0.15/h (SPOT)
    # Mas garantido rodar

# ╔══════════════════════════════════════════════════════════╗
# ║ OPÇÃO 4: Streaming (jobs contínuos)                    ║
# ╚══════════════════════════════════════════════════════════╝

new_cluster:
  spark_version: "14.3.x-scala2.12"
  node_type_id: "i3.xlarge"
  num_workers: 4
  
  # Não mata cluster mesmo sem uso
  idle_timeout_minutes: 0                 # Never auto-terminate
  
  # Job roda continuamente
  # Perfeito para Kafka streaming
```

---

## 6️⃣ Comparar com Always-On

### Antes (Always-On)

```yaml
# OLD: Cluster sempre ligado
jobs:
  t3_gold_anomalias:
    tasks:
      - existing_cluster_id: "0401-150803-wefgy1hc"
        # Usa cluster que já existe e fica 24/7
        # Custo: $2,500/mês mesmo sem usar
```

### Depois (Job Cluster)

```yaml
# NEW: Cluster por job
jobs:
  t3_gold_anomalias:
    tasks:
      - new_cluster:
          spark_version: "14.3.x-scala2.12"
          node_type_id: "i3.xlarge"
          num_workers: 4
        # Cria quando executa, deleta após terminar
        # Custo: ~$0.50 por execução = $15/mês
```

---

## 7️⃣ Deploy e Test

### Passo 1: Atualizar databricks.yml

```bash
# Editar arquivo
vim databricks.yml

# Substituir existing_cluster_id por new_cluster
# Em cada job
```

### Passo 2: Validar Sintaxe

```bash
# Validar YAML
python -c "
import yaml
with open('databricks.yml') as f:
    yaml.safe_load(f)
print('✅ Valid')
"
```

### Passo 3: Deploy DEV

```bash
# Fazer deploy em dev primeiro
export DATABRICKS_HOST_DEV=https://...
export DATABRICKS_TOKEN_DEV=dapi...

databricks bundle deploy --target dev
```

### Passo 4: Verificar Cluster Criado

```bash
# Listar clusters criados
databricks clusters list

# Você verá novos clusters:
# - job-<jobid>-run-<runid>-worker
# - job-<jobid>-run-<runid>-driver
```

### Passo 5: Executar Job Test

```bash
# Rodar um job para testar
databricks jobs run-now --job-id 123

# Ver status
databricks runs get --run-id 456
```

### Passo 6: Monitorar Cluster

```bash
# Ver cluster criado para este job
# Databricks UI → Jobs → <job_name> → Runs → <run_id>
# Click em "Cluster" para ver detalhes
```

---

## 8️⃣ Troubleshooting

### Problema: "Cluster not found"

```bash
# Se erro de cluster inexistente, verificar:
# 1. spark_version existe?
databricks clusters spark-versions

# 2. node_type_id válido?
databricks clusters list-node-types

# 3. AWS region correto?
# Checar zona_id em aws_attributes
```

### Problema: "Job takes too long"

```yaml
# Soluções:
# 1. Aumentar num_workers
num_workers: 8  # de 4 para 8

# 2. Aumentar node_type
node_type_id: "i3.2xlarge"  # de i3.xlarge

# 3. Ativar auto-scaling
autoscale:
  min_workers: 2
  max_workers: 8

# 4. Otimizar Spark config
spark_conf:
  "spark.sql.shuffle.partitions": "300"  # Mais partições
```

### Problema: "Cluster initialization failed"

```bash
# Verificar logs:
# Databricks UI → Clusters → <cluster_id> → Logs

# Causas comuns:
# 1. spark_version incompatível com node_type
# 2. Permissions não suficientes
# 3. Quota de clusters atingida

# Solução: Use versão stable
spark_version: "14.3.x-scala2.12"
```

---

## 9️⃣ Dicas de Otimização

### Economizar Mais

```yaml
# 1. Usar SPOT instances
aws_attributes:
  availability: "SPOT"  # 70% desconto

# 2. Auto-terminate mais agressivo
idle_timeout_minutes: 5  # Kill após 5 min

# 3. Menos workers em SQL jobs
num_workers: 1  # Só 2 nodes total

# 4. Usar autoscale ao invés de fixed
autoscale:
  min_workers: 1  # Mínimo muito baixo
  max_workers: 4  # Máximo contido
```

### Ganho: -60% no custo vs always-on

---

## 🔟 Seu Projeto (Santander)

### Recomendação Específica

```yaml
# databricks.yml

resources:
  jobs:
    # GOLD JOBS: 4 workers (análises)
    t3_gold_anomalias:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 4
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 15

    t3_gold_performance:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 4
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 15

    t3_gold_bcb:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 4
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 15

    # SQL LOAD JOBS: 2 workers (cargas rápidas)
    t_carga_sql_acoes:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2              # Menos workers
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 10    # Termina mais rápido

    t_carga_sql_clientes:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 10

    t_carga_sql_fraude:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 10

    t_carga_sql_streaming:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 10

    t_carga_sql_macro:
      tasks:
        - new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            aws_attributes:
              availability: "SPOT"
            idle_timeout_minutes: 10
```

---

## ✅ Checklist

- [ ] Editar databricks.yml
- [ ] Substituir existing_cluster_id → new_cluster
- [ ] Validar YAML syntax
- [ ] Deploy em DEV
- [ ] Testar um job
- [ ] Verificar cluster criado
- [ ] Monitorar custo
- [ ] Deploy em PROD
- [ ] Economizar $2,050/mês! 🎉

---

**Próximo passo:** Editar seu databricks.yml e fazer o primeiro deploy!
