# Sincronização Bidirecional Airflow ↔ Databricks Workflow

## Visão Geral

Implementação de sincronização completa entre Airflow DAG e Databricks Workflow, garantindo que ambos orquestradores mantenham a mesma sequência de jobs e dependências.

## Arquitetura de Sincronização

```

              SINGLE SOURCE OF TRUTH                          
                     databricks.yml                            
                                                             
    
    WORKFLOW PAI: pipeline_completo                          
    - Todas as dependências definidas                       
    - Schedule único (06:00)                                  
    - Tasks configuradas com dependencies                    
    
                                                                
                                                                
    
    JOBS INDIVIDUAIS (sem schedule)                       
    - T3_gold_anomalias, t3_gold_performance, etc.       
    - Podem ser executados manualmente se necessário      
    
                                                             

                   
        
                               
  
 Airflow DAG          Databricks       
 (Gerado Auto)       Workflow Pai      
    
                               
        
                   
         Consistência Garantida
```

## Estrutura do Workflow Pai

### Databricks Asset Bundles

**Arquivo:** `databricks.yml`

```yaml
resources:
  jobs:
    # Workflow pai com todas as dependências
    pipeline_completo:
      name: "[${var.environment}] Pipeline Completo"
      tasks:
        # Setup
        - task_key: t0_unity_catalog
          depends_on: []
        
        # Ingestion (paralelo)
        - task_key: t1_extracao_acoes
          depends_on: [t0_unity_catalog]
        - task_key: t1_extracao_bcb
          depends_on: [t0_unity_catalog]
        - task_key: t5_streaming
          depends_on: [t0_unity_catalog]
          condition: "${var.enable_streaming} == true"
        
        # Silver (por fonte)
        - task_key: t2_silver_acoes
          depends_on: [t1_extracao_acoes]
        - task_key: t2_silver_bcb
          depends_on: [t1_extracao_bcb]
        
        # Gold Market (paralelo)
        - task_key: t3_anomalias
          depends_on: [t2_silver_acoes, t2_silver_bcb, t2_silver_world_bank]
        - task_key: t3_performance
          depends_on: [t2_silver_acoes, t2_silver_bcb, t2_silver_world_bank]
        
        # Correlação (depende de ambos)
        - task_key: t3_acoes_cambio
          depends_on: [t3_performance, t3_bcb]
        
        # Gold Client (sequencial)
        - task_key: t7_corretora_analises
          depends_on: [t6_clientes_silver]
        - task_key: t9_scd
          depends_on: [t7_corretora_amalises]
        - task_key: t3_fraude
          depends_on: [t9_scd]
        
        # Carga SQL (paralelo após Gold)
        - task_key: t_sql_acoes
          depends_on: [t3_anomalias, t3_acoes_cambio]
        - task_key: t_sql_clientes
          depends_on: [t7_corretora_analises, t9_scd]
        
        # Finalização
        - task_key: t8_lakehouse_monitoring
          depends_on: [t_sql_acoes, t_sql_clientes, t_sql_fraude, ...]
        - task_key: t4_observabilidade
          depends_on: [t_sql_acoes, t_sql_clientes, t_sql_fraude, ...]
      
      schedule:
        quartz_crom_expression: "0 6 * * ?"  # 06:00 BRT
        pause_status: UNPAUSED
    
    # Jobs individuais (sem schedule - para execução manual)
    t3_gold_anomalias:
      tasks: [...]
      # SEM schedule - controlado pelo workflow pai
    
    t3_gold_performance:
      tasks: [...]
      # SEM schedule - controlado pelo workflow pai
```

### Airflow DAG (Gerado Automaticamente)

**Arquivo:** `dags/dag_pipeline_santander.py` (gerado via script)

```python
# Header
# DAG: Pipeline Corretora Santander (Sincronizado com Databricks Asset Bundles)
# Gerado automaticamente via scripts/sync_airflow_from_databricks.py

# Configurações de ambiente
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")
REPO_PATH = os.getenv("DATABRICKS_REPO_PATH")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hk")

# Tasks geradas com dependências
t0_unity_catalog = databricks_task("t0_unity_catalog", "jobs/job_unity_catalog.py")
t1_extracao_acoes = databricks_task("t1_extracao_acoes", "jobs/job_extracao_acoes.py")
t1_extracao_bcb = databricks_task("t1_extracao_bcb", "jobs/job_extracao_bcb.py")
# ... todas as tasks

# Dependências sincronizadas do databricks.yml
[t0_unity_catalog] >> [t1_extracao_acoes, t1_extracao_bcb, t1_extracao_world_bank, t5_streaming, t6_clientes_ordens]
[t1_extracao_acoes] >> t2_silver_acoes
[t1_extracao_bcb] >> t2_silver_bcb
[t2_silver_acoes, t2_silver_bcb, t2_silver_world_bank] >> [t3_anomalias, t3_performance, t3_bcb, t3_world_bank]
[t3_performance, t3_bcb] >> t3_acoes_cambio
# ... todas as dependências
```

## Como Funciona a Sincronização

### 1. Modificar Sequência

**Passo 1:** Editar `databricks.yml`

```yaml
# Adicionar nova dependência
- task_key: t3_anomalias
  depends_on:
    - task_key: t2_silver_acoes
    - task_key: t2_silver_bcb
    - task_key: t2_silver_world_bank
    - task_key: novo_job  # ← Nova dependência
```

**Passo 2:** Executar script de sincronização

```bash
python scripts/sync_airflow_from_databricks.py --generate
```

**Passo 3:** Commitar mudanças

```bash
git add databricks.yml dags/dag_pipeline_santander.py
git commit -m "Adiciona nova dependência e sincroniza Airflow DAG"
git push
```

### 2. Adicionar Novo Job

**Passo 1:** Adicionar job no `databricks.yml`

```yaml
jobs:
  novo_job_gold:
    name: "[${var.environment}] Novo Job Gold"
    tasks:
      - task_key: novo
        python_wheel_task:
          entry_point: jobs.job_novo_gold
        depends_on:
          - task_key: t3_performance
```

**Passo 2:** Adicionar no workflow pai

```yaml
pipeline_completo:
  tasks:
    # ... outras tasks
    - task_key: t3_performance
      # ...
    - task_key: novo_job_gold
      depends_on:
        - task_key: t3_performance
```

**Passo 3:** Executar script de sincronização

```bash
python scripts/sync_airflow_from_databricks.py --generate
```

### 3. Modificar Schedule

**Passo 1:** Alterar schedule do workflow pai

```yaml
pipeline_completo:
  schedule:
    quartz_cron_expression: "0 7 * * ?"  # Mudar de 06:00 para 07:00
```

**Passo 2:** Executar script de sincronização

```bash
python scripts/sync_airflow_from_databricks.py --generate
```

**Passo 3:** Atualizar schedule do Airflow DAG (manual ou via script futuro)

## Validação Automática

### CI/CD Integration

O CI/CD valida a sincronização automaticamente:

```yaml
# .github/workflows/deploy-databricks.yml
- name: Sync Airflow DAG with Databricks Asset Bundles
  run: |
    python scripts/sync_airflow_from_databricks.py --validate --generate
    
- name: Check for unsynced changes
  run: |
    if git diff --name-only | grep -q "dags/dag_pipeline_santander.py"; then
      echo " DAG foi regerado pelo script de sincronização"
      echo " Por favor, commit as mudanças geradas"
      git diff dags/dag_pipeline_santander.py
      exit 1
```

### Validação Manual

```bash
# Validar consistência
python scripts/sync_airflow_from_databricks.py --validate

# Gerar DAG
python scripts/sync_airflow_from_databricks.py --generate
```

## Mapeamento de Dependências

### Legenda de Dependências

| Tipo | Exemplo | Significado |
|------|---------|-------------|
| `[] >> task` | `[t1, t2] >> t3` | t3 depende de t1 e t2 (ambos devem completar) |
| `task >> []` | `t1 >> [t2, t3]` | t2 e t3 podem rodar em paralelo após t1 |
| `condition` | `condition: "${var.enable_streaming} == true"` | Task só roda se condição for verdadeira |

### Estrutura Atual

```

 SETUP                                                        
 t0_unity_catalog                                           

                   
        
                               
  
 INGESTION (Paralelo)   SILVER CLIENTES    
 t1_extracao_acoes    t6_clientes_silver
 t1_extracao_bcb     
 t1_extracao_world_bank
 t5_streaming*       * Só roda se enable_streaming=true
 t6_clientes_ordens 

                   
        
                               
  
 SILVER (Por Fonte)   SILVER CLIENTES    
 t2_silver_acoes      (já finalizado)   
 t2_silver_bcb       
 t2_silver_world_bank

                   
        
                               
  
 GOLD MARKET         GOLD CLIENT        
 t3_anomalias        t7_corretora...     
 t3_performance      t9_scd             
 t3_bcb              t3_fraude          
 t3_world_bank      
 t3_acoes_cambio  

                   
        
                               
  
 CARGA SQL           STREAMING GOLD     
 t_sql_acoes         t10_streaming...    
 t_sql_clientes     
 t_sql_fraude     
 t_sql_streaming  
 t_sql_macro      

                   
                   

 FINALIZAÇÃO                                                  
 t8_lakehouse_monitoring  t4_observabilidade             

```

## Ambientes e Streaming

### HK (Homologação Reduzido)

```yaml
pipeline_completo:
  # Tasks com condicional
  - task_key: t5_streaming
    condition: "${var.enable_streaming} == false"  # Não roda em HK
  
  - task_key: t10_streaming_gold
    condition: "${var.enable_streaming} == false"  # Não roda em HK
```

**Resultado em HK:**
-  Setup, Ingestion, Silver, Gold Market, Gold Client funcionam
-  Streaming tasks são puladas (economia de custo)
-  SQL Streaming é pulado

### PROD (Produção Completa)

```yaml
pipeline_completo:
  # Tasks com condicional
  - task_key: t5_streaming
    condition: "${var.enable_streaming} == true"  # Roda em PROD
  
  - task_key: t10_streaming_gold
    condition: "${var.enable_streaming} == true"  # Roda em PROD
```

**Resultado em PROD:**
-  Todas as tasks funcionam incluindo streaming
-  SQL Streaming carrega dados em tempo real
-  Análises de fraude e anomalias em tempo real

## Troubleshooting

### Erro: "DAG não contém dependências"

**Causa:** Databricks.yml tem workflow pai mas DAG não foi regerado

**Solução:**
```bash
python scripts/sync_airflow_from_databricks.py --generate
git add dags/dag_pipeline_santander.py
git commit -m "Atualiza DAG com dependências do workflow pai"
```

### Erro: "Dependência circular detectada"

**Causa:** Dependência circular no workflow pai (A depende de B, B depende de A)

**Solução:**
```yaml
# Revisar databricks.yml e remover dependência circular
- task_key: t3_anomalias
  depends_on:
    - task_key: t3_performance  #  Circular se t3_performance depende de t3_anomalias
```

### Erro: "Task não encontrada no mapeamento"

**Causa:** Task_key no workflow não tem mapeamento para job_path

**Solução:**
```python
# Adicionar mapeamento em scripts/sync_airflow_from_databricks.py
def _map_task_to_job_path(self, task_key: str) -> str:
    mapping = {
        'novo_task': 'jobs/job_novo.py',  # ← Adicionar aqui
    }
    return mapping.get(task_key, '')
```

## Boas Práticas

###  DO

- **Sempre** edite `databricks.yml` primeiro
- **Sempre** rode script de sincronização após mudanças
- **Sempre** commit ambos os arquivos juntos
- **Sempre** teste dependências em HK antes de PROD
- **Use** workflow pai para dependências complexas

###  DON'T

- **Nunca** edite o DAG manualmente (será sobrescrito)
- **Nunca** adicione dependências apenas no DAG
- **Nunca** ignore avisos de desincronização
- **Nunca** adicione schedules individuais (use workflow pai)
- **Nunca** remova workflow pai sem motivo

## Monitoramento

### Dashboard de Dependências

Criar dashboard no Airflow para visualizar:

```python
# Airflow → Browse → DAG → Graph View
# Mostra dependências visualmente
```

### Logs de Sincronização

```bash
python scripts/sync_airflow_from_databricks.py --validate --generate
```

Saída:
```
 Validando consistência...
 DAG consistente com databricks.yml (22 tasks)
 Workflow pai detectado - dependências sincronizadas
 Streaming condicional configurado: ${var.enable_streaming}
```

## Resumo

### Vantagens da Sincronização Bidirecional

 **Single Source of Truth**: `databricks.yml` é a única fonte
 **Consistência Garantida**: CI/CD valida sempre
 **Manutenção Simplificada**: Edite um lugar, reflita em ambos
 **Dependências Complexas**: Suporta workflows com múltiplas dependências
 **Ambientes Diferentes**: HK sem streaming, PROD completo
 **Prevenção de Erros**: Validação antes de deploy
 **Documentação Viva**: DAG sempre atualizado com dependências

### Fluxo de Trabalho

1. **Editar** `databricks.yml` (dependências, schedule, etc.)
2. **Executar** `python scripts/sync_airflow_from_databricks.py`
3. **Validar** consistência via CI/CD
4. **Commitar** ambos os arquivos
5. **Deploy** para Databricks (deploy via bundle)
6. **Deploy** para Airflow (ou reiniciar containers)

O sistema garante que Airflow e Databricks Workflow mantenham exatamente a mesma sequência de jobs e dependências, com suporte a ambientes HK/PROD e streaming condicional!
