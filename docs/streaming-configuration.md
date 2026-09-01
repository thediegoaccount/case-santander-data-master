# Configuração do Processo de Streaming

## Visão Geral

O pipeline de streaming processa transações financeiras em tempo real usando Azure Event Hub + Databricks Structured Streaming + Delta Lake.

## Arquitetura de Streaming

```

                      AZURE EVENT HUB                        
  evhcasesantander (Namespace)                              
  transacoes-financeiras (Event Hub)                        
                                                             
  Transações de compra/venda em tempo real                  

                    (Kafka Protocol)
                   

              DATABRICKS AUTO LOADER                         
  job_streaming.py (t5_streaming)                          
                                                             
  - Lê parquet do Event Hub via cloudFiles                 
  - Schema: timestamp, ticker, preco, quantidade, etc.      
  - Trigger: availableNow (batch incremental)               

                   
                   

                   BRONZE - ADLS                            
  abfss://bronze@stcasesantander/kafka/                     
                                                             
  Dados brutos parquet do Event Hub                         

                   
                   

              PROCESSAMENTO STREAMING                        
  job_streaming.py (t5_streaming)                          
                                                             
  Transformações:                                           
  - timestamp → to_timestamp                                
  - hora, minuto (extração)                                 
  - valor_total = preco * quantidade                        
  - alerta_volume (Normal/Medio/Alto)                       
  - alerta_preco (Normal/Baixo/Alto)                        
  - processado_em (timestamp de processamento)              

                   
                   

                   SILVER - ADLS                            
  abfss://silver@stcasesantander/streaming/                
  Unity Catalog: case_santander.silver.streaming           
                                                             
  Dados processados em Delta Lake                           
  Checkpoint: abfss://silver@stcasesantander/checkpoints/   

                   
                   

              STREAMING TO GOLD                              
  job_streaming_to_gold.py (t10_streaming_gold)             
                                                             
  CDC (Change Data Feed):                                    
  - Lê apenas mudanças desde última versão                  
  - Filtra inserts (_change_type = 'insert')                
  - Evita full scan da tabela                               

                   
        
                               
  
 GOLD 1              GOLD 2           
 fraudes_streaming   anomalias_intraday
   
 GOLD 3              GOLD 4           
 volume_intraday     ranking_acoes_realtime
  
```

## Configuração Atual

### 1. Job de Ingestão Streaming (`jobs/job_streaming.py`)

**Função:** Ingesta dados do Event Hub para Bronze/Silver

**Código principal:**
```python
# Auto Loader - Databricks cloudFiles
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
    .option("cloudFiles.maxFilesPerTrigger", 1) \
    .schema(schema_transacao) \
    .load(bronze_kafka_path)

# Transformações
df_processado = df_stream \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("hora", F.hour("timestamp")) \
    .withColumn("minuto", F.minute("timestamp")) \
    .withColumn("valor_total", F.round(F.col("preco") * F.col("quantidade"), 2)) \
    .withColumn("alerta_volume", F.when(...)) \
    .withColumn("alerta_preco", F.when(...)) \
    .withColumn("processado_em", F.lit(datetime.now().isoformat()))

# Write Stream
query = df_processado.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \  # Batch incremental
    .start(silver_streaming_path)
```

**Schema de dados:**
```python
schema_transacao = StructType([
    StructField("timestamp",    StringType(),  True),
    StructField("ticker",       StringType(),  True),
    StructField("preco",        DoubleType(),  True),
    StructField("quantidade",   LongType(),    True),
    StructField("tipo",         StringType(),  True),
    StructField("corretora",    StringType(),  True),
    StructField("id_transacao", StringType(),  True)
])
```

**Transformações aplicadas:**
- **timestamp**: Conversão para timestamp
- **hora, minuto**: Extração de componentes de tempo
- **valor_total**: Preço × Quantidade
- **alerta_volume**: Normal (<5000), Médio (5000-8000), Alto (>8000)
- **alerta_preco**: Normal (15-80), Baixo (<15), Alto (>80)
- **processado_em**: Timestamp de processamento

**Output:**
- **Bronze**: `abfss://bronze@stcasesantander/kafka/` (não processado)
- **Silver**: `abfss://silver@stcasesantander/streaming/` (processado)
- **Unity Catalog**: `case_santander.silver.streaming`

### 2. Job de Streaming to Gold (`jobs/job_streaming_to_gold.py`)

**Função:** Transforma Silver.streaming em tabelas Gold analíticas

**CDC (Change Data Feed):**
```python
# Lê apenas mudanças desde última versão
ultima_versao = spark.sql("""
    SELECT COALESCE(MAX(versao_cdf), 0)
    FROM case_santander.gold.observabilidade
    WHERE tabela = 'streaming'
""").collect()[0][0]

df_cdf = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", ultima_versao) \
    .table("case_santander.silver.streaming") \
    .filter("_change_type = 'insert'") \
    .drop("_change_type", "_commit_version", "_commit_timestamp")
```

**Tabelas Gold geradas:**

#### GOLD 1: `case_santander.gold.fraudes_streaming`
**Função:** Detecção de fraudes em tempo real

**Lógica:**
```python
# Transações suspeitas:
# - Volume > 10000
# - Preço fora de faixa normal
# - Múltiplas transações do mesmo ticker em curto período
```

#### GOLD 2: `case_santander.gold.anomalias_intraday`
**Função:** Anomalias de preço vs histórico por ticker/hora

**Lógica:**
```python
# Z-score de preço vs média histórica
# Detecção de desvios significativos
```

#### GOLD 3: `case_santander.gold.volume_intraday`
**Função:** Volume negociado por ticker e hora do dia

**Lógica:**
```python
# Agregação por ticker, hora
# Volume total, médio, máximo
```

#### GOLD 4: `case_santander.gold.ranking_acoes_realtime`
**Função:** Ranking de ativos por volume no dia atual

**Lógica:**
```python
# Ordenação por volume total
# Top 10 ativos do dia
```

## Configuração de Ambientes

### HK (Homologação Reduzido)

**Status:** **STREAMING DESABILITADO** 

**Configuração:**
```yaml
# databricks.yml
environments:
  hk:
    variables:
      enable_streaming: false

# Workflow pai
- task_key: t5_streaming
  condition: "${var.enable_streaming} == false"  # Não roda em HK

- task_key: t10_streaming_gold
  condition: "${var.enable_streaming} == false"  # Não roda em HK
```

**Implicações:**
-  job_streaming.py não é executado
-  job_streaming_to_gold.py não é executado
-  Tabelas Silver/Gold de streaming não são atualizadas
-  Economia de custo (Event Hub não é usado)
-  Pipeline mais rápido (sem processamento de streaming)

### PROD (Produção Completo)

**Status:** **STREAMING HABILITADO** 

**Configuração:**
```yaml
# databricks.yml
environments:
  prod:
    variables:
      enable_streaming: true

# Workflow pai
- task_key: t5_streaming
  condition: "${var.enable_streaming} == true"  # Roda em PROD

- task_key: t10_streaming_gold
  condition: "${var.enable_streaming} == true"  # Roda em PROD
```

**Implicações:**
-  job_streaming.py é executado
-  job_streaming_to_gold.py é executado
-  Tabelas Silver/Gold de streaming são atualizadas
-  Detecção de fraudes em tempo real
-  Análises intraday em tempo real
-  Custo de Event Hub (~R$ 500-1000/mês)

## Dependências no Workflow

### Dependências do Streaming

```
t0_unity_catalog
    ↓
t5_streaming (condicional: enable_streaming == true)
    ↓
t10_streaming_gold (condicional: enable_streaming == true)
    ↓
t_sql_streaming (condicional: enable_streaming == true)
```

### Integração com Outros Jobs

**Dependência de performance_acoes:**
```yaml
# job_streaming_to_gold.py depende de:
# - case_santander.silver.streaming (t5_streaming)
# - case_santander.gold.performance_acoes (t3_performance)
```

## Configuração de Checkpoint

**Local:**
```python
checkpoint_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/checkpoints/streaming/"
```

**Gerenciamento:**
```python
# Limpa checkpoint anterior antes de iniciar
dbutils.fs.rm(checkpoint_path, recurse=True)

# Schema location para evolução automática
.option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
```

## Trigger Mode

**Atual:** `trigger(availableNow=True)`

**Significado:**
- Processa todos os dados disponíveis de uma vez
- Execução em batch incremental
- Não é contínuo (24/7)
- Executa como parte do pipeline diário

**Alternativas futuras:**
- `trigger(processingTime='1 minute')` - Execução contínua
- `trigger(once=True)` - Execução única

## Economia de Custo

### HK (Sem Streaming)
- **Event Hub:** R$ 0/mês (não usado)
- **Processamento:** Menor carga no cluster
- **Economia estimada:** ~R$ 500-1000/mês

### PROD (Com Streaming)
- **Event Hub:** ~R$ 500-1000/mês
- **Processamento:** Maior carga no cluster
- **Benefício:** Detecção de fraudes em tempo real

## Troubleshooting

### Erro: "Checkpoint já existe"

**Causa:** Checkpoint anterior não foi limpo

**Solução:**
```python
# O job já limpa automaticamente
dbutils.fs.rm(checkpoint_path, recurse=True)
```

### Erro: "Nenhuma transação nova encontrada"

**Causa:** Event Hub sem novos dados

**Solução:**
```python
# O job detecta e encerra graciosamente
if total_streaming == 0:
    print("Nenhuma transacao nova encontrada. Job encerrado.")
    return
```

### Erro: "CDC indisponível"

**Causa:** Change Data Feed não habilitado na tabela

**Solução:**
```python
# Fallback automático para leitura completa
except Exception:
    df_cdf = None
    _row = spark.sql("SELECT COUNT(*) as total FROM case_santander.silver.streaming").collect()[0]
    total_streaming = _row["total"]
    print(f"CDC indisponivel — leitura completa: {total_streaming} transacoes")
```

## Próximos Passos

### Melhorias Futuras

1. **Streaming Contínuo (24/7)**
   - Mudar para `trigger(processingTime='1 minute')`
   - Execução independente do pipeline diário

2. **Alertas em Tempo Real**
   - Integração com Microsoft Teams/Slack
   - Notificações de fraudes detectadas

3. **ML em Streaming**
   - Modelos de ML para detecção de anomalias
   - Predição de preços em tempo real

4. **Monitoring Avançado**
   - Dashboard de latência de streaming
   - Métricas de throughput

## Resumo

**Configuração atual:**
- **Ingestão:** Event Hub → Auto Loader → Bronze → Silver
- **Processamento:** Transformações Spark Structured Streaming
- **Análise:** 4 tabelas Gold (fraudes, anomalias, volume, ranking)
- **HK:** Desabilitado (economia de custo)
- **PROD:** Habilitado (deteção em tempo real)
- **Trigger:** Batch incremental (availableNow)
- **CDC:** Change Data Feed para leitura incremental

O streaming está **totalmente configurado** e integrado ao pipeline com suporte a ambientes HK/PROD!
