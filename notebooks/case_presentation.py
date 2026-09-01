# Databricks notebook source
# MAGIC %md
# MAGIC <div style="background: linear-gradient(135deg, #EC0000 0%, #7B0000 100%); padding: 40px; border-radius: 12px; text-align: center;">
# MAGIC   <h1 style="color: white; font-size: 2.4em; margin: 0 0 8px 0; letter-spacing: 1px;">Case Técnico — Engenharia de Dados</h1>
# MAGIC   <h2 style="color: #FFD700; font-size: 1.6em; margin: 0 0 20px 0;">Santander Corretora Digital</h2>
# MAGIC   <p style="color: #FFE0E0; font-size: 1.1em; margin: 0;">
# MAGIC     Plataforma de Dados Financeiros em Tempo Real · Azure Databricks · Delta Lake · Medallion Architecture
# MAGIC   </p>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | # | Seção | O que demonstra |
# MAGIC |---|-------|-----------------|
# MAGIC | 1 | Contexto e arquitetura geral | Visão de produto + decisões de design |
# MAGIC | 2 | Fontes de dados e Bronze | Ingestão multi-fonte, idempotência |
# MAGIC | 3 | Transformações Silver | Qualidade, LGPD, enriquecimento |
# MAGIC | 4 | Streaming — Event Hub + Auto Loader | Ingestão em tempo real, CDC |
# MAGIC | 5 | Gold — anomalias e performance | Z-Score, analytics financeiro |
# MAGIC | 6 | Gold — score de risco e fraude | Modelo ponderado, 4 regras de fraude |
# MAGIC | 7 | Streaming Gold — 4 análises RT | Fraude + anomalia + volume + ranking |
# MAGIC | 8 | SCD Type 2 | Auditoria regulatória de mudanças |
# MAGIC | 9 | Observabilidade e qualidade | Monitoramento, OPTIMIZE, VACUUM |
# MAGIC | 10 | CI/CD e orquestração | GitHub Actions, Airflow, Workflow |
# MAGIC | 11 | Segurança e governança | Key Vault, Unity Catalog, LGPD |
# MAGIC | 12 | Q&A — perguntas da banca | Respostas técnicas preparadas |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Contexto de Negócio e Arquitetura
# MAGIC
# MAGIC ### Problema
# MAGIC Uma corretora digital precisa de uma plataforma de dados capaz de:
# MAGIC - Monitorar **9 ações B3** em tempo real com detecção de anomalias estatísticas
# MAGIC - Calcular **score de risco** de 10.000 clientes para compliance e limites operacionais
# MAGIC - Detectar **fraude** em ordens de compra/venda em batch e streaming
# MAGIC - Integrar indicadores macroeconômicos (Selic, câmbio, IPCA, PIB)
# MAGIC - Rastrear **mudanças de perfil de risco** de clientes com auditoria completa (Resolução CVM 30/2021)
# MAGIC - Garantir conformidade com **LGPD** e padrões de segurança do Banco Central
# MAGIC
# MAGIC ### Volumetria
# MAGIC | Fonte | Volume | Frequência |
# MAGIC |-------|--------|-----------|
# MAGIC | Yahoo Finance (9 ações) | ~8.534 registros | Diária |
# MAGIC | BCB API (Selic, Câmbio, IPCA) | ~3.068 registros | Diária/Mensal |
# MAGIC | World Bank (PIB, Desemprego) | ~59 registros | Anual |
# MAGIC | Kaggle (clientes crédito) | 10.000 registros | Carga única |
# MAGIC | Azure Event Hub (transações) | 200+ / lote | Contínuo |

# COMMAND ----------
# MAGIC %md
# MAGIC ### Arquitetura da Solução

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC 
# MAGIC               PLATAFORMA DE DADOS — SANTANDER CORRETORA DIGITAL                          
# MAGIC 
# MAGIC                                                                                           
# MAGIC   FONTES               BRONZE              SILVER              GOLD              CONSUMO  
# MAGIC   (externas)           (bruto,              (limpo,             (analítico,                
# MAGIC                         imutável)            enriquecido)        negócio)                  
# MAGIC                                                                                           
# MAGIC   Yahoo Finance acoes               acoes         anomalias                  
# MAGIC   BCB API       bcb                 bcb                performance_acoes           
# MAGIC   World Bank    world_bank           world_bank                           Azure SQL 
# MAGIC   Kaggle        clientes LGPDclientes score_risco      Genie AI  
# MAGIC                        ordens               ordens             deteccao_fraude  Dashboard  
# MAGIC   Event Hub kafka               streaming      fraude_streaming            
# MAGIC   (Kafka/Auto Loader)                        (CDC)              anomalias_intraday          
# MAGIC                                                                 volume_intraday             
# MAGIC                                                                 ranking_realtime            
# MAGIC     
# MAGIC   ORQUESTRAÇÃO:  Databricks Workflow (prod) · Apache Airflow + Docker (dev)               
# MAGIC   GOVERNANÇA:    Unity Catalog · RBAC · Secret Scope · Azure Key Vault                   
# MAGIC   CI/CD:         GitHub Actions (quality → security → test → build → deploy)              
# MAGIC   CONFORMIDADE:  LGPD (hash SHA-256) · SCD Type 2 · VACUUM 7 dias · CVM Res.30/2021      
# MAGIC 
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ### Decisões de Arquitetura — Por quê cada escolha?
# MAGIC
# MAGIC | Decisão | Alternativa Descartada | Motivo |
# MAGIC |---------|------------------------|--------|
# MAGIC | **Delta Lake** em Silver/Gold | Parquet puro | ACID, CDC, SCD Type 2 via MERGE, Time Travel |
# MAGIC | **Parquet** no Bronze | Delta | Bronze é imutável — sem updates, sem MERGE necessário |
# MAGIC | **Auto Loader** para streaming | Kafka Consumer direto | Schema evolution, checkpointing nativo, sem listar diretório |
# MAGIC | **Liquid Clustering** | `partitionBy` estático | Rebalanceamento incremental via OPTIMIZE sem reescrever tabela |
# MAGIC | **Broadcast Join** | Sort-merge join padrão | `df_score` (7.900 × 4 cols) → broadcasting elimina shuffle de rede |
# MAGIC | **Z-Score por ticker** | Threshold fixo (> 5%) | Agnóstico ao preço absoluto — PETR4 ≠ WEGE3 em magnitude |
# MAGIC | **Unity Catalog** | Metastore legado | RBAC granular, linhagem de dados, compartilhamento entre workspaces |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Fontes de Dados — Camada Bronze

# COMMAND ----------

# Setup: carrega configurações do projeto
import sys, hashlib
sys.path.append("/Workspace/Repos/diego.silva0001@gmail.com/case-santander-data-master")

from src.config.settings import ACOES, get_paths

print("=" * 60)
print("AÇÕES B3 MONITORADAS")
print("=" * 60)
nomes_map = {
    "PETR4.SA": "Petrobras",    "VALE3.SA":  "Vale",
    "ITUB4.SA": "Itaú Unibanco","BBDC4.SA":  "Bradesco",
    "ABEV3.SA": "Ambev",        "MGLU3.SA":  "Magazine Luiza",
    "WEGE3.SA": "WEG",          "BBAS3.SA":  "Banco do Brasil",
    "SANB11.SA":"Santander BR",
}
setores_map = {
    "PETR4.SA": "Commodities", "VALE3.SA":  "Commodities",
    "ITUB4.SA": "Financeiro",  "BBDC4.SA":  "Financeiro",
    "BBAS3.SA": "Financeiro",  "SANB11.SA": "Financeiro",
    "ABEV3.SA": "Consumo",     "MGLU3.SA":  "Varejo",
    "WEGE3.SA": "Indústria",
}
for i, acao in enumerate(ACOES, 1):
    print(f"  {i:2}. {acao:<12}  {nomes_map.get(acao):<20}  {setores_map.get(acao)}")

# COMMAND ----------

# MAGIC %sql
-- Visão geral: tabelas Bronze no Unity Catalog
SHOW TABLES IN case_santander.bronze;

# COMMAND ----------

# MAGIC %sql
-- Volume por tabela Bronze
SELECT 'bronze.acoes'      AS tabela, COUNT(*) AS registros FROM case_santander.bronze.acoes     UNION ALL
SELECT 'bronze.bcb',                  COUNT(*)              FROM case_santander.bronze.bcb        UNION ALL
SELECT 'bronze.world_bank',           COUNT(*)              FROM case_santander.bronze.world_bank UNION ALL
SELECT 'bronze.clientes',             COUNT(*)              FROM case_santander.bronze.clientes   UNION ALL
SELECT 'bronze.ordens',               COUNT(*)              FROM case_santander.bronze.ordens     UNION ALL
SELECT 'bronze.kafka',                COUNT(*)              FROM case_santander.bronze.kafka
ORDER BY registros DESC;

# COMMAND ----------

# MAGIC %sql
-- Amostra Bronze: ações brutas exatamente como vieram do Yahoo Finance
-- Sem variação %, sem empresa, sem setor — dado bruto e fiel à fonte
SELECT date, ticker, open, high, low, close, volume, data_extracao
FROM case_santander.bronze.acoes
WHERE ticker = 'SANB11.SA'
ORDER BY date DESC
LIMIT 8;

# COMMAND ----------

# MAGIC %sql
-- Bronze BCB: indicadores macroeconômicos brutos
-- Particionado por extracao (não por data) para evitar conflito de colunas
SELECT data, indicador, valor, extracao
FROM case_santander.bronze.bcb
ORDER BY indicador, data DESC
LIMIT 10;

# COMMAND ----------
# MAGIC %md
# MAGIC > **Bronze = dado fiel à fonte, imutável.**
# MAGIC > Usar Parquet (não Delta) aqui é uma decisão intencional: Bronze nunca recebe updates ou merges.
# MAGIC > A coluna `data_extracao` garante idempotência — podemos reextrair sem perder histórico de qualquer dia.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Camada Silver — Transformações e Enriquecimento
# MAGIC
# MAGIC ### O que muda Bronze → Silver?

# COMMAND ----------

# MAGIC %sql
-- ANTES (Bronze): tipos inferidos, sem contexto de negócio
DESCRIBE TABLE case_santander.bronze.acoes;

# COMMAND ----------

# MAGIC %sql
-- DEPOIS (Silver): tipos corretos + features derivadas de negócio
DESCRIBE TABLE case_santander.silver.acoes;

# COMMAND ----------

# MAGIC %sql
-- Comparação direta Bronze vs Silver para o mesmo ticker
SELECT
    'BRONZE' AS camada,
    CAST(date AS STRING) AS date,
    ticker,
    ROUND(open, 2) AS open, ROUND(close, 2) AS close,
    volume,
    CAST(NULL AS DOUBLE)  AS variacao_pct,
    CAST(NULL AS STRING)  AS empresa,
    CAST(NULL AS STRING)  AS setor
FROM case_santander.bronze.acoes
WHERE ticker = 'SANB11.SA'
UNION ALL
SELECT
    'SILVER',
    CAST(date AS STRING),
    ticker,
    ROUND(open, 2), ROUND(close, 2),
    volume,
    ROUND(variacao_diaria_pct, 4),
    empresa,
    setor
FROM case_santander.silver.acoes
WHERE ticker = 'SANB11.SA'
ORDER BY date DESC, camada
LIMIT 16;

# COMMAND ----------

# MAGIC %sql
-- Features derivadas na Silver: distribuição por setor
SELECT
    setor,
    COUNT(DISTINCT ticker)                     AS tickers,
    COUNT(*)                                   AS registros,
    ROUND(AVG(variacao_diaria_pct), 4)         AS variacao_media_pct,
    ROUND(AVG(amplitude_diaria), 4)            AS amplitude_media,
    ROUND(AVG(volume) / 1e6, 2)               AS volume_medio_M
FROM case_santander.silver.acoes
GROUP BY setor
ORDER BY volume_medio_M DESC;

# COMMAND ----------

# MAGIC %sql
-- Silver BCB: datas tipadas, precisão financeira de 6 casas decimais
SELECT
    data, indicador,
    ROUND(valor, 6) AS valor,
    ano, mes, trimestre
FROM case_santander.silver.bcb
WHERE indicador IN ('selic', 'cambio_usd')
  AND ano = 2024
ORDER BY data DESC
LIMIT 10;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.1 LGPD na Silver — Pseudonimização e Mascaramento

# COMMAND ----------

# Demonstrar o hash SHA-256 e mascaramento
print("LGPD — Pseudonimização aplicada ANTES de gravar no Bronze")
print("=" * 60)

customer_id = 15634872
sobrenome   = "Silva"

hash_cliente    = hashlib.sha256(str(customer_id).encode()).hexdigest()[:16]
sobrenome_masked = sobrenome[0] + "*" * (len(sobrenome) - 1)

print(f"\nDado original (Kaggle):  CustomerID = {customer_id}, Surname = '{sobrenome}'")
print(f"Dado armazenado:         hash_cliente = '{hash_cliente}', sobrenome_masked = '{sobrenome_masked}'")
print()
print("Propriedades do hash SHA-256:")
print("   Determinístico — mesmo ID sempre gera o mesmo hash (joins funcionam)")
print("   Irreversível  — impossível recuperar CustomerID a partir do hash")
print("   Sem colisão prática — SHA-256 com 16 chars cobre 16^16 = 1,8 × 10^19 valores")
print()
print("O que NUNCA é armazenado em nenhuma camada:")
print("   CPF         CustomerID original")
print("   Email       Nome completo")
print("   Telefone    Endereço")

# COMMAND ----------

# MAGIC %sql
-- Silver Clientes: perfil com LGPD + features derivadas
SELECT
    id_cliente,
    hash_cliente,        -- SHA-256 truncado: jamais o CustomerID original
    sobrenome_masked,    -- "Silva" → "S****"
    idade,
    faixa_etaria,        -- feature nova: "30-39", "40-49" etc.
    score_credito,
    score_categoria,     -- Baixo / Médio / Alto
    perfil_risco,
    saldo,
    faixa_saldo,
    ativo,
    churn
FROM case_santander.silver.clientes
LIMIT 10;

# COMMAND ----------

# MAGIC %sql
-- Distribuição de perfil de risco
SELECT
    perfil_risco,
    COUNT(*)                                                    AS clientes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)          AS pct,
    ROUND(AVG(score_credito), 0)                               AS score_medio,
    ROUND(AVG(saldo), 2)                                       AS saldo_medio,
    SUM(CASE WHEN churn = true THEN 1 ELSE 0 END)              AS churners
FROM case_santander.silver.clientes
GROUP BY perfil_risco
ORDER BY clientes DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Streaming em Tempo Real — Azure Event Hub + Auto Loader

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC  Simulador de Transações          Azure Event Hub           Databricks
# MAGIC                    
# MAGIC   for ticker in ACOES:                                   Auto Loader (cloudFiles)        
# MAGIC     preco = random     Kafka  Partições     · checkpoint automático         
# MAGIC     qtd   = random       Parquet  por ticker             · schema evolution              
# MAGIC     tipo  = C/V                                          · sem listar diretório          
# MAGIC                                                    
# MAGIC                                                                                             
# MAGIC                                                                  bronze.kafka                
# MAGIC                                                                                             
# MAGIC                                                                                             
# MAGIC                                                               Transformações + alertas       
# MAGIC                                                               (alerta_volume, alerta_preco)  
# MAGIC                                                                                             
# MAGIC                                                                                             
# MAGIC                                                               silver.streaming  (CDC ON)     
# MAGIC                                                             
# MAGIC ```

# COMMAND ----------

# Lógica do Auto Loader (código real de job_streaming.py)
print("""
AUTO LOADER — Por que é melhor que listar diretórios?

  Problema: ADLS com 1M arquivos → list() leva minutos
  Solução:  Auto Loader usa notificações de evento do Azure Storage
            (sem varredura de diretório)

df_stream = spark.readStream \\
    .format("cloudFiles")                              # Auto Loader
    .option("cloudFiles.format", "parquet")            # formato dos arquivos
    .option("cloudFiles.schemaLocation",               # guarda schema para evolução
            checkpoint_path + "/schema") \\
    .option("cloudFiles.maxFilesPerTrigger", 1)        # throughput controlado
    .schema(schema_transacao) \\
    .load(bronze_kafka_path)                           # ADLS path

df_stream.writeStream \\
    .format("delta") \\
    .trigger(availableNow=True)    # micro-batch sob demanda
    .option("checkpointLocation", checkpoint_path) \\
    .option("mergeSchema", "true") \\
    .toTable("case_santander.bronze.kafka")

Propriedades:
   Exactly-once: checkpoint garante sem duplicação mesmo após crash
   Schema evolution: novos campos não quebram o pipeline
   Escala a bilhões de arquivos sem mudança de código
""")

# COMMAND ----------

# MAGIC %sql
-- Amostra das transações streaming processadas
SELECT
    id_transacao,
    timestamp,
    ticker,
    ROUND(preco, 2)       AS preco,
    quantidade,
    tipo,                 -- compra / venda
    ROUND(valor_total, 2) AS valor_total,
    alerta_volume,        -- Normal / Volume Medio / Volume Alto
    alerta_preco,         -- Normal / Preco Alto / Preco Baixo
    hora,
    minuto
FROM case_santander.silver.streaming
ORDER BY timestamp DESC
LIMIT 10;

# COMMAND ----------

# MAGIC %sql
-- Volume de transações por ticker e tipo
SELECT
    ticker,
    tipo,
    COUNT(*)                       AS transacoes,
    SUM(quantidade)                AS volume_total,
    ROUND(AVG(preco), 2)           AS preco_medio,
    ROUND(SUM(valor_total), 0)     AS valor_financeiro_total
FROM case_santander.silver.streaming
GROUP BY ticker, tipo
ORDER BY ticker, tipo;

# COMMAND ----------

# MAGIC %sql
-- CDC habilitado em silver.streaming: confirmar configuração
SHOW TBLPROPERTIES case_santander.silver.streaming;

# COMMAND ----------

# MAGIC %sql
-- Leitura CDC: apenas inserções novas desde a versão 1
-- É exatamente o que o job_streaming_to_gold.py faz a cada execução
SELECT
    id_transacao, timestamp, ticker, tipo, preco, valor_total,
    _change_type,
    _commit_version,
    _commit_timestamp
FROM table_changes('case_santander.silver.streaming', 1)
WHERE _change_type = 'insert'
ORDER BY _commit_timestamp DESC
LIMIT 15;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Por que `trigger(availableNow=True)` e não streaming contínuo?
# MAGIC
# MAGIC | Modo | Latência | Custo | Caso de uso |
# MAGIC |------|----------|-------|-------------|
# MAGIC | `availableNow=True` | 3-5 min | Baixo — cluster para após processar |  Este case |
# MAGIC | `processingTime("30s")` | 30s | Médio — cluster sempre ativo | Alertas < 1 min |
# MAGIC | `continuous` | < 1s | Alto — cluster dedicado | HFT, pagamentos RT |
# MAGIC
# MAGIC Para detecção de fraude intraday, 3-5 min de latência é aceitável. O custo de infraestrutura é ~10x menor que streaming contínuo.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Camada Gold — Anomalias e Performance
# MAGIC
# MAGIC ### 5.1 Detecção de Anomalias via Z-Score

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC          x − μ          variacao_diaria_pct − média_do_ticker
# MAGIC z =   =  
# MAGIC          σ                      stddev_do_ticker
# MAGIC
# MAGIC |z| > 2  →  Anomalia  (apenas 4,6% dos valores ficam fora de ±2σ)
# MAGIC  z > 2   →  "Alta Anormal"
# MAGIC  z < −2  →  "Queda Anormal"
# MAGIC
# MAGIC Por que por ticker e não global?
# MAGIC  PETR4 (R$15) e WEGE3 (R$50) têm volatilidades e magnitudes diferentes.
# MAGIC  Normalizar por ticker torna o z-score comparável entre ativos.
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
-- Distribuição de anomalias detectadas
SELECT
    tipo_anomalia,
    COUNT(*)                                    AS ocorrencias,
    ROUND(AVG(ABS(zscore)), 4)                 AS zscore_medio,
    ROUND(MAX(ABS(zscore)), 4)                 AS zscore_maximo,
    ROUND(AVG(ABS(variacao_diaria_pct)), 4)    AS variacao_media_abs_pct
FROM case_santander.gold.anomalias
GROUP BY tipo_anomalia
ORDER BY ocorrencias DESC;

# COMMAND ----------

# MAGIC %sql
-- Top 10 anomalias mais extremas
SELECT
    date,
    ticker,
    empresa,
    setor,
    ROUND(close, 2)               AS preco_fechamento,
    ROUND(variacao_diaria_pct, 4) AS variacao_pct,
    ROUND(zscore, 4)              AS zscore,
    tipo_anomalia
FROM case_santander.gold.anomalias
WHERE anomalia = true
ORDER BY ABS(zscore) DESC
LIMIT 10;

# COMMAND ----------

# MAGIC %sql
-- Qual ação é mais volátil? (% de dias com anomalia)
SELECT
    ticker,
    empresa,
    setor,
    COUNT(*)                                              AS total_dias,
    SUM(CASE WHEN anomalia THEN 1 ELSE 0 END)            AS dias_anomalia,
    ROUND(SUM(CASE WHEN anomalia THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                         AS pct_anomalias,
    ROUND(AVG(CASE WHEN anomalia THEN ABS(zscore) END),4) AS zscore_medio_anomalias
FROM case_santander.gold.anomalias
GROUP BY ticker, empresa, setor
ORDER BY pct_anomalias DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.2 Performance por Setor e Ano

# COMMAND ----------

# MAGIC %sql
-- Performance anual por setor: retorno médio vs. volatilidade
SELECT
    setor,
    ano,
    COUNT(DISTINCT ticker)              AS tickers,
    ROUND(AVG(variacao_media_pct), 4)  AS retorno_medio_pct,
    ROUND(AVG(volatilidade), 4)        AS volatilidade_media,
    ROUND(AVG(volume_medio) / 1e6, 2)  AS volume_medio_M,
    ROUND(SUM(volume_total) / 1e9, 2)  AS volume_total_B
FROM case_santander.gold.performance_acoes
GROUP BY setor, ano
ORDER BY setor, ano;

# COMMAND ----------

# MAGIC %sql
-- Risco x Retorno por ticker (base para análise de portfólio)
SELECT
    ticker,
    empresa,
    setor,
    ROUND(AVG(variacao_media_pct), 4)   AS retorno_medio,
    ROUND(AVG(volatilidade), 4)         AS volatilidade_media,
    CASE
        WHEN AVG(variacao_media_pct) > 0 AND AVG(volatilidade) < 1.5
            THEN 'Ideal (alto retorno, baixo risco)'
        WHEN AVG(variacao_media_pct) > 0 AND AVG(volatilidade) >= 1.5
            THEN 'Retorno com risco'
        WHEN AVG(variacao_media_pct) <= 0
            THEN 'Risco sem retorno'
    END AS perfil_ativo
FROM case_santander.gold.performance_acoes
GROUP BY ticker, empresa, setor
ORDER BY retorno_medio DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Camada Gold — Score de Risco e Detecção de Fraude
# MAGIC
# MAGIC ### 6.1 Score de Risco de Clientes

# COMMAND ----------
# MAGIC %md
# MAGIC **Fórmula de score ponderado:**
# MAGIC ```
# MAGIC score_risco = score_credito   × 0.40   (40% — capacidade de pagamento histórica)
# MAGIC            + score_atividade  × 0.20   (20% — frequência de uso da plataforma)
# MAGIC            + score_diversif   × 0.20   (20% — diversificação de carteira)
# MAGIC            + score_comportam  × 0.20   (20% — padrão de comportamento histórico)
# MAGIC
# MAGIC Score → Categoria → Limite Operacional
# MAGIC   0–40   → Alto Risco      → R$  50.000
# MAGIC  40–70   → Risco Moderado  → R$ 200.000
# MAGIC  70–100  → Baixo Risco     → R$ 500.000
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
-- Distribuição de clientes por categoria de risco
SELECT
    categoria_risco,
    COUNT(*)                                                   AS clientes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)         AS pct,
    ROUND(AVG(score_risco), 2)                                 AS score_medio,
    ROUND(AVG(limite_operacional), 0)                          AS limite_medio,
    SUM(CASE WHEN requer_revisao = true THEN 1 ELSE 0 END)     AS requer_revisao
FROM case_santander.gold.score_risco_clientes
GROUP BY categoria_risco
ORDER BY score_medio DESC;

# COMMAND ----------

# MAGIC %sql
-- Clientes de alto risco ativos: candidatos a revisão de compliance
SELECT
    s.hash_cliente,
    s.categoria_risco,
    ROUND(s.score_risco, 2)         AS score,
    s.limite_operacional,
    c.perfil_risco                  AS perfil_declarado,
    c.score_credito,
    c.idade,
    c.faixa_etaria,
    c.ativo
FROM case_santander.gold.score_risco_clientes s
JOIN case_santander.silver.clientes c ON s.hash_cliente = c.hash_cliente
WHERE s.categoria_risco = 'Alto Risco'
  AND c.ativo = true
ORDER BY s.score_risco ASC
LIMIT 10;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.2 Detecção de Fraude — 4 Regras Batch

# COMMAND ----------
# MAGIC %md
# MAGIC | Regra | Condição | Justificativa |
# MAGIC |-------|----------|---------------|
# MAGIC | 1. Valor Operacional Elevado | `valor_total > limite_operacional` | Supera o limite calculado pelo score de risco |
# MAGIC | 2. Volume Suspeito | `quantidade > 9.000 unidades` | Muito acima da média de mercado para varejo |
# MAGIC | 3. Preço Atípico | `preco > R$90 ou preco < R$12` | Fora da faixa histórica das 9 ações monitoradas |
# MAGIC | 4. Perfil Incompatível | `perfil = Conservador AND valor > R$200.000` | Suitability: ordem incompatível com perfil declarado |
# MAGIC
# MAGIC **Score:** `total_alertas ≥ 3 → Crítico | = 2 → Alto | = 1 → Médio | = 0 → Normal`

# COMMAND ----------

# MAGIC %sql
-- Distribuição de nível de risco de fraude
SELECT
    nivel_risco_fraude,
    COUNT(*)                                              AS ordens,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)    AS pct,
    ROUND(AVG(valor_total), 2)                            AS valor_medio,
    SUM(CASE WHEN requer_revisao = true THEN 1 ELSE 0 END) AS requer_revisao
FROM case_santander.gold.deteccao_fraude
GROUP BY nivel_risco_fraude
ORDER BY
    CASE nivel_risco_fraude
        WHEN 'Critico' THEN 1 WHEN 'Alto' THEN 2
        WHEN 'Medio'   THEN 3 ELSE 4
    END;

# COMMAND ----------

# MAGIC %sql
-- Ordens críticas: detalhes para equipe de compliance
SELECT
    f.id_ordem,
    f.hash_cliente,
    f.ticker,
    ROUND(f.preco, 2)                AS preco,
    f.quantidade,
    ROUND(f.valor_total, 2)          AS valor_total,
    f.tipo,
    f.nivel_risco_fraude,
    f.total_alertas,
    f.alerta_valor_alto,
    f.alerta_volume_suspeito,
    f.alerta_preco_atipico,
    f.alerta_perfil_incompativel,
    c.perfil_risco                   AS perfil_cliente
FROM case_santander.gold.deteccao_fraude f
JOIN case_santander.silver.clientes c ON f.hash_cliente = c.hash_cliente
WHERE f.nivel_risco_fraude = 'Critico'
ORDER BY f.total_alertas DESC, f.valor_total DESC
LIMIT 15;

# COMMAND ----------

# Demonstrar o Broadcast Join usado na detecção de fraude
print("""
BROADCAST JOIN — Aplicado em fraude.py e streaming_gold.py


  # df_score: ~7.900 linhas × 4 colunas < 1 MB → broadcast
  df_score = spark.table("case_santander.gold.score_risco_clientes") \\
                  .select("hash_cliente", "score_risco",
                           "categoria_risco", "limite_operacional")

  df_fraude = df_ordens \\
      .join(F.broadcast(df_score), on="hash_cliente", how="left")
      # ↑ distribui df_score a todos os executors
      # ↑ df_ordens (5.341 linhas) não é shuffled

  SEM broadcast: sort-merge join → rede carrega 5.341 × 7.900 pares
  COM broadcast: df_score vai uma vez para cada nó → zero shuffle
""")

# COMMAND ----------

# MAGIC %sql
-- Confirmar BroadcastHashJoin no plano de execução
EXPLAIN FORMATTED
SELECT o.*, s.score_risco, s.categoria_risco, s.limite_operacional
FROM case_santander.silver.ordens o
LEFT JOIN case_santander.gold.score_risco_clientes s
    ON o.hash_cliente = s.hash_cliente;
-- Procurar por "BroadcastHashJoin" no plano de execução físico

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Streaming Gold — 4 Análises em Tempo Real

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC silver.streaming (CDC)  job_streaming_to_gold.py
# MAGIC                                      
# MAGIC                   
# MAGIC                                                                         
# MAGIC           fraude_streaming   anomalias_intraday   volume_intraday   ranking_realtime
# MAGIC           (4 regras RT)      (Z-Score por hora)   (pressão C/V)     (rank por volume)
# MAGIC ```
# MAGIC
# MAGIC O job usa **CDC (Change Data Feed)** — lê apenas as transações novas desde a última versão.
# MAGIC Custo: O(novos_registros), não O(total_tabela).

# COMMAND ----------

# MAGIC %sql
-- Fraude Streaming: mesma lógica batch + regra de desvio histórico
SELECT
    nivel_risco_fraude,
    COUNT(*)                              AS transacoes,
    ROUND(AVG(valor_total), 2)            AS valor_medio,
    SUM(CASE WHEN alerta_desvio_historico THEN 1 ELSE 0 END) AS com_desvio_historico
FROM case_santander.gold.fraude_streaming
GROUP BY nivel_risco_fraude
ORDER BY
    CASE nivel_risco_fraude
        WHEN 'Critico' THEN 1 WHEN 'Alto' THEN 2
        WHEN 'Medio'   THEN 3 ELSE 4
    END;

# COMMAND ----------

# MAGIC %sql
-- Anomalias intraday: variações extremas por hora do dia
SELECT
    ticker,
    hora,
    ROUND(preco_medio_hora, 2)     AS preco_medio_hora,
    ROUND(preco_medio, 2)          AS preco_historico,
    volume_hora,
    ROUND(zscore_intraday, 4)      AS zscore,
    tipo_anomalia_intraday
FROM case_santander.gold.anomalias_intraday
WHERE anomalia = true
ORDER BY ABS(zscore_intraday) DESC
LIMIT 15;

# COMMAND ----------

# MAGIC %sql
-- Volume Intraday: pressão compradora vs. vendedora por hora
SELECT
    ticker,
    hora,
    total_transacoes,
    ROUND(volume_compras / (volume_compras + volume_vendas + 0.001) * 100, 1) AS pct_compras,
    ROUND(volume_vendas  / (volume_compras + volume_vendas + 0.001) * 100, 1) AS pct_vendas,
    pressao_mercado,           -- "Compra" / "Venda" / "Neutro"
    alerta_volume,             -- "Normal" / "Alto" / "Critico"
    ROUND(pct_volume_diario, 2) AS pct_volume_diario
FROM case_santander.gold.volume_intraday
ORDER BY hora, ticker
LIMIT 18;

# COMMAND ----------

# MAGIC %sql
-- Ranking de ações em tempo real por volume financeiro
SELECT
    rank_volume                            AS posicao,
    ticker,
    total_transacoes,
    ROUND(volume_total / 1e6, 2)           AS volume_M,
    ROUND(valor_total / 1e6, 2)            AS valor_financeiro_M,
    ROUND(preco_medio_atual, 2)            AS preco_atual,
    ROUND(variacao_vs_historico_pct, 4)   AS variacao_vs_historico_pct,
    tendencia                              -- "Alta" / "Estavel" / "Queda"
FROM case_santander.gold.ranking_acoes_realtime
ORDER BY rank_volume;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. SCD Type 2 — Auditoria de Mudanças de Perfil
# MAGIC
# MAGIC **Requisito regulatório:** Resolução CVM 30/2021 exige rastreabilidade de mudanças de suitability (perfil de investidor).

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC Sem SCD Type 2:                    Com SCD Type 2:
# MAGIC   clientes_scd                       clientes_scd
# MAGIC               
# MAGIC    hash   perfil                   hash   perfil       data_inicio  data_fim   atual 
# MAGIC    abc1   Arrojado                 abc1   Conservador  2023-01-01   2023-08-15 FALSE 
# MAGIC                abc1   Moderado     2023-08-15   2024-03-10 FALSE 
# MAGIC                                       abc1   Arrojado     2024-03-10   9999-12-31 TRUE  
# MAGIC   Só o estado atual.                 
# MAGIC   Sem auditoria.                     Histórico completo. Auditável.
# MAGIC ```
# MAGIC
# MAGIC **Implementação via Delta MERGE:**
# MAGIC 1. Para registros que mudaram: `UPDATE SET data_fim = TODAY(), atual = FALSE`
# MAGIC 2. Para novos registros: `INSERT com data_inicio = TODAY(), data_fim = 9999-12-31, atual = TRUE`

# COMMAND ----------

# MAGIC %sql
-- Clientes que mudaram de perfil (têm mais de 1 registro no SCD)
SELECT
    hash_cliente,
    COUNT(*)                                   AS versoes,
    MIN(perfil_risco)                          AS perfil_inicial,
    MAX(CASE WHEN atual THEN perfil_risco END) AS perfil_atual,
    MIN(data_inicio)                           AS desde,
    MAX(data_inicio)                           AS ultima_mudanca
FROM case_santander.silver.clientes_scd
GROUP BY hash_cliente
HAVING COUNT(*) > 1
ORDER BY versoes DESC
LIMIT 15;

# COMMAND ----------

# MAGIC %sql
-- Histórico completo de um cliente específico (substituir hash conforme necessário)
SELECT
    hash_cliente,
    perfil_risco,
    score_credito,
    data_inicio,
    data_fim,
    atual,
    DATEDIFF(COALESCE(data_fim, CURRENT_DATE()), data_inicio) AS dias_neste_perfil
FROM case_santander.silver.clientes_scd
WHERE hash_cliente = (
    SELECT hash_cliente
    FROM case_santander.silver.clientes_scd
    GROUP BY hash_cliente
    HAVING COUNT(*) > 1
    LIMIT 1
)
ORDER BY data_inicio;

# COMMAND ----------

# MAGIC %sql
-- Evolução do score de risco Gold via SCD
SELECT
    hash_cliente,
    ROUND(score_risco, 2)   AS score_risco,
    categoria_risco,
    limite_operacional,
    data_inicio,
    data_fim,
    atual
FROM case_santander.gold.score_risco_scd
ORDER BY hash_cliente, data_inicio
LIMIT 20;

# COMMAND ----------

# MAGIC %sql
-- Estatística: quantos clientes já mudaram de categoria de risco?
SELECT
    COUNT(DISTINCT CASE WHEN versoes > 1 THEN hash_cliente END) AS clientes_mudaram,
    COUNT(DISTINCT hash_cliente)                                 AS total_clientes,
    ROUND(COUNT(DISTINCT CASE WHEN versoes > 1 THEN hash_cliente END) * 100.0
          / COUNT(DISTINCT hash_cliente), 2)                     AS pct_com_mudanca
FROM (
    SELECT hash_cliente, COUNT(*) AS versoes
    FROM case_santander.gold.score_risco_scd
    GROUP BY hash_cliente
);

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Observabilidade e Qualidade de Dados

# COMMAND ----------

# MAGIC %sql
-- Visão consolidada de qualidade — última execução
SELECT
    tabela,
    total_registros,
    ROUND(qualidade_pct, 2)                 AS qualidade_pct,
    total_nulos,
    total_duplicatas,
    CASE
        WHEN qualidade_pct < 95  THEN ' CRÍTICO'
        WHEN qualidade_pct < 99  THEN '🟡 ATENÇÃO'
        WHEN total_duplicatas > 0 THEN '🟡 ATENÇÃO'
        ELSE                          '🟢 OK'
    END AS status,
    timestamp_verificacao
FROM case_santander.gold.observabilidade
WHERE timestamp_verificacao = (
    SELECT MAX(timestamp_verificacao)
    FROM case_santander.gold.observabilidade
)
ORDER BY qualidade_pct ASC;

# COMMAND ----------

# MAGIC %sql
-- Histórico de qualidade ao longo do tempo
SELECT
    DATE(timestamp_verificacao) AS data,
    tabela,
    ROUND(qualidade_pct, 2)     AS qualidade_pct,
    total_registros
FROM case_santander.gold.observabilidade
WHERE tabela IN ('case_santander.silver.acoes', 'case_santander.gold.deteccao_fraude')
ORDER BY tabela, data DESC
LIMIT 20;

# COMMAND ----------

# MAGIC %sql
-- OPTIMIZE + ZORDER: compactar small files e reordenar para data skipping
-- (executado diariamente pelo job_observabilidade.py)
OPTIMIZE case_santander.gold.anomalias ZORDER BY (ticker, data_processamento);
-- Depois do OPTIMIZE: "numFiles" reduz, "sizeInBytes" por arquivo aumenta

# COMMAND ----------

# MAGIC %sql
-- VACUUM DRY RUN: ver o que seria removido (sem deletar de fato)
VACUUM case_santander.silver.streaming RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
-- Time Travel: consultar dados como estavam ontem (debugging, auditoria)
SELECT COUNT(*) AS registros_versao_0
FROM case_santander.gold.deteccao_fraude VERSION AS OF 0;
-- Ou por timestamp: TIMESTAMP AS OF '2025-04-10 06:00:00'

# COMMAND ----------

# MAGIC %sql
-- Histórico de versões Delta (time travel disponível)
DESCRIBE HISTORY case_santander.silver.acoes LIMIT 5;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 10. CI/CD e Orquestração

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.1 GitHub Actions — Pipeline de 8 Jobs

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC 
# MAGIC   PIPELINE CI/CD — GITHUB ACTIONS                                                
# MAGIC                                                                                   
# MAGIC   PULL REQUEST (qualquer branch)                                                  
# MAGIC   quality gate  bloqueia PR   
# MAGIC   (ruff + black + isort)          se falhar →                                    
# MAGIC           (se OK)                                                                
# MAGIC                                                                                  
# MAGIC   test  cobertura     
# MAGIC   (pytest + cov) + security (bandit + safety)  → Step Summary                   
# MAGIC                                                                                   
# MAGIC   MERGE → develop                                                                 
# MAGIC   quality → test + security → build (Docker) → deploy-dev → notify Slack        
# MAGIC                                                 ↑ automático                     
# MAGIC                                                                                   
# MAGIC   MERGE → main                                                                    
# MAGIC   quality → test + security → build → deploy-hk → [APROVAÇÃO MANUAL] → deploy-prod 
# MAGIC                                                         ↑                         
# MAGIC                             Settings > Environments > prod > Required Reviewers  
# MAGIC                                                                                  
# MAGIC                                                                GitHub Release +  
# MAGIC                                                                Notificação Slack 
# MAGIC 
# MAGIC ```
# MAGIC
# MAGIC **Destaques de segurança no CI:**
# MAGIC - `ruff` — linting moderno (substitui flake8/pylint)
# MAGIC - `black --check` — formatação sem modificar o código
# MAGIC - `bandit` — SAST: varredura de vulnerabilidades no código Python
# MAGIC - `safety` — vulnerabilidades conhecidas nas dependências (CVEs)
# MAGIC - Artefatos com relatórios JSON publicados por 30 dias
# MAGIC - `$GITHUB_STEP_SUMMARY` — tabela de deploy visível direto na UI do Actions

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.2 Databricks Workflow — Produção (06:00 BRT)

# COMMAND ----------
# MAGIC %md
# MAGIC ```
# MAGIC t0_unity_catalog 
# MAGIC  (schemas + Liquid Clustering + CDC)                               
# MAGIC                                                                    
# MAGIC t1_extracao  t2_silver 
# MAGIC (Yahoo + BCB + WorldBank)                                                       
# MAGIC                                                                                 
# MAGIC t5_streaming                      
# MAGIC (Auto Loader + Event Hub)                                                       
# MAGIC                                                                    t7_corretora_analises
# MAGIC t6_clientes_ordens          (posição + score
# MAGIC (Kaggle + LGPD + Ordens simuladas)                                  + perfil + SQL)
# MAGIC                                                                                 
# MAGIC                                                                                 
# MAGIC                                                                         t9_scd
# MAGIC                                                                   (SCD Type 2:
# MAGIC                                                                    clientes + score)
# MAGIC                                                                                 
# MAGIC                                                                                 
# MAGIC                                                                         t3_gold
# MAGIC                                                                   (anomalias + perf
# MAGIC                                                                    + fraude batch)
# MAGIC                                                                                 
# MAGIC                                                                                 
# MAGIC                                                                   t10_streaming_gold
# MAGIC                                                                   (fraude RT + anomalia
# MAGIC                                                                    intraday + vol + rank)
# MAGIC                                                                          
# MAGIC                                                              
# MAGIC                                                                                    
# MAGIC                                                      t8_lakehouse_monitor     t_carga_sql
# MAGIC                                                      (14 tabelas)             (Azure SQL)
# MAGIC                                                              
# MAGIC                                                                          
# MAGIC                                                                t4_observabilidade
# MAGIC                                                           (qualidade + OPTIMIZE + VACUUM)
# MAGIC
# MAGIC  Duração estimada: ~15-20 minutos para pipeline completo
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.3 Apache Airflow + Docker — Desenvolvimento Local

# COMMAND ----------

print("""
APACHE AIRFLOW — Por que dois orquestradores?


  Produção   → Databricks Workflow
              Nativo, gerenciado, auto-scaling, sem overhead operacional
              Integrado ao Unity Catalog e ao cluster

  Dev/Demo   → Apache Airflow + Docker Compose
              Padrão de mercado multi-cloud
              Demonstra portabilidade: os jobs .py são os MESMOS
              Facilita desenvolvimento local sem cluster Databricks

  Chave:     ZERO duplicação de lógica
             Airflow só orquestra via DatabricksSubmitRunOperator
             Não reescreve os jobs — apenas dispara e monitora

DAG (dags/dag_pipeline_santander.py):
  Schedule: '0 6 * * *'  →  06:00 BRT diário
  Operador: DatabricksSubmitRunOperator (API REST Databricks)
  Cluster:  configurável via variável de ambiente

Docker Compose (docker/docker-compose.prod.yml):
  CeleryExecutor com Redis broker
  Componentes: postgres, redis, webserver, scheduler, worker, triggerer, flower
  Scale workers: docker compose up -d --scale airflow-worker=N
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Segurança e Governança

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.1 Azure Key Vault + Service Principal OAuth2

# COMMAND ----------

print("""
SEGURANÇA — Padrão adotado em src/config/settings.py


  # NUNCA hardcode de credenciais
  # Todos os segredos via Azure Key Vault + Secret Scope Databricks

  client_id     = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
  tenant_id     = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
  client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
  storage_acct  = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")
  sql_conn      = dbutils.secrets.get(scope="kv-case-santander", key="sql-connection-string")

  # Autenticação ADLS: Service Principal OAuth2
  spark.conf.set(f"fs.azure.account.auth.type.{storage}.dfs.core.windows.net", "OAuth")
  spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage}.dfs.core.windows.net",
                 client_id)

  Benefícios:
     Rotação de chaves sem alterar código
     Auditoria de acesso no Key Vault (quem acessou, quando, qual chave)
     Permissões mínimas via RBAC Azure (princípio do menor privilégio)
     Service Principal: sem senha humana no pipeline (não expira com saída de funcionário)
     OAuth2 token com TTL curto (não persiste credencial em memória)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.2 Unity Catalog — Estrutura de Governança

# COMMAND ----------

# MAGIC %sql
-- Estrutura completa do catálogo
SELECT
    table_catalog AS catalogo,
    table_schema  AS camada,
    table_name    AS tabela,
    table_type
FROM information_schema.tables
WHERE table_catalog = 'case_santander'
ORDER BY
    CASE table_schema WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
    table_name;

# COMMAND ----------

# MAGIC %sql
-- Contagem total de registros por camada (panorama do pipeline)
SELECT camada, SUM(registros) AS total_registros FROM (
    SELECT 'bronze' AS camada, COUNT(*) AS registros FROM case_santander.bronze.acoes     UNION ALL
    SELECT 'bronze',           COUNT(*) FROM case_santander.bronze.bcb                     UNION ALL
    SELECT 'bronze',           COUNT(*) FROM case_santander.bronze.world_bank               UNION ALL
    SELECT 'bronze',           COUNT(*) FROM case_santander.bronze.clientes                 UNION ALL
    SELECT 'bronze',           COUNT(*) FROM case_santander.bronze.ordens                   UNION ALL
    SELECT 'bronze',           COUNT(*) FROM case_santander.bronze.kafka                    UNION ALL
    SELECT 'silver',           COUNT(*) FROM case_santander.silver.acoes                    UNION ALL
    SELECT 'silver',           COUNT(*) FROM case_santander.silver.bcb                      UNION ALL
    SELECT 'silver',           COUNT(*) FROM case_santander.silver.clientes                 UNION ALL
    SELECT 'silver',           COUNT(*) FROM case_santander.silver.ordens                   UNION ALL
    SELECT 'silver',           COUNT(*) FROM case_santander.silver.streaming                UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.anomalias                  UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.performance_acoes          UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.score_risco_clientes       UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.deteccao_fraude            UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.fraude_streaming           UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.anomalias_intraday         UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.volume_intraday            UNION ALL
    SELECT 'gold',             COUNT(*) FROM case_santander.gold.ranking_acoes_realtime
)
GROUP BY camada
ORDER BY CASE camada WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Q&A — Perguntas Técnicas da Banca
# MAGIC
# MAGIC > Esta seção antecipa as perguntas mais prováveis para um painel técnico de engenharia de dados num banco.

# COMMAND ----------
# MAGIC %md
# MAGIC ###  Arquitetura e Design

# COMMAND ----------
# MAGIC %md
# MAGIC **P1: Por que Medallion Architecture? Não bastaria uma tabela grande?**
# MAGIC
# MAGIC Três razões que se aplicam diretamente a um banco:
# MAGIC 1. **Auditoria regulatória** — Bronze é o espelho fiel da fonte original. Se a BCB API retornar dados errados, reprocessamos Silver e Gold sem precisar reextrair. Dados históricos são preservados.
# MAGIC 2. **Separação de responsabilidades** — Analistas consultam Gold (dado pronto). Só engenharia toca Bronze/Silver. Queries Gold são 3-5x mais baratas em Databricks por menor volume e ZORDER aplicado.
# MAGIC 3. **Controle de qualidade incremental** — Cada camada tem um critério de qualidade diferente. Bronze aceita qualquer dado. Silver rejeita nulos e aplica tipos. Gold exige joins consistentes e regras de negócio. Problemas são isolados por camada.

# COMMAND ----------
# MAGIC %md
# MAGIC **P2: Como escalaria este pipeline para 100 corretoras com 100x mais volume?**
# MAGIC
# MAGIC Sem mudança de arquitetura — apenas configuração:
# MAGIC 1. **Auto Loader** já usa notificações do Azure Storage (não lista diretório). Escala a bilhões de arquivos sem alterar código.
# MAGIC 2. **Databricks auto-scaling** — aumentar `max_workers` no cluster definition. Delta Lake suporta escritas concorrentes por ACID transactions.
# MAGIC 3. **Liquid Clustering** já implementado — rebalanceamento automático conforme volume cresce.
# MAGIC 4. **Streaming particionado** — adicionar `partitionBy("ticker")` no writeStream para paralelizar por ativo.
# MAGIC 5. **Broadcast Join** continua eficiente até ~10MB. Acima disso, migraria para bucketing + sort-merge com colunas pré-ordenadas no Delta.

# COMMAND ----------
# MAGIC %md
# MAGIC **P3: Por que Z-Score e não um modelo de Machine Learning para detecção de anomalias?**
# MAGIC
# MAGIC Escolha intencional por **interpretabilidade e auditabilidade**:
# MAGIC - Compliance bancário exige explicação: *"Esta ordem foi flagada porque z=3.2, ou seja, 3.2 desvios-padrão acima da média histórica deste ticker"*
# MAGIC - Z-Score é determinístico, versionável, reproduzível — essencial para auditoria do Banco Central
# MAGIC - Um modelo ML exigiria MLflow, versionamento, monitoramento de drift — complexidade sem ganho claro para este padrão de detecção
# MAGIC - **Próximo passo natural**: usar Z-Score como *feature* em um XGBoost de fraude, junto com score de risco e comportamento histórico

# COMMAND ----------
# MAGIC %md
# MAGIC ###  Performance e Otimização

# COMMAND ----------
# MAGIC %md
# MAGIC **P4: Explique Liquid Clustering vs. `partitionBy` tradicional.**
# MAGIC
# MAGIC ```
# MAGIC partitionBy("ano", "mes"):                Liquid Clustering (CLUSTER BY ticker, ano, mes):
# MAGIC   Estrutura física fixa.                    Databricks reorganiza incrementalmente via OPTIMIZE.
# MAGIC   Bronze → Silver → Gold                    Sem reescrita da tabela.
# MAGIC   Mudar estratégia = reescrever tudo.       Mudar estratégia = só rodar OPTIMIZE novamente.
# MAGIC
# MAGIC   Problema com streaming:                   Vantagem para streaming:
# MAGIC   Cada micro-batch cria arquivos novos      OPTIMIZE compacta small files automaticamente
# MAGIC   dentro das partições → "small files"      sem criar partições estáticas desequilibradas
# MAGIC   → queries lentas por excesso de I/O
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC **P5: Quando NÃO usaria Broadcast Join?**
# MAGIC
# MAGIC - Tabela de referência > 10-15 MB → risco de OOM nos executors (padrão Spark: `spark.sql.autoBroadcastJoinThreshold = 10MB`)
# MAGIC - Ambas as tabelas grandes → usar sort-merge join com bucketing e Delta
# MAGIC - Joins frequentes em streaming com estado → considerar `state store` do Structured Streaming
# MAGIC
# MAGIC **Neste case:** `df_score` (~7.900 × 4 colunas ≈ 400KB) e `df_perf` (9 × 8 colunas ≈ 1KB) são trivialmente broadcasted.

# COMMAND ----------
# MAGIC %md
# MAGIC ###  Streaming e Tempo Real

# COMMAND ----------
# MAGIC %md
# MAGIC **P6: Qual a latência real do pipeline de streaming, do Event Hub até gold.fraude_streaming?**
# MAGIC
# MAGIC | Etapa | Latência |
# MAGIC |-------|----------|
# MAGIC | Produtor → Event Hub | < 100ms |
# MAGIC | Auto Loader detectar arquivo | ~5-30s (notificação Azure Storage) |
# MAGIC | `trigger(availableNow=True)` processar batch | ~30-60s |
# MAGIC | Escrever bronze.kafka → silver.streaming | ~30s |
# MAGIC | CDC + processar streaming gold (4 análises) | ~60s |
# MAGIC | **Total** | **~3-5 minutos** |
# MAGIC
# MAGIC Para latência < 30s: migrar para `trigger(processingTime="30s")` — cluster sempre ativo, custo ~10x maior.

# COMMAND ----------
# MAGIC %md
# MAGIC **P7: O que acontece se o pipeline falhar no meio da execução?**
# MAGIC
# MAGIC **Exactly-once semantics** via checkpointing em 3 níveis:
# MAGIC 1. **Auto Loader** — registra no checkpoint quais arquivos já foram processados. Na reexecução, pula arquivos já commitados.
# MAGIC 2. **Delta Lake** — escreve atomicamente via write-ahead log. Um commit ou rollback completo; nunca estado parcial.
# MAGIC 3. **CDC** — `startingVersion` garante que o job de streaming gold só processa versões novas; não reprocessa o que já foi.
# MAGIC
# MAGIC Resultado: **sem dados duplicados, sem dados perdidos** após crash, OOM ou timeout.

# COMMAND ----------
# MAGIC %md
# MAGIC ###  Conformidade e LGPD

# COMMAND ----------
# MAGIC %md
# MAGIC **P8: Como implementaria o "direito ao esquecimento" da LGPD?**
# MAGIC
# MAGIC ```python
# MAGIC # 1. Derivar o hash do CPF/CustomerID solicitado
# MAGIC hash_alvo = hashlib.sha256(str(cpf_solicitado).encode()).hexdigest()[:16]
# MAGIC
# MAGIC # 2. DELETE em cascata em todas as tabelas que contêm hash_cliente
# MAGIC tabelas = ['silver.clientes', 'silver.clientes_scd', 'silver.ordens',
# MAGIC            'gold.score_risco_clientes', 'gold.score_risco_scd',
# MAGIC            'gold.deteccao_fraude', 'gold.posicao_clientes']
# MAGIC for t in tabelas:
# MAGIC     spark.sql(f"DELETE FROM case_santander.{t} WHERE hash_cliente = '{hash_alvo}'")
# MAGIC
# MAGIC # 3. VACUUM para remoção física dos arquivos Parquet com dados deletados
# MAGIC for t in tabelas:
# MAGIC     spark.sql(f"VACUUM case_santander.{t} RETAIN 0 HOURS")  # requer override de segurança
# MAGIC
# MAGIC # 4. Bronze: lifecycle policy de 30 dias no Azure ADLS
# MAGIC #    O dado bruto é removido automaticamente sem intervenção manual
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC **P9: Por que SCD Type 2 e não simplesmente UPDATE na tabela de clientes?**
# MAGIC
# MAGIC Três razões:
# MAGIC 1. **Regulatório** — CVM exige rastreabilidade de mudanças de suitability. UPDATE apaga o histórico.
# MAGIC 2. **Análises retroativas** — *"Qual era o perfil de risco deste cliente quando fez esta ordem em março de 2023?"* Com SCD Type 2, é uma query de filtro por data. Sem SCD, é impossível.
# MAGIC 3. **Detecção de comportamento suspeito** — Clientes que mudam de Conservador para Arrojado em < 30 dias são candidatos a revisão de compliance. Sem histórico, esse padrão é invisível.

# COMMAND ----------
# MAGIC %md
# MAGIC ###  Design Aberto

# COMMAND ----------
# MAGIC %md
# MAGIC **P10: Se você tivesse mais 1 semana, o que adicionaria?**
# MAGIC
# MAGIC Por prioridade de valor para o negócio:
# MAGIC
# MAGIC 1. **Modelo ML de Churn** com MLflow
# MAGIC    - Features: `gold.score_risco_clientes` + `gold.posicao_clientes` + `gold.deteccao_fraude`
# MAGIC    - Modelo: XGBoost com cross-validation, registrado no MLflow Model Registry
# MAGIC    - Saída: `gold.predicao_churn` com probabilidade e segmento de intervenção
# MAGIC
# MAGIC 2. **Alertas operacionais** via Databricks SQL Alerts
# MAGIC    - Email/Slack se `qualidade_pct < 99%` em tabela crítica
# MAGIC    - Alerta se `COUNT(nivel_risco_fraude = 'Critico') > threshold` num dia
# MAGIC    - Dashboard executivo em Databricks SQL com refresh automático
# MAGIC
# MAGIC 3. **Feature Store** no Unity Catalog
# MAGIC    - Centralizar as features de `score_risco`, `posicao_clientes`, `variacao_diaria_pct`
# MAGIC    - Reutilizável por múltiplos modelos sem recomputação

# COMMAND ----------
# MAGIC %md
# MAGIC **P11: Qual foi o maior desafio técnico deste case?**
# MAGIC
# MAGIC **Consistência entre streaming e batch na camada Gold.**
# MAGIC
# MAGIC O `job_streaming_to_gold.py` faz um `broadcast(df_perf)` de `gold.performance_acoes` (calculado pelo job batch diário) para detectar desvios históricos nas transações streaming. O desafio: se o streaming Gold rodar antes do batch Gold, o `df_perf` estará desatualizado.
# MAGIC
# MAGIC Solução: **dependência explícita no Databricks Workflow** — `t10_streaming_gold` tem `depends_on: t3_gold`. Assim o broadcast sempre usa os dados frescos do dia atual, não os do dia anterior.
# MAGIC
# MAGIC Segunda consequência: o Airflow DAG replica exatamente essa dependência via `t3_gold >> t10_streaming_gold`.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo — Cobertura Técnica do Case

# COMMAND ----------

print("""

          CASE SANTANDER — COBERTURA TÉCNICA COMPLETA                           

                                                                                  
  INGESTÃO               5 fontes: Yahoo Finance, BCB, WorldBank, Kaggle, Kafka  
  ARQUITETURA            Medallion (Bronze/Silver/Gold) + Unity Catalog           
  STREAMING              Auto Loader + Structured Streaming + CDC                 
  ANALYTICS              Z-Score, Score Ponderado 40/20/20/20, 4 Regras Fraude   
  SCD TYPE 2             MERGE Delta para auditoria regulatória de mudanças       
  OBSERVABILIDADE        Qualidade automatizada de 14 tabelas + Lakehouse Monitor 
  ORQUESTRAÇÃO           Databricks Workflow (prod) + Airflow + Docker (dev)      
  CI/CD                  GitHub Actions: quality→security→test→build→deploy       
  SEGURANÇA              Azure Key Vault + Service Principal OAuth2               
  LGPD                   SHA-256 + mascaramento + retenção 30d Bronze             
  GOVERNANÇA             Unity Catalog + RBAC + comentários em tabelas            
  PERFORMANCE            Liquid Clustering + Broadcast Join + ZORDER              
                                                                                  
  TECNOLOGIAS:                                                                    
    Azure Databricks 15.4 LTS  · Apache Spark 3.5.0  · Delta Lake                
    Azure Event Hub (Kafka)    · ADLS Gen2            · Azure SQL Database        
    Apache Airflow             · Docker               · GitHub Actions            
    Unity Catalog              · Azure Key Vault      · Python 3.11               

""")
