# Referência Técnica — Case Santander Data Master

## Índice

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Estrutura de Módulos](#2-estrutura-de-módulos)
3. [Referência de Funções](#3-referência-de-funções)
4. [Dicionário de Dados](#4-dicionário-de-dados)
5. [Algoritmos e Fórmulas](#5-algoritmos-e-fórmulas)
6. [APIs Externas Utilizadas](#6-apis-externas-utilizadas)
7. [Configuração e Dependências](#7-configuração-e-dependências)
8. [Guia de Desenvolvimento](#8-guia-de-desenvolvimento)
9. [Monitoramento e Alertas](#9-monitoramento-e-alertas)
10. [SDC Type 2](#10-sdc-type-2)
11. [LGPD — Práticas Adotadas](#11-lgpd--práticas-adotadas)
12. [Docker e Apache Airflow](#12-docker-e-apache-airflow)
13. [CI/CD — Multi-Ambiente](#13-cicd--multi-ambiente)
14. [Databricks Genie AI](#14-databricks-genie-ai)
15. [Lakehouse Monitoring](#15-lakehouse-monitoring)

---

## 1. Visão Geral da Arquitetura

### Fluxo de Dados (Medallion Architecture)

```
BRONZE (Dados Brutos)
  └── Parquet/Delta particionado por data de extração
  └── Sem transformações — fidelidade total à fonte
  └── Retenção: 30 dias (lifecycle policy ADLS)

SILVER (Dados Curados)
  └── Delta Lake com schema validado e mergeSchema
  └── Tipagem correta, nulos removidos, duplicatas eliminadas
  └── Features derivadas (variação %, ano, mês, trimestre)
  └── Particionamento otimizado para leitura

GOLD (Dados Analíticos)
  └── Delta Lake — tabelas de negócio
  └── Agregações, scores, alertas, SDC Type 2
  └── Prontas para consumo por dashboards, SQL e Genie AI
```

### Componentes de Plataforma

| Serviço | Nome no Projeto | Função |
|---|---|---|
| Azure ADLS Gen2 | `stcasesantander` | Data lake — Bronze/Silver/Gold |
| Azure Databricks | `dbw-case-santander` | Compute — Spark ETL e analytics |
| Azure Data Factory | `adf-case-santander` | Orquestração batch (05:00 diário) |
| Azure Event Hub | `evhcasesantander` | Streaming de transações financeiras |
| Azure Key Vault | `kv-case-santander` | Gestão de segredos e credenciais |
| Azure SQL Database | `sqldb-case-santander` | Serving layer para dashboards |
| Unity Catalog | `case_santander` | Governança e catálogo de dados |
| GitHub Actions | `.github/workflows/ci-cd.yml` | CI/CD multi-ambiente (dev/hk/prod) |
| Apache Airflow | `docker/docker-compose.yml` | Orquestração local via Docker |
| Databricks Genie | Space: Corretora Santander | Agente conversacional IA |
| Lakehouse Monitoring | Unity Catalog Quality | Monitoramento automático de qualidade |

---

## 2. Estrutura de Módulos

```
src/
├── config/
│   └── settings.py              # Configuração central: paths, credenciais, Spark
├── ingestion/
│   ├── yahoo_finance.py         # Extração de ações da B3 via yfinance
│   ├── bcb.py                   # Extração de indicadores do Banco Central
│   └── world_bank.py            # Extração de dados macroeconômicos
├── transformation/
│   ├── silver_acoes.py          # Bronze → Silver: ações
│   ├── silver_bcb.py            # Bronze → Silver: indicadores BCB
│   └── silver_world_bank.py     # Bronze → Silver: World Bank
├── gold/
│   ├── anomalias.py             # Detecção de anomalias por Z-Score
│   ├── fraude.py                # Motor de detecção de fraudes
│   └── performance.py           # Métricas de performance por ação/setor
├── clients/
│   └── sdc.py                   # Slowly Changing Dimensions Type 2
├── observability/
│   └── monitoring.py            # Qualidade de dados e alertas via Unity Catalog
└── streaming/
    └── __init__.py              # Placeholder streaming

jobs/
├── job_unity_catalog.py         # t0 — Registra tabelas Bronze no Unity Catalog
├── job_extracao.py              # t1 — Orquestra todas as ingestões
├── job_silver.py                # t2 — Orquestra todas as transformações Silver
├── job_corretora_analises.py    # t7 — Posição, score de risco, fraude, SQL
├── job_sdc.py                   # t9 — SDC Type 2 clientes e score risco
├── job_gold.py                  # t3 — Orquestra geração das tabelas Gold
├── job_observabilidade.py       # t4 — Orquestra monitoramento de qualidade
├── job_streaming.py             # t5 — Structured Streaming via arquivos Delta
├── job_clientes_ordens.py       # t6 — Ingestão Kaggle + ordens simuladas
├── job_lakehouse_monitoring.py  # t8 — Lakehouse Monitoring via SDK
└── job_carga_sql.py             # t_sql — Carga das tabelas Gold no Azure SQL

dags/
└── dag_pipeline_santander.py    # DAG Airflow — orquestra via API Databricks

docker/
├── Dockerfile                   # Imagem Airflow customizada
└── docker-compose.yml           # Stack completa: postgres + webserver + scheduler
```

---

## 3. Referência de Funções

### `src/config/settings.py`

#### `get_paths(storage_account)`

Retorna dicionário com todos os caminhos ABFSS do data lake.

```python
paths = get_paths("stcasesantander")
# paths["bronze_acoes"]    → abfss://bronze@.../acoes/
# paths["silver_clientes"] → abfss://silver@.../clientes/
# paths["gold_anomalias"]  → abfss://gold@.../anomalias/
```

**Retorno:** `dict[str, str]` com ~16 paths mapeados.

#### `configure_adls(spark, storage_account, client_id, tenant_id, client_secret)`

Configura autenticação OAuth2 para acesso ao ADLS Gen2 via Service Principal.

**Parâmetros:**

- `spark` — SparkSession ativa
- `storage_account` — Nome da conta ADLS
- `client_id`, `tenant_id`, `client_secret` — Credenciais do Service Principal

**Efeito:** Configura as chaves `fs.azure.*` na SparkSession para acesso ao ADLS.

#### `get_spark()`

Cria e retorna uma SparkSession via Databricks Connect (uso local).

**Retorno:** `SparkSession`

#### `get_credentials(dbutils)`

Recupera todos os segredos do Azure Key Vault via `dbutils.secrets`.

**Parâmetros:**

- `dbutils` — dbutils do Databricks

**Retorno:** `dict` com client_id, tenant_id, client_secret, storage_account, sql_conn, kaggle_username, kaggle_key.

---

### `src/ingestion/yahoo_finance.py`

#### `extrair_acoes(spark, storage_account, acoes=ACOES)`

Extrai histórico de 2 anos das 9 ações B3 monitoradas.

**Ações monitoradas:**

```
PETR4.SA, VALE3.SA, ITUB4.SA, BBDC4.SA, ABEV3.SA,
MGLU3.SA, WEGE3.SA, BBAS3.SA, SANB11.SA
```

**Saída:** Parquet em `bronze/acoes/data={data_hoje}/` com colunas:

```
ticker, date, open, high, low, close, volume, data_extracao
```

**Retorno:** `int` — total de registros gravados.

---

### `src/ingestion/bcb.py`

#### `extrair_bcb(spark, storage_account)`

Extrai indicadores do Banco Central do Brasil via REST API.

**Indicadores:**

| Indicador | Código BCB | Frequência |
|---|---|---|
| Selic (taxa diária) | 11 | Diária |
| Câmbio USD/BRL | 1 | Diária |
| IPCA (inflação) | 433 | Mensal |

**Saída:** Parquet em `bronze/bcb/extracao={data_hoje}/` com colunas:

```
data, valor, indicador, data_extracao
```

> **Nota:** A partição usa `extracao=` (não `data=`) para evitar conflito com a coluna `data` dos indicadores.

**Retorno:** `int` — total de registros gravados.

---

### `src/ingestion/world_bank.py`

#### `extrair_world_bank(spark, storage_account)`

Extrai indicadores macroeconômicos do Brasil via World Bank REST API.

**Indicadores:**

| Indicador | Código | Frequência |
|---|---|---|
| Crescimento do PIB | NY.GDP.MKTP.KD.ZG | Anual |
| Taxa de desemprego | SL.UEM.TOTL.ZS | Anual |

**Saída:** Parquet em `bronze/world_bank/extracao={data_hoje}/` com colunas:

```
ano, valor, indicador, data_extracao, fonte
```

**Retorno:** `int` — total de registros gravados (0 se API retornar vazio).

---

### `src/transformation/silver_acoes.py`

#### `transformar_acoes(spark, storage_account)`

Transforma dados Bronze de ações para Silver com Delta Lake.

**Transformações aplicadas:**

- Cast de tipos (`date` → `DateType`, preços → `DoubleType`)
- Filtro de nulos em `close` e zeros em `volume`
- `variacao_diaria_pct`: `(close - open) / open * 100`
- `amplitude_diaria`: `high - low`
- Colunas temporais: `ano`, `mes`, `trimestre`
- `empresa`: mapeamento ticker → nome
- `setor`: mapeamento ticker → setor B3
- `dropDuplicates(["date", "ticker"])`

**Particionamento Delta:** `ano`, `mes`

**Retorno:** `int` — total de registros gravados.

---

### `src/transformation/silver_bcb.py`

#### `transformar_bcb(spark, storage_account)`

Transforma dados Bronze do BCB para Silver.

**Transformações:**

- Parse de data (formato `dd/MM/yyyy` → `DateType`)
- Cast de `valor` para `DoubleType` (6 casas decimais)
- Extração de `ano`, `mes`, `trimestre`
- `dropDuplicates(["data", "indicador"])`

**Particionamento Delta:** `indicador`, `ano`

---

### `src/transformation/silver_world_bank.py`

#### `transformar_world_bank(spark, storage_account)`

Transforma dados Bronze do World Bank para Silver.

**Transformações:**

- Compatível com schema antigo (`data`) e novo (`ano`)
- Cast para `integer`
- `mergeSchema: true` para compatibilidade

**Retorno:** `int` — total de registros.

---

### `src/gold/anomalias.py`

#### `detectar_anomalias(spark, storage_account)`

Detecta movimentos anômalos de mercado usando Z-Score por ação.

**Algoritmo:**

```
Para cada ação (Window.partitionBy("ticker")):
  media   = AVG(variacao_diaria_pct)
  desvio  = STDDEV(variacao_diaria_pct)
  zscore  = (variacao_diaria_pct - media) / desvio

Classificação:
  zscore >  2  → "Alta Anormal"
  zscore < -2  → "Queda Anormal"
  -2 ≤ z ≤ 2  → "Normal"
```

**Saída:** Delta em `gold/anomalias/`

- Total: ~4.524 registros
- Anomalias: ~5,31%

---

### `src/gold/fraude.py`

#### `detectar_fraude(spark)`

Avalia ordens via Unity Catalog com 4 regras de risco.

**Regras:**

| Regra | Critério | Coluna |
|---|---|---|
| 1 | `valor_total > limite_operacional` | `alerta_valor_alto` |
| 2 | `quantidade > 9000` | `alerta_volume_suspeito` |
| 3 | `preco > 90 OR preco < 12` | `alerta_preco_atipico` |
| 4 | Conservador + `valor_total > 200k` | `alerta_perfil_incompativel` |

**Score final:**

```
0 alertas → "Normal"
1 alerta  → "Médio"
2 alertas → "Alto"
3+ alertas → "Crítico"
requer_revisao = (total_alertas >= 2)
```

**Saída:** `case_santander.gold.deteccao_fraude`

- Total: ~5.341 ordens
- Críticas: ~302 (6%)

---

### `src/gold/performance.py`

#### `calcular_performance(spark, storage_account)`

Calcula métricas agregadas de performance por ação, setor e ano.

**Métricas por (ticker, empresa, setor, ano):**

```
preco_medio, preco_minimo, preco_maximo
variacao_media_pct, volatilidade (STDDEV)
volume_medio, volume_total, dias_negociados
```

**Saída:** Delta em `gold/performance_acoes/`

---

### `src/clients/sdc.py`

#### `aplicar_sdc_type2(spark, df_novos, tabela_uc, chave)`

Aplica SCD Type 2 genérico em tabela Delta via Unity Catalog.

**Lógica:**

```
Se tabela existe (DeltaTable.forName):
  MERGE: registros com mesma chave e atual=true
    → data_fim = hoje, atual = False
  INSERT: novos registros com data_inicio=hoje, data_fim=9999-12-31, atual=True

Se tabela não existe:
  CREATE: primeira carga com campos SCD
```

#### `aplicar_sdc_clientes(spark)`

Aplica SCD Type 2 em `case_santander.silver.clientes_sdc`.

**Campos rastreados:** `perfil_risco`, `score_credito`, `faixa_saldo`, `churn`

#### `aplicar_sdc_score_risco(spark)`

Aplica SCD Type 2 em `case_santander.gold.score_risco_sdc`.

**Campos rastreados:** `score_risco`, `categoria_risco`, `limite_operacional`

---

### `src/observability/monitoring.py`

#### `monitorar_tabela(spark, tabela_uc)`

Monitora qualidade de uma tabela via Unity Catalog (sem path ADLS).

**Métricas coletadas:**

```
total_registros     → COUNT(*)
total_nulos         → SUM de nulos por coluna
total_duplicatas    → COUNT(*) - COUNT(DISTINCT *)
qualidade_pct       → (1 - nulos / (total * colunas)) * 100
tempo_seg           → duração da checagem
```

**Alertas:**

```
CRITICAL: qualidade_pct < 95%
WARNING:  duplicatas > 0
ERROR:    total_registros == 0
```

#### `executar_monitoramento(spark, storage_account=None)`

Executa checagem em 9 tabelas críticas do pipeline via Unity Catalog.

**Tabelas monitoradas:**

```
silver: acoes, bcb, world_bank, clientes, ordens
gold:   anomalias, posicao_clientes, score_risco_clientes, deteccao_fraude
```

---

### `jobs/job_corretora_analises.py`

Notebook 11 refatorado. Executa:

1. **Posição de carteira** → `gold.posicao_clientes`
2. **Score de risco** → `gold.score_risco_clientes`
3. **Perfil de clientes** → `gold.perfil_clientes`
4. **Ordens consolidadas** → `gold.ordens_consolidadas`
5. **Ranking ações** → `gold.ranking_acoes_perfil`
6. **Carga SQL** → tabelas dbo.* no Azure SQL Database

### `jobs/job_unity_catalog.py`

Notebook 07 refatorado. Registra todas as tabelas Bronze e Gold no Unity Catalog:

- Tabelas Bronze Parquet: acoes, bcb, world_bank, kafka
- Tabelas Bronze Delta: clientes, ordens
- Tabelas Gold Delta: performance_acoes, anomalias, acoes_vs_cambio

### `jobs/job_lakehouse_monitoring.py`

Notebook 12 refatorado. Cria/verifica monitores via `WorkspaceClient`:

```python
w.lakehouse_monitors.create(
    full_name=tabela,
    assets_dir=f"/Shared/monitoring/{tabela}",
    output_schema_name="case_santander.gold",
    snapshot={}
)
```

---

## 4. Dicionário de Dados

### Bronze: `acoes`

| Coluna | Tipo | Descrição |
|---|---|---|
| ticker | string | Código da ação (ex: PETR4.SA) |
| date | string | Data do pregão |
| open | double | Preço de abertura |
| high | double | Preço máximo do dia |
| low | double | Preço mínimo do dia |
| close | double | Preço de fechamento |
| volume | long | Volume negociado |
| data_extracao | string | Data de execução do job |

### Bronze: `clientes`

| Coluna | Tipo | Descrição |
|---|---|---|
| id_cliente | string | ID prefixado CLI + CustomerId |
| hash_cliente | string | SHA-256 do CustomerId (16 chars) — LGPD |
| sobrenome_masked | string | Primeira letra + asteriscos — LGPD |
| score_credito | int | Credit score (300-850) |
| pais | string | País do cliente |
| genero | string | Gênero |
| idade | int | Idade |
| anos_cliente | int | Anos como cliente |
| saldo | double | Saldo em conta |
| faixa_saldo | string | Sem saldo / Baixo / Medio / Alto |
| num_produtos | int | Número de produtos contratados |
| perfil_risco | string | Conservador / Moderado / Arrojado |
| ativo | boolean | Membro ativo |
| churn | boolean | Se saiu do banco |
| salario_estimado | double | Salário estimado |
| data_extracao | string | Data de extração |

### Bronze: `ordens`

| Coluna | Tipo | Descrição |
|---|---|---|
| id_ordem | string | ID único da ordem |
| hash_cliente | string | SHA-256 do cliente — LGPD |
| perfil_risco | string | Perfil do cliente |
| faixa_saldo | string | Faixa de saldo do cliente |
| ticker | string | Ação negociada |
| preco | double | Preço unitário |
| quantidade | long | Quantidade de ações |
| valor_total | double | Valor total da ordem |
| tipo | string | compra / venda |
| corretora | string | Santander Corretora |
| status | string | executada / cancelada / pendente |
| data_ordem | string | Data da ordem |
| data_extracao | string | Data de extração |

### Silver: `acoes`

| Coluna | Tipo | Descrição |
|---|---|---|
| ticker | string | Código da ação |
| date | date | Data do pregão |
| open / high / low / close | double | Preços OHLC |
| volume | long | Volume negociado |
| variacao_diaria_pct | double | (close - open) / open * 100 |
| amplitude_diaria | double | high - low |
| empresa | string | Nome da empresa |
| setor | string | Setor B3 (Financeiro, Commodities etc.) |
| ano / mes / trimestre | int | Decomposição temporal |
| data_processamento | string | Data do processamento |

### Silver: `bcb`

| Coluna | Tipo | Descrição |
|---|---|---|
| data | date | Data da observação |
| valor | double | Valor do indicador (6 casas) |
| indicador | string | selic / cambio_usd_brl / ipca |
| ano / mes / trimestre | int | Decomposição temporal |
| data_processamento | string | Data do processamento |

### Silver: `world_bank`

| Coluna | Tipo | Descrição |
|---|---|---|
| ano | int | Ano de referência |
| valor | double | Valor do indicador (4 casas) |
| indicador | string | pib_anual / desemprego |
| data_extracao | string | Data de extração |
| fonte | string | world_bank |
| data_processamento | string | Data do processamento |

### Silver: `clientes`

| Coluna | Tipo | Descrição |
|---|---|---|
| (colunas bronze clientes) | — | Herdadas do Bronze |
| faixa_etaria | string | Jovem / Adulto / Senior |
| score_categoria | string | Excelente / Bom / Regular / Ruim |
| data_processamento | string | Data do processamento |

### Silver: `ordens`

| Coluna | Tipo | Descrição |
|---|---|---|
| (colunas bronze ordens) | — | Herdadas do Bronze |
| data_ordem | date | Data da ordem (tipada) |
| ano | int | Ano da ordem |
| mes | int | Mês da ordem |
| data_processamento | string | Data do processamento |

### Silver: `streaming`

| Coluna | Tipo | Descrição |
|---|---|---|
| timestamp | timestamp | Timestamp da transação |
| ticker | string | Ação negociada |
| preco | double | Preço |
| quantidade | long | Quantidade |
| tipo | string | compra / venda |
| corretora | string | Corretora |
| id_transacao | string | ID único |
| hora / minuto | int | Decomposição temporal |
| valor_total | double | preco * quantidade |
| alerta_volume | string | Normal / Volume Medio / Volume Alto |
| alerta_preco | string | Normal / Preco Alto / Preco Baixo |
| processado_em | string | Timestamp de processamento |

### Silver: `clientes_sdc` (SDC Type 2)

| Coluna | Tipo | Descrição |
|---|---|---|
| (colunas silver clientes selecionadas) | — | Snapshot dos atributos |
| data_inicio | string | Início de vigência do registro |
| data_fim | string | Fim de vigência (9999-12-31 se atual) |
| atual | boolean | True se registro vigente |

### Gold: `anomalias`

| Coluna | Tipo | Descrição |
|---|---|---|
| date | date | Data do pregão |
| ticker | string | Código da ação |
| empresa | string | Nome da empresa |
| setor | string | Setor B3 |
| open / close | double | Preços |
| volume | long | Volume |
| variacao_diaria_pct | double | Variação % do dia |
| zscore | double | Z-Score da variação |
| anomalia | boolean | True se |zscore| > 2 |
| tipo_anomalia | string | Normal / Alta Anormal / Queda Anormal |
| data_processamento | string | Data do processamento |

### Gold: `posicao_clientes`

| Coluna | Tipo | Descrição |
|---|---|---|
| hash_cliente | string | ID pseudonimizado |
| ticker | string | Ação |
| quantidade_liquida | long | Compras - Vendas |
| total_comprado | double | Total comprado |
| total_vendido | double | Total vendido |
| valor_investido | double | total_comprado - total_vendido |
| resultado_estimado | double | total_vendido - total_comprado |
| total_ordens | long | Total de ordens |
| ordens_executadas | long | Ordens executadas |
| ordens_canceladas | long | Ordens canceladas |
| situacao | string | Comprado / Vendido a Descoberto / Zerado |
| perfil_risco | string | Perfil do cliente |
| faixa_saldo | string | Faixa de saldo |
| score_credito | int | Score de crédito |
| data_processamento | string | Data do processamento |

### Gold: `score_risco_clientes`

| Coluna | Tipo | Descrição |
|---|---|---|
| hash_cliente | string | ID pseudonimizado |
| perfil_risco | string | Conservador / Moderado / Arrojado |
| score_credito | int | Score de crédito do cliente |
| score_credito_norm | double | Score normalizado (0-100) |
| score_perfil | double | Pontuação por perfil |
| score_saldo | double | Pontuação por saldo |
| score_comportamento | double | Pontuação por comportamento |
| score_risco | double | Score final ponderado |
| categoria_risco | string | Baixo Risco / Risco Moderado / Risco Alto |
| limite_operacional | double | Limite em R$ |
| num_ativos | long | Número de ativos na carteira |
| total_ordens | long | Total de ordens |
| taxa_cancelamento_pct | double | % de ordens canceladas |
| data_processamento | string | Data do processamento |

### Gold: `score_risco_sdc` (SDC Type 2)

| Coluna | Tipo | Descrição |
|---|---|---|
| hash_cliente | string | ID pseudonimizado |
| score_risco | double | Score no período |
| categoria_risco | string | Categoria no período |
| limite_operacional | double | Limite no período |
| data_inicio | string | Início de vigência |
| data_fim | string | Fim de vigência |
| atual | boolean | True se vigente |

### Gold: `deteccao_fraude`

| Coluna | Tipo | Descrição |
|---|---|---|
| hash_cliente | string | ID pseudonimizado |
| ticker | string | Ação negociada |
| valor_total | double | Valor da ordem |
| quantidade | long | Quantidade |
| preco | double | Preço |
| perfil_risco | string | Perfil do cliente |
| score_risco | double | Score de risco |
| categoria_risco | string | Categoria de risco |
| limite_operacional | double | Limite operacional |
| alerta_valor_alto | boolean | Valor acima do limite |
| alerta_volume_suspeito | boolean | Quantidade > 9.000 |
| alerta_preco_atipico | boolean | Preço fora do range |
| alerta_perfil_incompativel | boolean | Conservador + valor alto |
| total_alertas | int | Soma dos alertas |
| score_fraude | string | Normal / Medio / Alto / Critico |
| requer_revisao | boolean | total_alertas >= 2 |
| data_processamento | string | Data do processamento |

### Gold: `perfil_clientes`

| Coluna | Tipo | Descrição |
|---|---|---|
| perfil_risco | string | Perfil de risco |
| faixa_etaria | string | Jovem / Adulto / Senior |
| score_categoria | string | Categoria do score |
| pais | string | País do cliente |
| total_clientes | long | Contagem |
| saldo_medio | double | Saldo médio |
| score_medio | double | Score médio |
| salario_medio | double | Salário estimado médio |
| total_churn | long | Total com churn |
| taxa_churn_pct | double | Taxa de churn % |

### Gold: `observabilidade`

| Coluna | Tipo | Descrição |
|---|---|---|
| camada | string | bronze / silver / gold |
| tabela | string | Nome da tabela monitorada |
| data_verificacao | string | Data da execução |
| total_registros | long | Contagem total |
| total_nulos | long | Total de valores nulos |
| total_duplicatas | long | Total de duplicatas |
| qualidade_pct | double | Score de qualidade [0, 100] |
| tempo_seg | double | Duração da checagem |

---

## 5. Algoritmos e Fórmulas

### Detecção de Anomalias (Z-Score)

```
Para cada ação i no ticker t (Window.partitionBy("ticker")):
  μ_t  = AVG(variacao_diaria_pct)
  σ_t  = STDDEV(variacao_diaria_pct)
  z_i  = (variacao_diaria_pct_i - μ_t) / σ_t

Critério (±2σ cobre ~95.4% da distribuição normal):
  z_i >  2  → "Alta Anormal"
  z_i < -2  → "Queda Anormal"
  |z_i| ≤ 2 → "Normal"
```

### Score de Risco de Clientes

```
score_credito_norm = score_credito / 850 * 100

score_perfil:
  Arrojado   → 100
  Moderado   → 60
  Conservador → 30

score_saldo:
  Alto    → 100
  Medio   → 60
  Baixo   → 30
  Sem saldo → 10

score_comportamento:
  taxa_cancelamento < 20% → 100
  taxa_cancelamento < 50% → 60
  taxa_cancelamento >= 50% → 20

score_risco = (score_credito_norm  * 0.4)
            + (score_perfil        * 0.2)
            + (score_saldo         * 0.2)
            + (score_comportamento * 0.2)

Categorização:
  score >= 70 → "Baixo Risco"    → limite R$ 500.000
  score >= 50 → "Risco Moderado" → limite R$ 200.000
  score >= 30 → "Risco Alto"     → limite R$  50.000
  score < 30  → "Risco Critico"  → limite R$  10.000
```

**Resultado observado (1.000 clientes):**

- Baixo Risco: 304 (30,4%)
- Risco Moderado: 534 (53,4%)
- Risco Alto: 162 (16,2%)

### Score de Qualidade de Dados

```
score_qualidade = (1 - total_nulos / (total_registros × num_colunas)) × 100

Alertas:
  score < 95%    → CRITICAL
  duplicatas > 0 → WARNING
  total == 0     → ERROR
```

### SDC Type 2 — Chaveamento Temporal

```
Para cada execução diária:
  1. MERGE: registros com mesma chave e atual=True
     → SET data_fim = hoje, atual = False
  2. INSERT: novo snapshot com
     → data_inicio = hoje
     → data_fim = '9999-12-31'
     → atual = True
```

---

## 6. APIs Externas Utilizadas

### 6.1 Yahoo Finance — `yfinance`

**Tipo:** Biblioteca Python  
**Autenticação:** Nenhuma  
**Arquivo:** `src/ingestion/yahoo_finance.py`

```python
import yfinance as yf
ticker = yf.Ticker("PETR4.SA")
df = ticker.history(period="2y")
```

| Parâmetro | Valor |
|---|---|
| `period` | `"2y"` — 2 anos de histórico |
| Ticker format | `"<CÓDIGO>.SA"` — sufixo B3 |

### 6.2 Banco Central do Brasil — SGS

**Base URL:** `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`  
**Autenticação:** Nenhuma

| Indicador | Código | Endpoint |
|---|---|---|
| Selic | 11 | `.../bcdata.sgs.11/dados?formato=json&dataInicial=...` |
| Câmbio | 1 | `.../bcdata.sgs.1/dados?formato=json&dataInicial=...` |
| IPCA | 433 | `.../bcdata.sgs.433/dados?formato=json` |

> **Nota sobre particionamento:** Os dados BCB usam `extracao=` como partição (não `data=`) para evitar conflito com a coluna `data` dos indicadores.

### 6.3 World Bank — Data API v2

**Base URL:** `https://api.worldbank.org/v2/country/BR/indicator/{indicator}`  
**Autenticação:** Nenhuma

| Indicador | Código |
|---|---|
| PIB (crescimento anual %) | `NY.GDP.MKTP.KD.ZG` |
| Desemprego (% força de trabalho) | `SL.UEM.TOTL.ZS` |

**Tratamento de vazio:** Se API retornar dataset vazio, a função retorna 0 sem lançar exceção.

### 6.4 Kaggle — Dataset Bank Customer Churn

**Dataset:** `mathchi/churn-for-bank-customers`  
**Autenticação:** API Key via Key Vault (`kaggle-username`, `kaggle-key`)

```python
url = "https://www.kaggle.com/api/v1/datasets/download/mathchi/churn-for-bank-customers"
response = requests.get(url, auth=(kaggle_username, kaggle_key), stream=True)
```

**Dados:** 10.000 clientes bancários reais anonimizados com atributos de crédito, saldo e churn.

**LGPD aplicada na extração:**
- `CustomerId` → `hash_cliente` (SHA-256, 16 chars)
- `Surname` → `sobrenome_masked` (primeira letra + asteriscos)

### 6.5 Azure Event Hub — Kafka Protocol

**Namespace:** `evhcasesantander`  
**Event Hub:** `transacoes-financeiras`  
**Autenticação:** Connection String via Key Vault

**Structured Streaming via arquivos:**
- Producer grava Parquet em `bronze/kafka/`
- Consumer usa `readStream.format("parquet")` com `.trigger(availableNow=True)`
- Saída em `silver/streaming/` como Delta Lake

### 6.6 Azure Key Vault

**Secret Scope:** `kv-case-santander`

| Key | Uso |
|---|---|
| `client-id` | Service Principal — autenticação ADLS |
| `tenant-id` | Azure AD Tenant |
| `client-secret` | Credencial Service Principal |
| `storage-account` | Nome da conta ADLS Gen2 |
| `eventhub-connection-string` | Conexão Event Hub |
| `sql-connection-string` | Conexão Azure SQL Database |
| `kaggle-username` | Autenticação Kaggle API |
| `kaggle-key` | Chave Kaggle API |

---

## 7. Configuração e Dependências

### Variáveis em `config/config.py`

| Variável | Valor | Descrição |
|---|---|---|
| `STORAGE_ACCOUNT` | `stcasesantander` | Conta ADLS Gen2 |
| `EVENTHUB_NAME` | `transacoes-financeiras` | Event Hub |
| `SQL_SERVER` | `sqlsvr-case-santander.database.windows.net` | Azure SQL |
| `SQL_DATABASE` | `sqldb-case-santander` | Banco de dados |
| `CATALOG` | `case_santander` | Unity Catalog |
| `ACOES` | Lista de 9 tickers | Ações B3 monitoradas |

### Cluster Databricks

| Propriedade | Valor |
|---|---|
| Nome | `cluster-case-santander` |
| Runtime | 15.4 LTS (Spark 3.5.0, Scala 2.12) |
| Node | Standard_D4pds_v6 (16 GB, 4 Cores) |
| Auto-termination | 20 minutos |
| Cluster ID | `0401-150803-wefgy1hc` |

**Configuração Spark (Advanced):**

```
spark.databricks.delta.schema.autoMerge.enabled true
spark.hadoop.fs.azure.account.auth.type.stcasesantander.dfs.core.windows.net OAuth
spark.hadoop.fs.azure.account.oauth.provider.type.stcasesantander.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.id.stcasesantander.dfs.core.windows.net {{secrets/kv-case-santander/client-id}}
spark.hadoop.fs.azure.account.oauth2.client.secret.stcasesantander.dfs.core.windows.net {{secrets/kv-case-santander/client-secret}}
spark.hadoop.fs.azure.account.oauth2.client.endpoint.stcasesantander.dfs.core.windows.net https://login.microsoftonline.com/{tenant_id}/oauth2/token
```

**Bibliotecas instaladas no cluster:**

```
yfinance, requests, azure-eventhub
```

### `requirements.txt`

```
yfinance>=0.2.37
requests>=2.31.0
azure-eventhub>=5.15.1
databricks-connect==15.4
databricks-sdk>=0.20.0
pytest>=7.4.0
```

### `requirements-airflow.txt`

```
apache-airflow-providers-databricks==4.7.0
databricks-sdk>=0.20.0
requests>=2.31.0
```

---

## 8. Guia de Desenvolvimento

### Setup Local com Databricks Connect

```bash
# Clonar repositório
git clone https://github.com/thediegoaccount/case-santander-data-master.git
cd case-santander-data-master

# Criar ambiente virtual
python3 -m venv ~/.venv/databricks
source ~/.venv/databricks/bin/activate

# Instalar dependências
pip install -r requirements.txt

# Configurar Databricks Connect
databricks configure --token
# Host: https://adb-7405606224366149.9.azuredatabricks.net
# Token: <seu_token>

# Adicionar cluster ID ao config
echo "cluster_id = 0401-150803-wefgy1hc" >> ~/.databrickscfg

# Testar conexão
databricks-connect test

# Rodar testes
pytest tests/ -v
```

### Estrutura de Branches

```
main       → produção (protegida — PR + aprovação + CI obrigatórios)
develop    → integração (deploy automático para dev)
feature/*  → novas funcionalidades (PR para develop ou main)
```

### Fluxo de Desenvolvimento

```
1. git checkout -b feature/minha-feature
2. Edite os .py no VSCode
3. Teste localmente: pytest tests/ -v
4. Teste via Databricks Connect (SparkSession remota)
5. git push origin feature/minha-feature
6. Abra PR no GitHub → CI valida automaticamente
7. Merge → CI/CD faz deploy para o ambiente correspondente
8. Databricks Workspace sincroniza via Git folder
```

### Adicionando uma Nova Ação Monitorada

1. Adicione o ticker em `ACOES` em `src/config/settings.py`
2. Adicione mapeamento nome/setor em `transformar_acoes()` (`src/transformation/silver_acoes.py`)
3. Execute: `pytest tests/ -v`
4. Abra PR com a mudança

### Adicionando um Novo Indicador BCB

1. Consulte código da série em `bcb.gov.br/dados/serie`
2. Adicione a função `buscar_diario()` ou `buscar_mensal()` em `extrair_bcb()` (`src/ingestion/bcb.py`)
3. Atualize transformação em `transformar_bcb()` se necessário

### Adicionando uma Nova Regra de Fraude

1. Crie coluna de alerta em `detectar_fraude()` (`src/gold/fraude.py`)
2. Inclua na soma `total_alertas`
3. Atualize o dicionário de dados neste documento

### Executando Jobs Manualmente

```bash
# Via Databricks CLI
databricks jobs run-now --job-id 604800593989824

# Via UI
pipeline-case-santander → Run now → selecionar tasks

# Via Airflow (Docker local)
http://localhost:8080 → DAGs → pipeline_corretora_santander → Trigger DAG
```

---

## 9. Monitoramento e Alertas

### Tabelas Monitoradas

| Tabela | Threshold Mínimo | Qualidade Esperada |
|---|---|---|
| `silver.acoes` | ≥ 4.000 registros | ≥ 95% |
| `silver.bcb` | ≥ 3.000 registros | ≥ 95% |
| `silver.world_bank` | ≥ 50 registros | ≥ 87% (nulos esperados) |
| `silver.clientes` | ≥ 10.000 registros | 100% |
| `silver.ordens` | ≥ 5.000 registros | 100% |
| `gold.anomalias` | ≥ 4.000 registros | 100% |
| `gold.posicao_clientes` | ≥ 3.000 registros | 100% |
| `gold.score_risco_clientes` | ≥ 1.000 registros | 100% |
| `gold.deteccao_fraude` | ≥ 5.000 registros | 100% |

### Lakehouse Monitoring

6 tabelas com perfil automático via Databricks Lakehouse Monitoring:

```
gold.anomalias
gold.posicao_clientes
gold.score_risco_clientes
gold.deteccao_fraude
silver.clientes
silver.ordens
```

- Dashboard de qualidade gerado automaticamente por tabela
- Anomaly detection habilitado no schema gold
- Data profiling para métricas por coluna

### Consultar Histórico de Qualidade

```sql
SELECT
  camada,
  tabela,
  qualidade_pct,
  total_registros,
  total_nulos,
  total_duplicatas,
  data_verificacao
FROM case_santander.gold.observabilidade
ORDER BY data_verificacao DESC, qualidade_pct ASC;
```

---

## 10. SDC Type 2

### Conceito

Slowly Changing Dimensions Type 2 mantém histórico completo de mudanças em atributos de clientes ao longo do tempo, permitindo auditorias e análises temporais.

### Tabelas SDC

| Tabela | Dimensão | Atributos Rastreados |
|---|---|---|
| `silver.clientes_sdc` | Clientes | perfil_risco, score_credito, faixa_saldo, churn |
| `gold.score_risco_sdc` | Score de Risco | score_risco, categoria_risco, limite_operacional |

### Campos de Controle

| Campo | Tipo | Descrição |
|---|---|---|
| `data_inicio` | string | Data de início de vigência do registro |
| `data_fim` | string | Data de fim (`9999-12-31` se registro atual) |
| `atual` | boolean | `True` se registro vigente |

### Exemplo de Consulta Histórica

```sql
-- Histórico de mudança de perfil de um cliente
SELECT hash_cliente, perfil_risco, data_inicio, data_fim, atual
FROM case_santander.silver.clientes_sdc
WHERE hash_cliente = 'abc123...'
ORDER BY data_inicio;

-- Clientes que mudaram de perfil
SELECT hash_cliente, COUNT(*) as num_mudancas
FROM case_santander.silver.clientes_sdc
GROUP BY hash_cliente
HAVING COUNT(*) > 1
ORDER BY num_mudancas DESC;
```

---

## 11. LGPD — Práticas Adotadas

### Pseudonimização

| Campo Original | Técnica | Campo Resultante |
|---|---|---|
| `CustomerId` | Hash SHA-256 (16 chars) | `hash_cliente` |
| `Surname` | Primeira letra + asteriscos | `sobrenome_masked` |
| CPF (hipotético) | Mascaramento parcial | `cpf_masked` |
| Email (hipotético) | 2 chars + *** + domínio | `email_masked` |

### Controle de Acesso

- **Azure Key Vault:** Credenciais nunca expostas em código ou logs
- **Service Principal:** Princípio do menor privilégio (Storage Blob Data Contributor)
- **Unity Catalog:** Controle de acesso por schema e tabela
- **RBAC ADLS:** Acesso por container por camada

### Retenção de Dados

```
Bronze → Lifecycle Policy: deletar após 30 dias
Silver → Retenção: 90 dias (a configurar)
Gold   → Dados agregados: indefinido
```

### Separação Dado Analítico vs Transacional

- Dados analíticos usam `hash_cliente` (nunca CPF real)
- Dado transacional (sistema de origem) nunca exposto no pipeline
- De-para hash ↔ ID real disponível apenas em sistemas autorizados com log de auditoria

---

## 12. Docker e Apache Airflow

### Arquitetura

```
Docker Compose
├── postgres:13         → Banco de metadados do Airflow
├── airflow-webserver   → Interface web (localhost:8080)
├── airflow-scheduler   → Dispara as DAGs no horário
└── airflow-init        → Inicializa DB e cria usuário admin
```
### Papel do Airflow no projeto

O Airflow **não substitui** o Databricks Workflow — os dois coexistem com
responsabilidades distintas:

| Orquestrador | Ambiente | Trigger | Propósito |
|---|---|---|---|
| Databricks Workflow | Produção | Agendado 06:00 | Pipeline diário automatizado |
| Apache Airflow + Docker | Desenvolvimento | Manual | Testes, demonstração, multi-cloud |

Em produção, o **Databricks Workflow** é o único orquestrador ativo.
O **Airflow** demonstra como o pipeline seria orquestrado em um ambiente
externo ao Databricks — por exemplo, em uma empresa que já possui
infraestrutura Airflow ou que opera em multi-cloud.

Os **jobs Python são os mesmos** — nenhuma duplicação de código.
O Airflow chama os mesmos `jobs/*.py` via API REST do Databricks.

### Inicialização

```bash
# Inicializar (primeira vez)
docker compose -f docker/docker-compose.yml --env-file docker/.env up airflow-init

# Subir stack completa
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d

### Uso recorrente
```bash
# Subir (já inicializado anteriormente)
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d

# Verificar status dos containers
docker compose -f docker/docker-compose.yml --env-file docker/.env ps

# Ver logs em tempo real
docker compose -f docker/docker-compose.yml --env-file docker/.env logs -f

# Derrubar a stack
docker compose -f docker/docker-compose.yml --env-file docker/.env down
```

# Acessar
http://localhost:8080
Login: admin / admin
```

### Arquivo `.env` necessário

```
DATABRICKS_HOST=https://adb-7405606224366149.9.azuredatabricks.net
DATABRICKS_TOKEN=<seu_token>
```

### Configuração da Conexão Databricks no Airflow

```
Admin → Connections → Add Connection:
  Connection Id:   databricks_default
  Connection Type: Databricks
  Host:            https://adb-7405606224366149.9.azuredatabricks.net
  Password:        <token>
```

### DAG — `pipeline_corretora_santander`

- **Schedule:** `0 6 * * *` (06:00 diário, horário Brasília)
- **Retry:** 2 tentativas com delay de 5 minutos
- **Operator:** `DatabricksSubmitRunOperator` — chama jobs via API REST
- **Cluster:** `0401-150803-wefgy1hc` (cluster já existente)

**Dependências idênticas ao Databricks Workflow:**

```
t0 >> t1 >> [t5, t6]
t5 >> t2
t6 >> [t2, t7]
t7 >> t9
[t2, t9] >> t3
t3 >> [t8, t_sql]
[t8, t_sql] >> t4
```

---

## 13. CI/CD — Multi-Ambiente

### Ambientes GitHub

| Ambiente | Branch Trigger | Proteção | Deploy Path |
|---|---|---|---|
| `dev` | `develop` | Nenhuma (automático) | `/case-santander/dev` |
| `hk` | `main` | Required reviewer | `/case-santander/hk` |
| `prod` | `main` | Required reviewer + 5min timer | `/case-santander/prod` |

### Pipeline CI/CD

```yaml
ci:               → Roda testes + valida módulos
deploy-dev:       → Deploy em dev (apenas branch develop)
deploy-hk:        → Deploy em hk (main, após CI)
deploy-prod:      → Deploy em prod (main, após hk + timer 5min)
```

### Proteções da Branch Main

- Pull Request obrigatório
- 1 aprovação necessária
- CI (`Integracao Continua`) deve passar
- Force push bloqueado
- Ruleset: `protect-main`

### Deploy via CI

O CI/CD faz deploy dos módulos `src/` e `jobs/` para cada ambiente no Databricks Workspace:

```bash
databricks workspace mkdirs $DEPLOY_PATH/src/config
databricks workspace import "$f" "$DEPLOY_PATH/$f" --language PYTHON --overwrite
```

---

## 14. Databricks Genie AI

### Configuração

- **Space:** `Corretora Santander — Análise de Dados`
- **Warehouse:** Serverless Starter Warehouse
- **Tabelas conectadas:** Todas as Gold + silver.clientes + silver.ordens

### Instructions configuradas

```
Você é um assistente de análise de dados da Corretora Santander.
Responda sempre em português brasileiro.

Ao responder:
- Use linguagem executiva e objetiva
- Sempre apresente números e percentuais
- Destaque insights de negócio
- Alerte sobre riscos quando relevante
- Sugira ações quando apropriado
```

### Common Questions configuradas

```
"Quais clientes têm maior risco de fraude e qual o valor médio das ordens suspeitas?"
"Qual o score médio de risco por perfil?"
"Quais anomalias foram detectadas?"
"Compare performance das ações por setor"
"Qual a taxa de churn por perfil de cliente?"
"Mostre os clientes com score de fraude crítico"
"Qual ação teve maior queda anormal?"
"Quantos clientes são conservadores?"
```

---

## 15. Lakehouse Monitoring

### Tabelas com Monitor Ativo

```python
w.lakehouse_monitors.create(
    full_name="case_santander.gold.anomalias",
    assets_dir="/Shared/monitoring/case_santander/gold/anomalias",
    output_schema_name="case_santander.gold",
    snapshot={}
)
```

### Tabelas monitoradas

| Tabela | Status |
|---|---|
| `gold.anomalias` | ACTIVE |
| `gold.posicao_clientes` | ACTIVE |
| `gold.score_risco_clientes` | ACTIVE |
| `gold.deteccao_fraude` | ACTIVE |
| `silver.clientes` | ACTIVE |
| `silver.ordens` | ACTIVE |

### Funcionalidades

- **Snapshot Monitor:** Tira "foto" dos dados a cada execução
- **Data profiling:** Métricas por coluna (min, max, média, nulos, únicos)
- **Anomaly detection:** Detecta drift de dados e variações anômalas
- **Dashboard automático:** Gerado por tabela com gráficos de qualidade
- **View refresh history:** Histórico de execuções dos monitores

### Consultar Tabelas de Métricas Geradas

```sql
-- Métricas de perfil geradas automaticamente pelo monitor
SELECT * FROM case_santander.gold.anomalias_profile_metrics
ORDER BY window_start DESC
LIMIT 10;

-- Métricas de drift
SELECT * FROM case_santander.gold.anomalias_drift_metrics
ORDER BY window_start DESC;
```

---

*Documentação atualizada em 2026-04-05 — Case Santander Data Master*