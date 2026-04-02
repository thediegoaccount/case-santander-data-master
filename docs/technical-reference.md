# Referência Técnica — Case Santander Data Master

## Índice

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Estrutura de Módulos](#2-estrutura-de-módulos)
3. [Referência de Funções](#3-referência-de-funções)
4. [Dicionário de Dados](#4-dicionário-de-dados)
5. [Algoritmos e Fórmulas](#5-algoritmos-e-fórmulas)
6. [Configuração e Dependências](#6-configuração-e-dependências)
7. [Guia de Desenvolvimento](#7-guia-de-desenvolvimento)
8. [Monitoramento e Alertas](#8-monitoramento-e-alertas)

---

## 1. Visão Geral da Arquitetura

### Fluxo de Dados (Medallion Architecture)

```
BRONZE (Dados Brutos)
  └── Parquet particionado por data
  └── Sem transformações — fidelidade total à fonte
  └── Retenção: 30 dias (lifecycle policy)

SILVER (Dados Curados)
  └── Delta Lake com schema validado
  └── Tipagem correta, nulos removidos
  └── Features derivadas (variação %, ano, mês, trimestre)
  └── Particionamento otimizado para leitura

GOLD (Dados Analíticos)
  └── Delta Lake — tabelas de negócio
  └── Agregações, scores, alertas
  └── Prontas para consumo por dashboards e SQL
```

### Componentes de Plataforma

| Serviço | Nome no Projeto | Função |
|---|---|---|
| Azure ADLS Gen2 | `stcasesantander` | Data lake — camadas Bronze/Silver/Gold |
| Azure Databricks | `dbw-case-santander` | Compute — Spark ETL e analytics |
| Azure Data Factory | `adf-case-santander` | Orquestração batch (05:00 diário) |
| Azure Event Hub | `evhcasesantander` | Streaming de transações financeiras |
| Azure Key Vault | `kv-case-santander` | Gestão de segredos e credenciais |
| Azure SQL Database | `sqldb-case-santander` | Serving layer para dashboards |
| Unity Catalog | `case_santander` | Governança e catálogo de dados |
| GitHub Actions | `.github/workflows/ci-cd.yml` | CI/CD multi-ambiente |

---

## 2. Estrutura de Módulos

```
src/
├── config/
│   └── settings.py         # Configuração central: paths, credenciais, Spark
├── ingestion/
│   ├── yahoo_finance.py    # Extração de ações da B3 via yfinance
│   ├── bcb.py              # Extração de indicadores do Banco Central
│   └── world_bank.py       # Extração de dados macroeconômicos
├── transformation/
│   ├── silver_acoes.py     # Bronze → Silver: ações
│   ├── silver_bcb.py       # Bronze → Silver: indicadores BCB
│   └── silver_world_bank.py # Bronze → Silver: World Bank
├── gold/
│   ├── anomalias.py        # Detecção de anomalias por Z-Score
│   ├── fraude.py           # Motor de detecção de fraudes
│   └── performance.py      # Métricas de performance por ação/setor
├── clients/
│   └── sdc.py              # Slowly Changing Dimensions Type 2
├── observability/
│   └── monitoring.py       # Qualidade de dados e alertas
└── streaming/
    └── __init__.py         # Consumer Kafka/Event Hub (placeholder)

jobs/
├── job_extracao.py         # Orquestra todas as ingestões
├── job_silver.py           # Orquestra todas as transformações Silver
├── job_gold.py             # Orquestra geração das tabelas Gold
├── job_observabilidade.py  # Orquestra monitoramento de qualidade
└── job_sdc.py              # Orquestra aplicação de SDC Type 2
```

---

## 3. Referência de Funções

### `src/config/settings.py`

#### `get_paths(spark=None)`
Retorna dicionário com todos os caminhos ABFSS do data lake.

```python
paths = get_paths()
# paths["bronze_acoes"]      → abfss://bronze@.../acoes
# paths["silver_acoes"]      → abfss://silver@.../acoes
# paths["gold_anomalias"]    → abfss://gold@.../anomalias
# paths["observabilidade"]   → abfss://gold@.../observabilidade
```

**Retorno:** `dict[str, str]` com ~20 paths mapeados.

#### `configure_adls(spark)`
Configura autenticação OAuth2 para acesso ao ADLS Gen2 via Service Principal.

**Parâmetros:**
- `spark` — SparkSession ativa

**Efeito:** Configura as chaves `fs.azure.*` na SparkSession.

#### `get_spark()`
Cria e retorna uma SparkSession via Databricks Connect.

**Retorno:** `SparkSession`

#### `get_credentials(spark, secret_name)`
Recupera um segredo do Azure Key Vault via `dbutils.secrets`.

**Parâmetros:**
- `spark` — SparkSession com dbutils disponível
- `secret_name` — Nome do segredo no Key Vault

**Retorno:** `str` com o valor do segredo.

---

### `src/ingestion/yahoo_finance.py`

#### `extrair_acoes(spark, paths)`
Extrai histórico de 2 anos das 9 ações B3 monitoradas.

**Ações monitoradas:**
```
PETR4.SA, VALE3.SA, ITUB4.SA, BBDC4.SA, ABEV3.SA,
MGLU3.SA, WEGE3.SA, BBAS3.SA, SANB11.SA
```

**Parâmetros:**
- `spark` — SparkSession
- `paths` — dict de paths (de `get_paths()`)

**Saída:** Parquet em `paths["bronze_acoes"]` com colunas:
```
ticker, date, open, high, low, close, volume, adjclose, extraction_timestamp
```

---

### `src/ingestion/bcb.py`

#### `extrair_bcb(spark, paths)`
Extrai indicadores do Banco Central do Brasil via REST API (`bcb.gov.br/dados/serie`).

**Indicadores extraídos:**

| Indicador | Código BCB | Frequência |
|---|---|---|
| Selic (taxa diária) | 11 | Diária |
| Câmbio USD/BRL | 1 | Diária |
| IPCA (inflação) | 433 | Mensal |

**Saída:** Parquet em `paths["bronze_bcb"]` com colunas:
```
data, valor, indicador
```

---

### `src/ingestion/world_bank.py`

#### `extrair_world_bank(spark, paths)`
Extrai indicadores macroeconômicos do Brasil via World Bank REST API.

**Indicadores extraídos:**

| Indicador | Código | Frequência |
|---|---|---|
| Crescimento do PIB | NY.GDP.MKTP.KD.ZG | Anual |
| Taxa de desemprego | SL.UEM.TOTL.ZS | Anual |

**Saída:** Parquet em `paths["bronze_world_bank"]` com colunas:
```
data, valor, indicador
```

---

### `src/transformation/silver_acoes.py`

#### `transformar_acoes(spark, paths)`
Transforma dados Bronze de ações para Silver com Delta Lake.

**Transformações aplicadas:**
- Cast de tipos (`date` → `DateType`, preços/volume → `DoubleType`)
- Filtro de nulos em `close` e zeros em `volume`
- Coluna `variacao_pct`: `(close - open) / open * 100`
- Coluna `amplitude`: `high - low`
- Colunas temporais: `ano`, `mes`, `trimestre`
- Coluna `empresa`: mapeamento de ticker para nome da empresa
- Coluna `setor`: mapeamento de ticker para setor B3

**Mapeamento ticker → empresa:**
```python
{
    "PETR4": "Petrobras",
    "VALE3": "Vale",
    "ITUB4": "Itaú Unibanco",
    "BBDC4": "Bradesco",
    "ABEV3": "Ambev",
    "MGLU3": "Magazine Luiza",
    "WEGE3": "WEG",
    "BBAS3": "Banco do Brasil",
    "SANB11": "Santander BR"
}
```

**Particionamento Delta:** `ano`, `mes`

---

### `src/transformation/silver_bcb.py`

#### `transformar_bcb(spark, paths)`
Transforma dados Bronze do BCB para Silver.

**Transformações:**
- Parse de data (formato `dd/MM/yyyy` → `DateType`)
- Cast de `valor` para `DoubleType` com arredondamento (4 casas)
- Extração de `ano`, `mes`, `trimestre`
- Filtro de nulos em `data` e `valor`

**Particionamento Delta:** `indicador`, `ano`

---

### `src/transformation/silver_world_bank.py`

#### `transformar_world_bank(spark, paths)`
Transforma dados Bronze do World Bank para Silver.

**Transformações:**
- Conversão de ano (string) para data (`yyyy-01-01`)
- Cast de `valor` para `DoubleType` (4 casas decimais)
- Filtro de nulos

**Particionamento Delta:** `indicador`, `ano`

---

### `src/gold/anomalias.py`

#### `detectar_anomalias(spark, paths)`
Detecta movimentos anômalos de mercado usando Z-Score por ação.

**Algoritmo:**
```
Para cada ação (janela particionada por ticker):
  media    = AVG(variacao_pct)
  desvio   = STDDEV(variacao_pct)
  z_score  = (variacao_pct - media) / desvio

Classificação:
  z_score >  2  → "Alta Anormal"
  z_score < -2  → "Queda Anormal"
  -2 ≤ z ≤ 2   → "Normal"
```

**Colunas adicionadas:** `z_score`, `tipo_anomalia`, `is_anomalia`

**Saída:** Delta em `paths["gold_anomalias"]`
- Total: 4.014 registros
- Anomalias: 213 (5,31%)

---

### `src/gold/fraude.py`

#### `detectar_fraude(spark, paths)`
Avalia ordens de clientes com base em 4 regras de risco.

**Regras de detecção:**

| Regra | Critério | Coluna gerada |
|---|---|---|
| 1 | `valor_total > limite_operacional_cliente` | `alerta_limite` |
| 2 | `quantidade > 9000` | `alerta_volume` |
| 3 | `preco > 90 OR preco < 12` | `alerta_preco` |
| 4 | Perfil incompatível com operação | `alerta_perfil` |

**Score final:**
```
total_alertas = soma das 4 regras
score_fraude:
  0 alertas → "Normal"
  1 alerta  → "Médio"
  2 alertas → "Alto"
  3+ alertas → "Crítico"

requer_revisao = (total_alertas >= 2)
```

**Saída:** Delta em `paths["gold_deteccao_fraude"]`
- Total: 5.445 ordens
- Críticas: 302 (6%)

---

### `src/gold/performance.py`

#### `calcular_performance(spark, paths)`
Calcula métricas agregadas de performance por ação, setor e ano.

**Métricas calculadas por (ticker, setor, ano):**
```
preco_medio_fechamento   → AVG(close)
preco_minimo             → MIN(low)
preco_maximo             → MAX(high)
variacao_media_pct       → AVG(variacao_pct)
volatilidade             → STDDEV(variacao_pct)
volume_medio             → AVG(volume)
volume_total             → SUM(volume)
dias_negociados          → COUNT(*)
```

**Saída:** Delta em `paths["gold_performance"]`
- Total: 24 registros (9 ações × ~2-3 anos)

---

### `src/clients/sdc.py`

#### `aplicar_sdc_clientes(spark, paths)`
Aplica SCD Type 2 na dimensão de clientes.

**Lógica:**
```
Para cada cliente novo ou com atributos alterados:
  1. Registros existentes com mesmo id → data_fim = hoje, atual = False
  2. Novo registro inserido             → data_inicio = hoje, atual = True

Campos rastreados: nome, perfil, saldo, score_credito, churn
```

**Operação:** Delta Lake `MERGE` (upsert atômico)

#### `aplicar_sdc_score_risco(spark, paths)`
Aplica SCD Type 2 no histórico de scores de risco de clientes.

**Campos rastreados:** `score_risco`, `categoria_risco`, `limite_operacional`

---

### `src/observability/monitoring.py`

#### `executar_monitoramento(spark, paths)`
Executa checagem de qualidade em todas as tabelas críticas do pipeline.

**Tabelas monitoradas:**
```
silver: acoes, bcb, world_bank, clientes, ordens
gold:   anomalias, performance_acoes, deteccao_fraude,
        score_risco_clientes, observabilidade
```

**Métricas coletadas por tabela:**
```
total_registros     → COUNT(*)
nulos_por_coluna    → COUNT(null) por cada coluna
duplicatas          → COUNT(*) - COUNT(DISTINCT *)
score_qualidade     → (1 - nulos / (total * num_colunas)) * 100
tempo_processamento → duração da checagem
```

**Regras de alerta:**
```
CRITICAL : score_qualidade < 95%
WARNING  : duplicatas > 0
ERROR    : total_registros == 0
```

**Saída:** Append em `paths["observabilidade"]` (tabela Gold)

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
| adjclose | double | Preço de fechamento ajustado |
| extraction_timestamp | timestamp | Data/hora da extração |

### Silver: `acoes`
| Coluna | Tipo | Descrição |
|---|---|---|
| ticker | string | Código sem sufixo .SA |
| date | date | Data do pregão |
| open / high / low / close / volume | double/long | OHLCV |
| variacao_pct | double | Variação percentual diária |
| amplitude | double | Diferença high - low |
| empresa | string | Nome da empresa |
| setor | string | Setor B3 |
| ano / mes / trimestre | int | Decomposição temporal |

### Silver: `bcb`
| Coluna | Tipo | Descrição |
|---|---|---|
| data | date | Data da observação |
| valor | double | Valor do indicador (4 casas) |
| indicador | string | selic / cambio / ipca |
| ano / mes / trimestre | int | Decomposição temporal |

### Silver: `world_bank`
| Coluna | Tipo | Descrição |
|---|---|---|
| data | date | Ano de referência (formato yyyy-01-01) |
| valor | double | Valor do indicador (4 casas) |
| indicador | string | pib / desemprego |
| ano | int | Ano extraído |

### Gold: `anomalias`
| Coluna | Tipo | Descrição |
|---|---|---|
| (colunas silver acoes) | — | Herdadas da Silver |
| z_score | double | Z-Score da variação diária |
| tipo_anomalia | string | Normal / Alta Anormal / Queda Anormal |
| is_anomalia | boolean | True se |z_score| > 2 |

### Gold: `deteccao_fraude`
| Coluna | Tipo | Descrição |
|---|---|---|
| id_ordem | string | Identificador da ordem |
| id_cliente | string | ID pseudonimizado (SHA-256) |
| ticker | string | Ação negociada |
| quantidade | int | Quantidade de ações |
| preco | double | Preço unitário |
| valor_total | double | Valor total da ordem |
| alerta_limite | int | 0 ou 1 — regra de limite |
| alerta_volume | int | 0 ou 1 — regra de volume |
| alerta_preco | int | 0 ou 1 — regra de preço |
| alerta_perfil | int | 0 ou 1 — regra de perfil |
| total_alertas | int | Soma dos alertas |
| score_fraude | string | Normal / Médio / Alto / Crítico |
| requer_revisao | boolean | True se total_alertas >= 2 |

### Gold: `score_risco_clientes`
| Coluna | Tipo | Descrição |
|---|---|---|
| id_cliente | string | ID pseudonimizado |
| score_credito | double | Componente de crédito (peso 0.4) |
| score_perfil | double | Componente de perfil (peso 0.2) |
| score_saldo | double | Componente de saldo (peso 0.2) |
| score_comportamento | double | Componente comportamental (peso 0.2) |
| score_risco | double | Score final ponderado [0, 1] |
| categoria_risco | string | Baixo / Moderado / Alto |
| limite_operacional | double | Limite em R$ conforme categoria |
| data_inicio | date | Início de vigência (SCD Type 2) |
| data_fim | date | Fim de vigência (null se atual) |
| atual | boolean | True se registro vigente |

### Gold: `observabilidade`
| Coluna | Tipo | Descrição |
|---|---|---|
| tabela | string | Nome da tabela monitorada |
| total_registros | long | Contagem total |
| score_qualidade | double | Percentual de qualidade [0, 100] |
| nulos_total | long | Total de valores nulos |
| duplicatas | long | Total de registros duplicados |
| alertas | string | JSON com lista de alertas |
| tempo_processamento_s | double | Duração da checagem em segundos |
| timestamp_monitoramento | timestamp | Data/hora da execução |

---

## 5. Algoritmos e Fórmulas

### Detecção de Anomalias (Z-Score)

```
Para cada ação i no ticker t:
  μ_t  = média de variacao_pct de todos os dias de t
  σ_t  = desvio padrão de variacao_pct de t
  z_i  = (variacao_pct_i - μ_t) / σ_t

Critério:
  |z_i| > 2  → Anomalia (±2σ cobre ~95.4% da distribuição normal)
```

**Resultado observado:**
- Anomalias detectadas: 213 de 4.014 (5,31%)
- Distribuição: Alta Anormal e Queda Anormal por ticker

### Score de Risco de Clientes

```
score_risco = (score_credito   × 0.4)
            + (score_perfil    × 0.2)
            + (score_saldo     × 0.2)
            + (score_comportamento × 0.2)

Categorização:
  score > 0.70  → "Baixo Risco"   → limite R$ 500.000
  score ∈ [0.5, 0.70] → "Moderado" → limite R$ 200.000
  score < 0.50  → "Alto Risco"    → limite R$  50.000
```

**Resultado observado (amostra de 1.000 clientes):**
- Baixo Risco: 304 (30,4%)
- Risco Moderado: 534 (53,4%)
- Alto Risco: 162 (16,2%)

### Score de Qualidade de Dados

```
score_qualidade = (1 - nulos / (total_registros × num_colunas)) × 100

Alertas:
  score < 95%  → CRITICAL
  duplicatas > 0 → WARNING
  total == 0   → ERROR
```

---

## 6. Configuração e Dependências

### Variáveis de Configuração (`config/config.py`)

| Variável | Valor | Descrição |
|---|---|---|
| `STORAGE_ACCOUNT` | `stcasesantander` | Nome da conta ADLS Gen2 |
| `EVENTHUB_NAMESPACE` | `evhcasesantander` | Namespace do Event Hub |
| `SQL_SERVER` | `sqlsrvcasesantander` | Servidor Azure SQL |
| `DATABRICKS_CATALOG` | `case_santander` | Unity Catalog principal |
| `BRONZE_SCHEMA` | `bronze` | Schema da camada Bronze |
| `SILVER_SCHEMA` | `silver` | Schema da camada Silver |
| `GOLD_SCHEMA` | `gold` | Schema da camada Gold |

### Segredos no Key Vault (`kv-case-santander`)

| Secret Name | Uso |
|---|---|
| `client-id` | Service Principal — autenticação ADLS |
| `tenant-id` | Azure AD Tenant |
| `client-secret` | Service Principal — credencial |
| `storage-account` | Nome da conta de armazenamento |
| `eventhub-connection-string` | Conexão Event Hub (Kafka) |
| `sql-connection-string` | Conexão Azure SQL Database |
| `kaggle-username` | Autenticação Kaggle API |
| `kaggle-key` | Chave Kaggle API |

### Dependências Python (`requirements.txt`)

```
yfinance==0.2.37           # Dados de ações via Yahoo Finance
requests==2.31.0           # Chamadas REST (BCB, World Bank)
azure-eventhub==5.15.1     # Consumer Kafka/Event Hub
databricks-cli==0.18.0     # Upload de arquivos ao workspace
databricks-connect==15.4   # SparkSession remota
databricks-sdk>=0.20.0     # API REST do Databricks
pytest>=7.4.0              # Framework de testes
```

---

## 7. Guia de Desenvolvimento

### Setup Local

```bash
# Clone e instale dependências
git clone <repo>
cd case-santander-data-master
pip install -r requirements.txt

# Execute os testes
pytest tests/ -v
```

### Estrutura de Branches

```
main       → produção (deploy automático para hk e prod com aprovação)
develop    → desenvolvimento (deploy automático para dev)
feature/*  → novas funcionalidades (PR para develop)
```

### Adicionando uma Nova Ação Monitorada

1. Adicione o ticker com sufixo `.SA` na lista `ACOES` em `src/config/settings.py`
2. Adicione o mapeamento nome/setor em `transformar_acoes()` (`src/transformation/silver_acoes.py`)
3. Execute os testes: `pytest tests/ -v`

### Adicionando um Novo Indicador BCB

1. Consulte o código da série em `bcb.gov.br/dados/serie`
2. Adicione a entrada no dicionário de indicadores em `extrair_bcb()` (`src/ingestion/bcb.py`)
3. Atualize o mapeamento de transformação em `transformar_bcb()` se necessário

### Adicionando uma Nova Regra de Fraude

1. Crie a coluna de alerta em `detectar_fraude()` (`src/gold/fraude.py`)
2. Inclua a nova coluna na soma `total_alertas`
3. Atualize o dicionário de dados neste documento

### Executando um Job Manualmente

```bash
# Via Databricks CLI
databricks jobs run-now --job-id <job_id>

# Via Databricks Workflow UI
pipeline-case-santander → Run now → selecionar tasks
```

### Padrão de Logging

Todos os jobs utilizam `logging` padrão do Python:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"[EXTRAÇÃO] {len(df)} registros extraídos de {source}")
logger.error(f"[ERRO] Falha ao processar {source}: {e}")
```

---

## 8. Monitoramento e Alertas

### Tabelas Monitoradas

| Tabela | Tipo | Threshold Mínimo |
|---|---|---|
| `silver.acoes` | Delta | ≥ 4.000 registros, qualidade ≥ 95% |
| `silver.bcb` | Delta | ≥ 3.000 registros, qualidade ≥ 95% |
| `silver.world_bank` | Delta | ≥ 50 registros |
| `silver.clientes` | Delta | ≥ 10.000 registros |
| `silver.ordens` | Delta | ≥ 5.000 registros |
| `gold.anomalias` | Delta | ≥ 4.000 registros |
| `gold.deteccao_fraude` | Delta | ≥ 5.000 registros |
| `gold.score_risco_clientes` | Delta | ≥ 1.000 registros |
| `gold.observabilidade` | Delta | Append por execução |

### Lakehouse Monitoring

6 tabelas com perfil automático configurado via Databricks Lakehouse Monitoring:
- Dashboard de qualidade gerado automaticamente
- Alertas por degradação de schema
- Drift detection em distribuições de valores

### Consultar Histórico de Qualidade

```sql
SELECT 
  tabela,
  score_qualidade,
  total_registros,
  alertas,
  timestamp_monitoramento
FROM case_santander.gold.observabilidade
ORDER BY timestamp_monitoramento DESC
LIMIT 100;
```

---

*Documentação gerada em 2026-04-02 — Case Santander Data Master*
