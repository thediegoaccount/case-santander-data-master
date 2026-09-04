# A2 — REGRAS DE NEGÓCIO (engenharia reversa do código)

> **Procedência.** Repositório `case-santander-data-master`, branch
> `release/segunda-chance-dm`, **HEAD lido = `c2a8811`**
> (`fix: corrige codigo morto em fraude.py e denominador instavel da Fase V`).
> Working tree suja apenas em documentação (`M README.md`, `M docs/README.md`,
> `?? EXECUTIVE_SUMMARY.md`) — nenhum `.py` pendente, então o que está
> documentado aqui **é** o código de `c2a8811`.
>
> **⚠ O `INVENTARIO.md` da Fase 0 foi lido no commit `f7265c7`, dois commits
> atrás.** Duas afirmações dele estão desatualizadas em relação ao código que
> este documento descreve — listadas na §7.4 (D-01 e D-02). Onde houver
> divergência de número de linha entre `INVENTARIO.md` e este arquivo, **vale
> este**, porque as linhas foram lidas em `c2a8811`.
>
> **Método.** Toda regra abaixo foi lida no `.py`. README, `docs/` e
> `EXECUTIVE_SUMMARY.md` **não** foram usados como fonte de regra — o
> `notebooks/case_presentation.py` aparece apenas na §7 como *contraparte
> documental divergente*, nunca como fonte.
>
> **Segredos.** Nenhum valor de secret, salt, connection string, hostname
> interno ou amostra de dado real aparece aqui. Só o mecanismo e a localização.

---

## SUMÁRIO

| § | Conteúdo |
|---|---|
| 1 | Módulos de ingestão — entrada, saída, regras |
| 2 | Módulos de transformação (Bronze→Silver) |
| 3 | Módulos Gold (batch) |
| 4 | Módulos Gold (streaming) |
| 5 | SCD, observabilidade, qualidade, segurança |
| 6 | Tabela de limiares e constantes do domínio |
| 7 | **Regras contraditórias ou duplicadas** |
| 8 | Relações (joins) |
| 9 | Validações — o que bloqueia e o que é decorativo |
| 10 | Dados sensíveis / LGPD |
| 11 | Campos [NÃO CONFIRMADO] |

**Contagens finais:** 208 campos derivados documentados · 96 limiares/constantes
catalogados · 13 achados de regra contraditória ou duplicada · 6 pontos
[NÃO CONFIRMADO].

---

## 1. INGESTÃO — `src/ingestion/`

### 1.1 `src/ingestion/clientes_kaggle.py` — cadastro de clientes

**Entrada.** Dataset público de churn bancário baixado da API do Kaggle
(`URL_DATASET`, `:21`, dataset `mathchi/churn-for-bank-customers`), com
autenticação Basic usando dois secrets do Key Vault (`kaggle-username`,
`kaggle-key`, `:53`). ZIP extraído em `/tmp/kaggle_<pid>/churn.csv` (`:47,:64`).

**Saída.** `case_santander.<env>_bronze.clientes`, via `merge_ou_cria` com chave
`hash_cliente` (`:104-105`).

**Regras implementadas:**

**`id_cliente`** — `:82`
- a) `id_cliente = "CLI" + str(CustomerId)`
- b) sem ramos condicionais
- c) origem: coluna `CustomerId` do CSV do Kaggle
- d) nulo: `CustomerId` nulo produziria a string `"CLInan"` (pandas), não há guarda
- e) domínio: string com prefixo `CLI` seguida do identificador original
- f) sem unidade
- **⚠ Achado LGPD (§10).** Este campo carrega o `CustomerId` **em claro**, ao
  lado do `hash_cliente` que anonimiza o mesmo valor.

**`hash_cliente`** — `:83`
- a) `hash_cliente = SHA256(str(CustomerId) || salt)`, hex de 64 caracteres
  (`src/security/hashing.py:39-45`)
- b) sem ramos
- c) origem: `CustomerId` do CSV; `salt` vem do Key Vault
  (`src/security/hashing.py:14-17` → `src/config/secrets.py:32-37`, chave `salt`)
- d) se o salt não puder ser lido, `get_salt` levanta `ValueError`
  (`hashing.py:16-17`) — a ingestão **aborta**, não grava sem salt
- e) domínio: `[0-9a-f]{64}`
- f) sem unidade
- É a chave de negócio do cliente em todo o lakehouse (MERGE, joins e SCD).

**`sobrenome_masked`** — `:84`
- a) `SHA256(str(Surname) || salt)` — mesma função, mesmo salt
- b)-f) idênticos ao `hash_cliente`
- Nota: como o salt é o mesmo, sobrenomes iguais geram hashes iguais → o campo
  preserva cardinalidade e permite agrupar por sobrenome sem revelá-lo.

**`perfil_risco`** — `classificar_perfil`, `:24-30`
- a) função de `score_credito`
- b) ramos completos:
  - `score >= 750` → `"Arrojado"`
  - `score >= 600` → `"Moderado"`
  - **else** → `"Conservador"`
- c) origem: `CustomerId`→não; vem de `CreditScore` do CSV (`:85`)
- d) `score` nulo (`NaN`): `NaN >= 750` é `False`, `NaN >= 600` é `False` →
  cai no else → `"Conservador"`. **Não há tratamento explícito de nulo**: um
  cliente sem score é classificado como Conservador, não como desconhecido.
- e) domínio: `{Arrojado, Moderado, Conservador}` — 3 valores, sem nulo possível
- f) adimensional (score de crédito, escala 0–850 pelo uso em
  `jobs/job_corretora_analises.py:84`)

**`faixa_saldo`** — `classificar_saldo`, `:33-41`
- a) função do saldo em conta
- b) ramos completos:
  - `balance == 0` → `"Sem saldo"`
  - `balance < 50000` → `"Baixo"`
  - `balance < 150000` → `"Medio"`
  - **else** → `"Alto"`
- c) origem: `Balance` do CSV (`:86`)
- d) zero tem ramo próprio (`"Sem saldo"`). Nulo/`NaN`: todas as comparações
  dão `False` → cai no else → **`"Alto"`**. Saldo negativo cairia em `"Baixo"`.
- e) domínio: `{Sem saldo, Baixo, Medio, Alto}` — 4 valores
- f) unidade do dataset Kaggle, tratada como **BRL** por todo o pipeline a
  jusante (`jobs/job_corretora_analises.py:126` calcula `saldo_medio`) —
  [NÃO CONFIRMADO] que a fonte original seja BRL; ver §11.

**`ativo`** — `:87` — a) `ativo = (IsActiveMember == 1)`; b) sem ramos;
c) `IsActiveMember`; d) nulo → `False`; e) `{true,false}`; f) booleano.

**`churn`** — `:88` — a) `churn = (Exited == 1)`; b) sem ramos; c) `Exited`;
d) nulo → `False`; e) `{true,false}`; f) booleano.

**`data_extracao`** — `:89` — a) `datetime.now()` formatado `%Y-%m-%d`
(`:75`); b) sem ramos; c) relógio do driver; d) n/a; e) data ISO;
f) data local do cluster (sem timezone explícito — ver §11).

**Renomeação de 8 colunas** (`:96-101`), regra de vocabulário de domínio:
`CreditScore→score_credito`, `Geography→pais`, `Gender→genero`, `Age→idade`,
`Tenure→anos_cliente`, `Balance→saldo`, `NumOfProducts→num_produtos`,
`EstimatedSalary→salario_estimado`.

**Semântica de escrita** (`src/utils/delta.py:27-44`): MERGE por `hash_cliente`
com `whenMatchedUpdateAll` + `whenNotMatchedInsertAll`. Se a tabela não existe
(`"is not a Delta table"` ou `"Table or view not found"` na mensagem), faz
primeira carga com `overwrite`; **qualquer outro erro sobe** (`:43-44`).

---

### 1.2 `src/ingestion/ordens_simuladas.py` — ordens de compra e venda

**Entrada.** `case_santander.<env>_bronze.clientes` (`:46`), colunas
`hash_cliente`, `perfil_risco`, `faixa_saldo`.
**Saída.** `case_santander.<env>_bronze.ordens`, MERGE por `id_ordem` (`:87-88`).

**Regra de reprodutibilidade** (`:38,:48,:57`): `random.seed(42)` +
`orderBy("hash_cliente")` antes de amostrar + `df.sample(random_state=42)`.
O `orderBy` é obrigatório porque Delta não garante ordem de leitura — sem ele a
mesma seed sortearia clientes diferentes a cada execução (comentário `:40-44`).

**Regra de amostragem** (`:56-57`): `n = min(1000, total_de_clientes)`. Se
`bronze.clientes` estiver vazia, retorna 0 sem gravar (`:52-54`).

**Regras de geração de cada ordem** (`:61-85`):

| Campo | a) fórmula | b) ramos | c) origem | d) nulo/zero | e) domínio | f) unidade |
|---|---|---|---|---|---|---|
| `id_ordem` | `"ORD" + hash_cliente + "-" + data_ordem(%Y%m%d) + "-" + contador com 4 dígitos` (`:69`) | — | `bronze.clientes.hash_cliente` + data sorteada + contador global | contador nunca 0 (inicia em 1, `:60,:66`) | string determinística | — |
| `hash_cliente` | cópia | — | `bronze.clientes.hash_cliente` | — | hex 64 | — |
| `perfil_risco` | cópia | — | `bronze.clientes.perfil_risco` | — | `{Arrojado,Moderado,Conservador}` | — |
| `faixa_saldo` | cópia | — | `bronze.clientes.faixa_saldo` | — | `{Sem saldo,Baixo,Medio,Alto}` | — |
| `ticker` | `random.choice(ACOES)` (`:76`) | — | lista literal `:25-28` | — | 9 tickers B3 | — |
| `preco` | `round(uniform(10,100), 2)` (`:63`) | — | gerador | nunca 0 | `[10.00, 100.00]` | **BRL** |
| `quantidade` | `randint(100, 10000)` (`:64`) | — | gerador | nunca 0 | `[100, 10000]` | unidades (ações) |
| `valor_total` | `round(preco * quantidade, 2)` (`:79`) | — | derivado | nunca 0 | `[1.000,00 , 1.000.000,00]` | **BRL** |
| `tipo` | `random.choice(["compra","venda"])` (`:80`) | — | gerador | — | `{compra, venda}` | — |
| `corretora` | literal `"Santander Corretora"` (`:81`) | — | constante | — | 1 valor | — |
| `status` | `random.choice(["executada","cancelada","pendente"])` (`:82`) | — | gerador | — | 3 valores | — |
| `data_ordem` | `2024-01-01 + timedelta(days=randint(0,457))` (`:65`) | — | gerador | — | `2024-01-01` a `2025-04-02` | data |
| `data_extracao` | `now()` `%Y-%m-%d` (`:37`) | — | relógio | — | data ISO | data |

**Quantidade de ordens por cliente:** `randint(1, 10)` (`:62`) — ou seja, entre
1.000 e 10.000 ordens por execução com 1.000 clientes amostrados.

---

### 1.3 `src/ingestion/yahoo_finance.py` — cotações B3

**Entrada.** API `yfinance`, `ticker.history(period="2y")` (`:50`), para os 9
tickers de `src/config/settings.py:2-12`.
**Saída.** Parquet em `abfss://bronze@<sa>.dfs.core.windows.net/acoes/data=<hoje>/`
(`:71-73`), modo `overwrite`.

Regras:
- **Janela histórica:** 2 anos por ticker (`:50`).
- **Retry:** decorator `@retry_on_connection_error(max_attempts=3)` (`:13`) —
  backoff 1s × 2 por tentativa (`src/utils/retry.py:85-90`), capturando as
  quatro famílias de exceção de conexão/timeout (`retry.py:89`).
- **Rate limit:** `rate_limiter.wait_if_needed("yahoo_finance")` por ticker
  (`:46`) + `time.sleep(0.5)` fixo entre tickers (`:58`).
- **Isolamento por falha:** exceção em um ticker é logada e o laço continua
  (`:60-61`) — a extração **não aborta** por causa de um ativo.
- **Campos derivados (3):** `ticker` = literal do laço (`:51`);
  `data_extracao` = `now()` `%Y-%m-%d` (`:52`); `ambiente` = `get_env()`
  (`:53`, tag de rastreabilidade, domínio `{hk, prod}`).
- **Normalização de nomes:** todas as colunas viram minúsculas com espaço → `_`
  (`:68`) — é o que transforma `Stock Splits` em `stock_splits`.
- **Curto-circuito:** se nada foi extraído, retorna 0 sem gravar (`:63-65`).

### 1.4 `src/ingestion/bcb.py` — séries do Banco Central

**Entrada.** API SGS do BCB, 3 séries (`:158-164`):

| Código SGS | `indicador` gravado | Significado de domínio |
|---|---|---|
| `11` | `selic` | taxa Selic |
| `1` | `cambio_usd_brl` | taxa de câmbio USD/BRL |
| `433` | `ipca` | IPCA (inflação mensal) |

**Janela obrigatória:** `dataInicial=01/04/2021`, `dataFinal=01/04/2026`
(`:29-30`) — regra imposta pelo BCB desde 26/03/2025, comentada em `:41,:47-48`.

**Saída.** Parquet em `bronze/bcb/extracao=<hoje>/` (`:181-183`).

**Cadeia de validação de 8 etapas** por série (`buscar_serie`, `:39-152`), toda
com retry local (`max_retries=3`, backoff `2**tentativa` segundos, `:68`):

| # | Validação | Linha | Falha final |
|---|---|---|---|
| 1 | `status_code == 200` | `:63-71` | retorna DataFrame vazio |
| 2 | corpo não vazio | `:74-80` | DataFrame vazio |
| 3 | `Content-Type` contém `json` ou `javascript` (rejeita HTML de erro) | `:83-93` | DataFrame vazio |
| 4 | `response.json()` parseável | `:96-107` | DataFrame vazio |
| 5 | payload é lista não vazia | `:110-112` | DataFrame vazio (**sem retry**) |
| 6 | constrói DataFrame | `:115` | — |
| 7 | colunas `data` e `valor` presentes | `:118-120` | DataFrame vazio (**sem retry**) |
| 8 | enriquece e converte `valor` para numérico com `errors="coerce"` + `dropna` | `:123-127` | linhas não numéricas são descartadas |

**Campos derivados (4):** `indicador` = nome lógico da série (`:123`);
`data_extracao` (`:124`); `ambiente` (`:125`); `valor` = `to_numeric(coerce)`
seguido de `dropna` (`:126-127`) — valores não numéricos viram nulo e **são
removidos**.

**Degradação:** se as 3 séries falharem, retorna 0 sem gravar (`:169-171`).
Se apenas 1 ou 2 falharem, grava as que funcionaram (`:167`) — o job de silver a
jusante não sabe que faltou série.

### 1.5 `src/ingestion/world_bank.py` — indicadores macro

**Entrada.** API World Bank, país `BR`, `per_page=30` (`:46`), 2 indicadores:
`NY.GDP.MKTP.KD.ZG` → `pib_anual` e `SL.UEM.TOTL.ZS` → `desemprego` (`:80-81`).
**Saída.** Parquet em `bronze/world_bank/extracao=<hoje>/` (`:92-94`).

Regras:
- **Separação retry/erro de dado** (`:37-49` vs `:51-78`): só a chamada de rede
  `_requisitar` tem retry (3 tentativas); erro de schema/JSON **não** é
  retentado, é logado e o indicador vira vazio (`:75-78`).
- **Filtro de nulos na origem** (`:60`): registros com `value is None` são
  descartados **antes** de virar linha.
- **Campos derivados (6):** `ano` = `r["date"]` (string na origem, cast para
  inteiro só na silver); `valor` = `to_numeric(coerce)` (`:70`); `indicador`
  (`:66`); `data_extracao` (`:67`); `fonte` = literal `"world_bank"` (`:68`);
  `ambiente` (`:69`).
- **Degradação:** se ambos falharem, retorna 0 sem gravar (`:85-87`).

### 1.6 `src/ingestion/api_wrapper.py` — governança de chamadas externas

Não gera campo, mas carrega uma regra operacional: **janela de rate limit de
60 segundos por (ambiente, API)** (`:36`), com contador resetado a cada minuto
(`:28-38`). `wait_if_needed` dorme o tempo restante da janela quando o limite é
atingido (`:64-70`). Limite default para API não catalogada: **60 req/min**
(`:46,:62`). Os limites por API e ambiente estão na §6.

---

## 2. TRANSFORMAÇÃO — `src/transformation/` (Bronze → Silver)

### 2.1 `src/transformation/silver_acoes.py`

**Entrada.** Parquet `bronze/acoes/` (`:22,:27`).
**Saída.** Delta em `silver/acoes/` particionado por `ano`,`mes` (`:66-71`) +
registro como tabela externa `<...>_silver.acoes` (`:89`).

**Campos derivados (8):**

**`date`** — `:31` — a) `to_date(date)`; b) —; c) `bronze.acoes.date`;
d) valor não parseável → null (e a linha **não** é filtrada por isso);
e) date; f) —.

**`ano` / `mes` / `trimestre`** — `:32-34` — a) `year(date)` / `month(date)` /
`quarter(date)`; b) —; c) `date` já tipada; d) `date` null → todos null (e a
linha vira partição `__HIVE_DEFAULT_PARTITION__`); e) `ano` inteiro, `mes` 1–12,
`trimestre` 1–4; f) —.

**`open`/`high`/`low`/`close`** — `:35-38` — arredondados para **2 casas**
(BRL).

**`variacao_diaria_pct`** — `:39-40`
- a) `round((close - open) / open * 100, 4)`
- b) sem ramos condicionais
- c) `bronze.acoes.close` e `bronze.acoes.open` (já arredondados a 2 casas)
- d) **`open = 0` → divisão por zero. Não há guarda.** Em Spark isso produz
  `null` (não exceção). `open` null → resultado null. `close` null é
  eliminado depois pelo filtro `:62`, mas o cálculo já ocorreu.
- e) percentual real, sem limite teórico; na prática ±alguns %
- f) **percentual (%)**

**`amplitude_diaria`** — `:41-42` — a) `round(high - low, 2)`; b) —;
c) `bronze.acoes.high`, `.low`; d) qualquer nulo → null; e) ≥ 0 se os dados
forem coerentes; f) **BRL**.

**`empresa`** — `:43-53` — mapa ticker→razão social, 9 ramos + else:
`PETR4.SA`→Petrobras, `VALE3.SA`→Vale, `ITUB4.SA`→Itau, `BBDC4.SA`→Bradesco,
`ABEV3.SA`→Ambev, `MGLU3.SA`→Magazine Luiza, `WEGE3.SA`→WEG,
`BBAS3.SA`→Banco do Brasil, `SANB11.SA`→Santander, **else `"Desconhecido"`**.
d) ticker null → else → `"Desconhecido"`. e) 10 valores possíveis.

**`setor`** — `:54-60` — 5 ramos + else:
`{PETR4,VALE3}`→`Commodities`; `{ITUB4,BBDC4,BBAS3,SANB11}`→`Financeiro`;
`ABEV3`→`Consumo`; `MGLU3`→`Varejo`; `WEGE3`→`Industria`; **else `"Outros"`**.
e) 6 valores possíveis.

**`data_processamento`** — `:61` — `now()` `%Y-%m-%d`.

**Filtros de qualidade** (`:62-63`): mantém só linhas com `close IS NOT NULL`
**e** `volume > 0`. Linhas com `volume = 0` (pregão sem negócio) são
**descartadas** — decisão de domínio relevante: dias sem liquidez não entram nas
médias de performance.

**Colunas removidas** (`:64`): `dividends`, `stock_splits`.

**Gate de qualidade** (`:78-82`) — ver §9.

### 2.2 `src/transformation/silver_bcb.py`

**Entrada.** `bronze/bcb/extracao=*/` com `basePath` (`:16`).
**Saída.** Delta `silver/bcb/` (`:37-41`) + registro `<...>_silver.bcb` (`:58`).

**Campos derivados (6):**
- **`data`** `:24` — `to_date(data, "dd/MM/yyyy")`. O formato brasileiro é
  explícito; sem ele o parse falharia silenciosamente. Não parseável → null →
  **linha descartada** por `:31`.
- **`ano`/`mes`/`trimestre`** `:25-27` — `year/month/quarter` de `data`.
- **`valor`** `:28` — `round(valor, 6)`. Seis casas porque câmbio e Selic
  exigem precisão. Unidade **depende do `indicador`**: `selic` = % a.a.,
  `cambio_usd_brl` = BRL por USD, `ipca` = % no mês. *(A unidade não está
  declarada em coluna; é conhecimento implícito do código da ingestão,
  `src/ingestion/bcb.py:158-164`.)*
- **`data_processamento`** `:29`.

**Filtros:** `valor IS NOT NULL` e `data IS NOT NULL` (`:30-31`).
**Deduplicação:** `dropDuplicates(["data","indicador"])` (`:32`) — a chave
lógica da série. **Coluna de partição `extracao` é removida** (`:33`), o que
significa que a silver **não guarda de qual extração veio cada linha**.

### 2.3 `src/transformation/silver_world_bank.py`

**Entrada.** `bronze/world_bank/extracao=*/` (`:16`).
**Saída.** Delta `silver/world_bank/` (`:34-38`) + registro (`:55`).

**Regra de compatibilidade de schema** (`:20-25`) — três ramos:
- se existe coluna `ano` → `cast("integer")`
- **elif** existe coluna `data` → `ano = cast(data as integer)` (schema antigo)
- **else** → `raise Exception("Coluna de ano não encontrada no Bronze!")` — o
  job **aborta**.

**Campos derivados (3):** `ano` (acima), `valor` = `round(valor, 4)` (`:29`),
`data_processamento` (`:30`). **Filtros:** `valor IS NOT NULL`,
`ano IS NOT NULL` (`:31-32`).
Unidade: `pib_anual` = % de crescimento anual do PIB;
`desemprego` = % da força de trabalho — determinado pelos indicadores
`NY.GDP.MKTP.KD.ZG` e `SL.UEM.TOTL.ZS` (`src/ingestion/world_bank.py:80-81`).

### 2.4 `src/transformation/silver_clientes.py`

**Entrada.** `<...>_bronze.clientes` (`:24`). **Saída.** `<...>_silver.clientes`
via MERGE por `hash_cliente` (`:37`).

**`faixa_etaria`** — `:25-28`
- a) função de `idade`
- b) ramos completos: `idade < 30` → `"Jovem"`; `idade < 50` → `"Adulto"`;
  **else** → `"Senior"`
- c) `bronze.clientes.idade` (← `Age` do Kaggle)
- d) `idade` null → ambas as comparações `false` → **`"Senior"`**. Idade
  negativa cairia em `"Jovem"`. Sem guarda.
- e) `{Jovem, Adulto, Senior}`
- f) anos

**`score_categoria`** — `:29-33`
- a) função de `score_credito`
- b) ramos completos: `>= 750` → `"Excelente"`; `>= 650` → `"Bom"`;
  `>= 550` → `"Regular"`; **else** → `"Ruim"`
- c) `bronze.clientes.score_credito` (← `CreditScore`)
- d) null → todas falsas → **`"Ruim"`**
- e) `{Excelente, Bom, Regular, Ruim}`
- f) adimensional (escala 0–850)
- **⚠ Ver §7, achado C-05:** os cortes divergem de `classificar_perfil`.

**`data_processamento`** — `:34`.

### 2.5 `src/transformation/silver_ordens.py`

**Entrada.** `<...>_bronze.ordens` (`:24`). **Saída.** `<...>_silver.ordens`
via MERGE por `id_ordem` (`:31`).

**Campos derivados (4):** `data_ordem` = `to_date(data_ordem)` (`:25`);
`ano` = `year(data_ordem)` (`:26`); `mes` = `month(data_ordem)` (`:27`);
`data_processamento` (`:28`). Nulo em `data_ordem` propaga para `ano`/`mes`
sem filtro — **não há guarda**; linhas com data inválida entram na silver.

---

## 3. GOLD BATCH — `src/gold/`

### 3.1 `src/gold/anomalias.py` — detecção de anomalia de preço diário

**Entrada.** Delta em `silver/acoes/` (`:16`), com
`dropDuplicates(["date","ticker"])` (`:17`).
**Saída.** Delta em `gold/anomalias/` (`:41-45`), 12 colunas (`:33-39`).

**Janela:** `Window.partitionBy("ticker")` — **sem `orderBy` e sem
`rowsBetween`**, ou seja, a média e o desvio são calculados sobre **toda a
história disponível daquele ticker** (`:19`).

**`media_variacao`** — `:22` — a) `avg(variacao_diaria_pct)` sobre a janela do
ticker; b) —; c) `silver.acoes.variacao_diaria_pct`; d) nulos são ignorados por
`avg`; e) percentual; f) **%**. *(coluna intermediária, não selecionada em
`:33-39`.)*

**`std_variacao`** — `:23` — a) `stddev(variacao_diaria_pct)` (desvio amostral)
sobre a janela do ticker; b) —; c) mesma origem; d) **com uma única observação
por ticker, `stddev` retorna null**; e) ≥ 0 ou null; f) **%**.
*(intermediária.)*

**`zscore`** — `:24-25`
- a) `zscore = round((variacao_diaria_pct - media_variacao) / std_variacao, 4)`
- b) sem ramos
- c) `silver.acoes.variacao_diaria_pct` + as duas janelas acima
- d) **`std_variacao = 0` (ticker com variação constante) → divisão por zero →
  null; `std_variacao` null (1 observação) → null. Não há guarda** — compare com
  `streaming_gold.py:144-145`, que **tem** guarda para o caso equivalente (§7,
  achado C-08).
- e) real, tipicamente `[-4, 4]`; null quando o desvio é 0/nulo
- f) adimensional (nº de desvios-padrão)

**`anomalia`** — `:26-27`
- a) `abs(zscore) > 2` → `True`; **else** `False`
- b) 2 ramos (when + otherwise)
- c) `zscore`
- d) `zscore` null → `abs(null) > 2` é null → **não é true** → cai no
  `otherwise` → **`False`**. Ou seja, dia sem z-score calculável é classificado
  como **não anômalo**.
- e) `{true, false}` — nunca null
- f) booleano

**`tipo_anomalia`** — `:28-31`
- a)/b) ramos completos: `zscore > 2` → `"Alta Anormal"`;
  `zscore < -2` → `"Queda Anormal"`; **else** → `"Normal"`
- c) `zscore`
- d) `zscore` null → else → **`"Normal"`**
- e) `{Alta Anormal, Queda Anormal, Normal}`
- f) —

**`data_processamento`** — `:32`.

**Escrita:** `mode("overwrite")` — a gold de anomalias é **recalculada
inteira** a cada execução, não é histórico incremental.

### 3.2 `src/gold/performance.py` — performance por ticker/setor/ano

**Entrada.** Delta `silver/acoes/` (`:24`) — **sem dedup**, diferente de
`anomalias.py:17`.
**Saída.** Delta `gold/performance_acoes/` (`:42-45`).

**Agrupamento:** `ticker, empresa, setor, ano` (`:28`).

**Campos derivados (9)** — `:29-39`:

| Campo | a) fórmula | c) origem | d) nulo/zero | e) domínio | f) unidade |
|---|---|---|---|---|---|
| `preco_medio` | `round(avg(close), 2)` | `silver.acoes.close` | grupo todo nulo → null | > 0 | **BRL** |
| `preco_minimo` | `round(min(close), 2)` | idem | idem | > 0 | **BRL** |
| `preco_maximo` | `round(max(close), 2)` | idem | idem | > 0 | **BRL** |
| `variacao_media_pct` | `round(avg(variacao_diaria_pct), 4)` | `silver.acoes.variacao_diaria_pct` | nulos ignorados | real | **%** |
| `volatilidade` | `round(stddev(variacao_diaria_pct), 4)` — desvio-padrão **amostral** da variação diária | idem | **1 observação no grupo → null** | ≥ 0 ou null | **%** |
| `volume_medio` | `round(avg(volume), 0)` | `silver.acoes.volume` | — | > 0 (filtro em silver) | ações/dia |
| `volume_total` | `round(sum(volume), 0)` | idem | — | > 0 | ações |
| `dias_negociados` | `count("*")` | contagem de linhas do grupo | — | ≥ 1 | dias |
| `data_processamento` | `lit(data_hoje)` | relógio | — | data ISO | — |

**Nenhum ramo condicional** neste módulo. `volatilidade` é a fonte do
`desvio_historico_rs` no streaming (§4.2) e do `alerta_desvio_historico` (§4.1)
— sua **unidade é percentual**, o que explica a divisão por 100 lá.

### 3.3 `src/gold/bcb_analise.py` — indicadores macroeconômicos BCB

**Entrada.** `<...>_silver.bcb` (`:24`).
**Saída.** `<...>_gold.indicadores_bcb` via `saveAsTable` overwrite (`:70-72`).

**Pivot** (`:28-32`): agrupa por `data`, pivota `indicador`, agrega
`first(valor)`. **As colunas `selic`, `cambio_usd_brl` e `ipca` nascem do pivot**
— seus nomes vêm dos literais de `src/ingestion/bcb.py:158-164`. Se uma série
faltar na silver, **a coluna não existe** e todo o `withColumn` que a referencia
falha com `UNRESOLVED_COLUMN` → o job aborta.

**Janelas** (`:36-38`) — todas `Window.orderBy("data").rangeBetween(...)`,
**globais (sem `partitionBy`)**, medidas em **segundos**:
`window_7d = -7*86400 .. 0`; `window_30d = -30*86400 .. 0`;
`window_12m = -365*86400 .. 0`.

**Campos derivados (12):**

**`selic`, `cambio_usd_brl`, `ipca`** (3, do pivot) — a) `first(valor)` por
`data`; b) —; c) `silver.bcb.valor` filtrado por `silver.bcb.indicador`;
d) dia sem a série → **null** (o pivot preenche com null); e) real;
f) `selic` % a.a., `cambio_usd_brl` **BRL/USD**, `ipca` % ao mês.

**`selic_media_7d`** — `:42-43` — a) `round(avg(selic) over window_7d, 4)`;
b) —; c) coluna `selic` do pivot; d) nulos ignorados por `avg`; se **todos**
os dias da janela forem null → null; e) real; f) **% a.a.**

**`selic_volatilidade_30d`** — `:44-45` — a)
`round(stddev(selic) over window_30d, 4)`; b) —; c) `selic`; d) uma única
observação na janela → **null**; e) ≥ 0 ou null; f) **% a.a.**

**`cambio_media_7d`** — `:46-47` — a)
`round(avg(cambio_usd_brl) over window_7d, 4)`; b) —; c) `cambio_usd_brl`;
d) idem; e) > 0; f) **BRL/USD**.

**`cambio_variacao_pct`** — `:48-51`
- a) `round((cambio_usd_brl - cambio_media_7d) / cambio_media_7d * 100, 2)`
- b) sem ramos
- c) `cambio_usd_brl` (pivot) e `cambio_media_7d` (calculada acima)
- d) **`cambio_media_7d = 0` → divisão por zero → null. Não há guarda.**
  Qualquer nulo nos insumos → null.
- e) percentual; e) na prática pequeno
- f) **%** (desvio do câmbio do dia em relação à média móvel de 7 dias)

**`ipca_acumulado_12m`** — `:52-53` — a) `sum(ipca) over window_12m` — **soma
simples**, sem arredondamento e **sem capitalização composta**; b) —;
c) `ipca` (pivot); d) nulos ignorados por `sum`; janela toda nula → null;
e) real; f) **%** (soma de variações mensais, não é o IPCA acumulado composto —
ver §11).

**`tendencia_selic`** — `:54-57`
- a)/b) ramos completos: `selic > selic_media_7d` → `"Alta"`;
  `selic < selic_media_7d` → `"Queda"`; **else** → `"Estavel"`
- c) `selic`, `selic_media_7d`
- d) qualquer null → ambas falsas → **`"Estavel"`**
- e) `{Alta, Queda, Estavel}`

**`alerta_cambio`** — `:58-61`
- a)/b) ramos completos: `abs(cambio_variacao_pct) > 5` → `"Critico"`;
  `abs(cambio_variacao_pct) > 2` → `"Alto"`; **else** → `"Normal"`
- c) `cambio_variacao_pct`
- d) null → **`"Normal"`** (falso negativo silencioso)
- e) `{Critico, Alto, Normal}`
- f) limiares em **pontos percentuais**

**`alerta_inflacao`** — `:62-65`
- a)/b) ramos completos: `ipca_acumulado_12m > 5` → `"Critico"`;
  `> 3` → `"Alto"`; **else** → `"Normal"`
- c) `ipca_acumulado_12m`
- d) null → **`"Normal"`**
- e) `{Critico, Alto, Normal}`
- f) **%** — o 5 e o 3 são leitura direta de "IPCA acumulado acima de 5% / 3%
  ao ano" (o teto e o centro da meta de inflação, na prática)

**`data_processamento`** — `:66`.

### 3.4 `src/gold/world_bank_analise.py` — contexto macroeconômico

**Entrada.** `<...>_silver.world_bank` (`:24`).
**Saída.** `<...>_gold.contexto_macroeconomico` (`:93-95`).

**Pivot** por `ano` (`:28-32`) → colunas `pib_anual` e `desemprego`.
**Janela:** `Window.orderBy("ano")` — global, para `lag` de 1 ano.

**Campos derivados (10):**

**`pib_anual`, `desemprego`** (2, do pivot) — c) `silver.world_bank.valor`
filtrado por `indicador`; d) ano sem o indicador → null;
f) **%** de crescimento do PIB / **%** da força de trabalho.

**`pib_variacao_pct`** — `:38-42`
- a) `round((pib_anual - lag(pib_anual)) / lag(pib_anual) * 100, 2)`
- b) sem ramos
- c) `pib_anual` do próprio ano e do ano anterior (`lag` sobre `Window.orderBy("ano")`)
- d) **primeiro ano da série → `lag` null → resultado null**;
  **`lag(pib_anual) = 0` → divisão por zero → null**; sem guarda
- e) real
- f) **%** — atenção: é a variação percentual **da taxa de crescimento**, não a
  variação do PIB. `pib_anual` já é um percentual. É uma derivada de segunda
  ordem. Ver §11.

**`desemprego_variacao_pct`** — `:43-46`
- a) `round(desemprego - lag(desemprego), 2)` — **diferença absoluta**, não
  razão, apesar do sufixo `_pct` no nome
- b) sem ramos
- c) `desemprego` do ano e do anterior
- d) primeiro ano → null; sem divisão, logo sem risco de divisão por zero
- e) real
- f) **pontos percentuais (p.p.)** — o nome do campo diz `_pct` mas o conteúdo
  é p.p.; ver §7, achado C-13

**`tendencia_pib`** — `:47-51`
- a)/b) ramos completos: `pib_variacao_pct > 2` → `"Crescimento Alto"`;
  `> 0` → `"Crescimento Moderado"`; `> -2` → `"Queda Moderada"`;
  **else** → `"Queda Severa"`
- c) `pib_variacao_pct`
- d) null (primeiro ano) → todas falsas → **`"Queda Severa"`** — falso alarme
  garantido na primeira linha da série
- e) 4 valores

**`tendencia_desemprego`** — `:52-56`
- a)/b) ramos completos: `desemprego_variacao_pct < -1` →
  `"Melhorando (Rápido)"`; `< 0` → `"Melhorando"`; `< 1` → `"Piorando"`;
  **else** → `"Piorando (Rápido)"`
- c) `desemprego_variacao_pct`
- d) null → todas falsas → **`"Piorando (Rápido)"`**
- e) 4 valores
- **Nota de domínio:** a faixa `[0, 1)` é rotulada `"Piorando"`, o que inclui
  variação exatamente 0 (desemprego estável) — não há rótulo "Estável".

**`cenario_macro`** — `:57-70`
- a)/b) ramos completos, avaliados em ordem:
  1. `pib_variacao_pct > 2 AND desemprego_variacao_pct < 0` → `"Expansao Economica"`
  2. `pib_variacao_pct > 0 AND desemprego_variacao_pct > 0` → `"Desaceleracao"`
  3. `pib_variacao_pct < 0 AND desemprego_variacao_pct > 1` → `"Recessao"`
  4. **else** → `"Transicao"`
- c) `pib_variacao_pct`, `desemprego_variacao_pct`
- d) qualquer null → nenhuma condição verdadeira → **`"Transicao"`**
- e) `{Expansao Economica, Desaceleracao, Recessao, Transicao}`
- **Lacuna de cobertura:** PIB entre 0 e 2 com desemprego caindo cai em
  `"Transicao"`; PIB < 0 com desemprego entre 0 e 1 também. `"Transicao"` é o
  balde de tudo que não encaixa, não um cenário nomeado.

**`alerta_risco`** — `:71-80`
- a)/b) ramos completos:
  1. `pib_variacao_pct < -2 OR desemprego > 15` → `"Alto"`
  2. `pib_variacao_pct < 0 OR desemprego > 10` → `"Moderado"`
  3. **else** → `"Baixo"`
- c) `pib_variacao_pct` (derivada) e **`desemprego` (nível absoluto, não a
  variação)**
- d) `pib_variacao_pct` null com `desemprego` conhecido: o `OR` ainda resolve
  pelo segundo termo. Ambos null → **`"Baixo"`**
- e) `{Alto, Moderado, Baixo}`
- f) limiares: PIB em **%**, desemprego em **% da força de trabalho**

**`impacto_bolsa`** — `:81-88`
- a)/b) ramos completos, derivados de `cenario_macro`:
  `"Expansao Economica"` → `"Positivo - Favoravel para acoes ciclicas"`;
  `"Desaceleracao"` → `"Negativo - Favora acoes defensivas"`;
  `"Recessao"` → `"Muito Negativo - Ouro e defensivas"`; **else** → `"Neutro"`
- c) coluna `cenario_macro` calculada acima
- d) `cenario_macro` nunca é null (tem else), então o else aqui só é atingido
  por `"Transicao"` → **`"Neutro"`**
- e) 4 valores

**`data_processamento`** — `:89`.

### 3.5 `src/gold/correlacao_acoes_cambio.py` — ação × câmbio

**Entrada.** `<...>_silver.acoes` (colunas `date,ticker,close,
variacao_diaria_pct,volume,ano,mes`, `:29-32`) e `<...>_silver.bcb` filtrada
`indicador = 'cambio_usd_brl'`, renomeando `data→date` e `valor→cambio`
(`:34-38`).
**Saída.** `<...>_gold.acoes_vs_cambio` (`:112-114`), 13 colunas (`:102-108`).

**Agregação diária por ativo** (`:42-50`) — campos derivados:
`preco_medio` = `round(avg(close),2)` [BRL]; `variacao_media_pct` =
`round(avg(variacao_diaria_pct),4)` [%]; `volume_dia` = `round(sum(volume),0)`
[ações] *(não vai para a saída final)*; `ano` = `first(ano)`; `mes` =
`first(mes)`.
*(Como a silver já tem uma linha por `(date,ticker)`, esta agregação é
efetivamente um passa-através — mas `first(ano)`/`first(mes)` seriam
não-determinísticos se houvesse duplicata.)*

**Join** (`:56`) — ver §8, join #3. Seguido de `dropna(subset=["cambio"])`
(`:57`): **dias de bolsa sem cotação de câmbio são descartados** (ex.: feriado
bancário com pregão) — decisão de domínio relevante.

**`cambio_variacao_pct`** — `:58-64`
- a) `round((cambio - lag(cambio)) / lag(cambio) * 100, 4)` sobre
  `Window.partitionBy("ticker").orderBy("date")`
- b) sem ramos
- c) coluna `cambio` (← `silver.bcb.valor` onde `indicador='cambio_usd_brl'`)
- d) **primeira data de cada ticker → `lag` null → null**; `lag(cambio) = 0` →
  divisão por zero → null; sem guarda
- e) real, tipicamente `[-5, 5]`
- f) **%**
- **Nota:** particionar por `ticker` replica o mesmo cálculo de variação
  cambial 9 vezes (uma por ticker) — é redundante mas correto, desde que todos
  os tickers tenham a mesma grade de datas. Se um ticker tiver buracos de
  pregão, sua variação cambial será calculada contra a data anterior **daquele
  ticker**, não do dia anterior de calendário. Ver §11.

**`correlacao_cambio`** — `:72-77`
- a) `round( covar_pop(variacao_media_pct, cambio_variacao_pct) /
  (stddev_pop(variacao_media_pct) * stddev_pop(cambio_variacao_pct)), 4)`,
  todos os três agregados sobre a **mesma janela móvel de 90 dias**
  `window_90d = Window.partitionBy("ticker").orderBy("date")
  .rangeBetween(-90*86400, 0)` (`:68`)
- b) sem ramos
- c) `variacao_media_pct` (de `silver.acoes.variacao_diaria_pct`) e
  `cambio_variacao_pct` (de `silver.bcb.valor`)
- d) **se qualquer um dos dois desvios-padrão populacionais for 0, o
  denominador é 0 → divisão por zero → null. Não há guarda.** Primeiras datas
  da janela têm poucos pontos; `stddev_pop` com 1 ponto = 0 → **null**
- e) teoricamente `[-1, 1]`; pode extrapolar levemente por arredondamento
- f) adimensional (coeficiente de correlação de Pearson populacional)

**`sensibilidade_cambio`** — `:78-83`
- a)/b) ramos completos, **avaliados nesta ordem**:
  1. `correlacao_cambio > 0.5` → `"Alta Positiva (Exportadora)"`
  2. `correlacao_cambio > 0.2` → `"Moderada Positiva"`
  3. `correlacao_cambio < -0.5` → `"Alta Negativa (Importadora)"`
  4. `correlacao_cambio < -0.2` → `"Moderada Negativa"`
  5. **else** → `"Baixa Correlacao"`
- c) `correlacao_cambio`
- d) null → todas falsas → **`"Baixa Correlacao"`** (falso negativo)
- e) 5 valores
- f) adimensional

**`alerta_desacoplamento`** — `:84-90`
- a)/b) 2 ramos: `abs(variacao_media_pct) > 5 AND abs(cambio_variacao_pct) < 1`
  → `"Sim - Acao se desacoplou do cambio"`; **else** → `"Nao"`
- c) `variacao_media_pct`, `cambio_variacao_pct`
- d) qualquer null → **`"Nao"`**
- e) 2 valores
- f) limiares em **%** (ação move mais de 5% enquanto o câmbio move menos de 1%)

**`recomendacao`** — `:91-100`
- a)/b) ramos completos:
  1. `correlacao_cambio > 0.3 AND cambio_variacao_pct > 2` →
     `"Comprar - Exportadora com cambio em alta"`
  2. `correlacao_cambio < -0.3 AND cambio_variacao_pct > 2` →
     `"Vender - Importadora com cambio em alta"`
  3. **else** → `"Neutro"`
- c) `correlacao_cambio`, `cambio_variacao_pct`
- d) qualquer null → **`"Neutro"`**
- e) 3 valores
- **Assimetria de domínio:** só há regra para **câmbio em alta**
  (`cambio_variacao_pct > 2`). Câmbio em queda forte nunca gera recomendação.
- **Nota de conformidade:** este campo emite recomendação de compra/venda de
  ativo. É conteúdo sujeito a regulação de análise/consultoria de valores
  mobiliários — sinalizado aqui como risco de domínio, não como bug.

**`data_processamento`** — `:101`.

### 3.6 `src/gold/fraude.py` — detecção de fraude em ordens (batch)

**Entrada.** `<...>_silver.ordens` (todas as colunas, `:23`) e
`<...>_gold.score_risco_clientes` (só `hash_cliente, score_risco,
categoria_risco, limite_operacional`, `:24-28`).
**Saída.** `<...>_gold.deteccao_fraude` via `saveAsTable` overwrite (`:94-98`).

**Regra de estratégia de join** (`:44-63`) — ver §8, join #2:
- `MAX_LINHAS_BROADCAST = 5_000_000` (`:44`)
- `total_linhas = df_score.count()` (`:47`) — barato no Delta porque sai dos
  metadados; `score_risco_clientes` tem 1 linha por cliente
- `use_broadcast = total_linhas < 5.000.000` (`:48`)
- **fallback** (`:51-56`): se a contagem falhar por qualquer motivo,
  `use_broadcast = False` → sort-merge, que funciona em qualquer tamanho

**As 4 regras de alerta de fraude:**

**`alerta_valor_alto`** — `:67-69`
- a)/b) `valor_total > limite_operacional` → `True`; **else** `False`
- c) `silver.ordens.valor_total` × `gold.score_risco_clientes.limite_operacional`
- d) **`limite_operacional` null** (cliente presente em `ordens` mas ausente de
  `score_risco_clientes` — o join é `left`) → comparação null → cai no
  `otherwise` → **`False`**. Ou seja, **um cliente desconhecido nunca dispara
  este alerta**: falha para o lado permissivo.
- e) `{true, false}` — nunca null
- f) ambos os lados em **BRL**

**`alerta_volume_suspeito`** — `:70-71`
- a)/b) `quantidade > 9000` → `True`; **else** `False`
- c) `silver.ordens.quantidade`
- d) null → `False`
- e) `{true, false}`
- f) **unidades (ações)**. Como `ordens_simuladas.py:64` sorteia
  `randint(100,10000)`, **≈10% das ordens disparam este alerta por construção
  do simulador** (§7, achado C-11).

**`alerta_preco_atipico`** — `:72-74`
- a)/b) `preco > 90 OR preco < 12` → `True`; **else** `False`
- c) `silver.ordens.preco`
- d) null → `False`
- e) `{true, false}`
- f) **BRL**. Com `uniform(10,100)` no simulador, ≈12,2% das ordens caem fora
  da faixa `[12, 90]` por construção.

**`alerta_perfil_incompativel`** — `:75-79`
- a)/b) `perfil_risco == "Conservador" AND valor_total > 200000` → `True`;
  **else** `False`
- c) `silver.ordens.perfil_risco` (propagado desde `bronze.clientes`,
  `classificar_perfil`) e `silver.ordens.valor_total`
- d) qualquer null → `False`
- e) `{true, false}`
- f) limite em **BRL**
- Regra de *suitability*: ordem grande incompatível com o perfil declarado.

**`total_alertas`** — `:80-84`
- a) `cast(alerta_valor_alto as int) + cast(alerta_volume_suspeito as int) +
  cast(alerta_preco_atipico as int) + cast(alerta_perfil_incompativel as int)`
- b) sem ramos
- c) as 4 colunas booleanas acima
- d) nenhuma delas é null (todas têm `otherwise`), logo **nunca é null**
- e) **inteiro em `{0,1,2,3,4}`**
- f) contagem
- **Todos os 4 alertas pesam igual** — não há ponderação.

**`score_fraude`** — `:85-89`
- a)/b) ramos completos: `total_alertas >= 3` → `"Critico"`;
  `== 2` → `"Alto"`; `== 1` → `"Medio"`; **else** → `"Normal"`
- c) `total_alertas`
- d) `total_alertas` nunca null → else só é atingido por `0`
- e) `{Critico, Alto, Medio, Normal}`

**`requer_revisao`** — `:90-91`
- a)/b) `total_alertas >= 2` → `True`; **else** `False`
- c) `total_alertas`
- d) n/a
- e) `{true, false}`
- Equivale a `score_fraude IN ('Alto','Critico')`.

**`data_processamento`** — `:92`. **Nota:** `silver.ordens` já traz uma coluna
`data_processamento` (`silver_ordens.py:28`); este `withColumn` **sobrescreve**
com a data do job de fraude.

### 3.7 `jobs/job_corretora_analises.py` — carteira e score de risco

Job com **lógica de negócio inline**, sem módulo `src/` correspondente. Produz
**5 tabelas gold**.

#### GOLD 1 — `gold.posicao_clientes` (`:30-66`)

**Entrada.** `<...>_silver.ordens` (`:32`) e `<...>_silver.clientes`
(`hash_cliente, perfil_risco, faixa_saldo, score_credito`, `:34-37`).
**Agrupamento:** `hash_cliente, ticker` (`:42`).
**Saída.** `<...>_gold.posicao_clientes` overwrite (`:63-65`).

**`quantidade_liquida`** — `:44-45`
- a) `sum( CASE WHEN tipo = 'compra' THEN quantidade ELSE -quantidade END )`
- b) 2 ramos (`when` compra / `otherwise` negativo)
- c) `silver.ordens.tipo`, `.quantidade`
- d) **o `otherwise` captura tudo que não é `'compra'`** — inclui `'venda'`,
  mas também qualquer valor futuro ou null. `quantidade` null → a parcela é
  null e `sum` a ignora.
- e) inteiro, **pode ser negativo** (posição vendida a descoberto)
- f) **unidades (ações)**
- **⚠ Regra contada sobre TODAS as ordens, inclusive `cancelada` e
  `pendente`** — ver §7, achado C-09.

**`total_comprado`** — `:46-47` — a)
`sum(CASE WHEN tipo='compra' THEN valor_total ELSE 0 END)`; b) 2 ramos;
c) `silver.ordens.tipo`, `.valor_total`; d) else = **0** (não null), então o
campo nunca é null se houver ao menos uma linha; e) ≥ 0; f) **BRL**.

**`total_vendido`** — `:48-49` — a)
`sum(CASE WHEN tipo='venda' THEN valor_total ELSE 0 END)`; demais itens
idênticos. **Assimetria:** `total_comprado` usa `tipo='compra'` e
`total_vendido` usa `tipo='venda'` — um `tipo` inesperado não entra em nenhum
dos dois, mas entra em `quantidade_liquida` como venda.

**`total_ordens`** — `:50` — `count("*")` do grupo; ≥ 1; contagem.

**`ordens_executadas`** — `:51` — `sum(CASE WHEN status='executada' THEN 1
ELSE 0 END)`; ≥ 0; contagem.

**`ordens_canceladas`** — `:52` — `sum(CASE WHEN status='cancelada' THEN 1
ELSE 0 END)`; ≥ 0; contagem. *(ordens `pendente` não são contadas em nenhum
dos dois.)*

**`valor_investido`** — `:54`
- a) `round(total_comprado - total_vendido, 2)`
- b) sem ramos
- c) as duas agregações acima
- d) nunca null (ambos os insumos têm `ELSE 0`)
- e) real, **pode ser negativo**
- f) **BRL**

**`resultado_estimado`** — `:55`
- a) `round(total_vendido - total_comprado, 2)`
- b) sem ramos; c) idem; d) nunca null; e) real; f) **BRL**
- **É exatamente `-valor_investido`** — os dois campos são o mesmo número com
  sinal trocado, gravados lado a lado (§7, achado C-10). Chamar isso de
  "resultado" é enganoso: **não há preço de mercado atual na conta**, logo não
  é P&L (marcação a mercado); é apenas o fluxo de caixa líquido invertido.

**`situacao`** — `:56-59`
- a)/b) ramos completos: `quantidade_liquida > 0` → `"Comprado"`;
  `< 0` → `"Vendido a Descoberto"`; **else** → `"Zerado"`
- c) `quantidade_liquida`
- d) `quantidade_liquida` null (grupo todo com `quantidade` null) → else →
  **`"Zerado"`**; zero tem ramo próprio (o else)
- e) `{Comprado, Vendido a Descoberto, Zerado}`

**`data_processamento`** — `:60`.

**Join** (`:61`) — ver §8, join #1. Traz `perfil_risco`, `faixa_saldo`,
`score_credito` do cadastro. **É `left`**, então um `hash_cliente` presente em
`ordens` mas ausente de `clientes` produz esses três campos **null** — o que
propaga para o score (abaixo).

#### GOLD 2 — `gold.score_risco_clientes` (`:68-119`)

**Entrada.** o próprio `df_posicao` em memória (`:70`), **não relido do disco**.
**Agrupamento:** `hash_cliente, perfil_risco, faixa_saldo, score_credito`
(`:71`).
**Saída.** `<...>_gold.score_risco_clientes` overwrite (`:116-118`).

**`num_ativos`** — `:73` — `count("ticker")` — como `df_posicao` tem uma linha
por `(cliente, ticker)`, equivale ao número de tickers na carteira. ≥ 1.

**`total_ordens`** — `:74` — `sum(total_ordens)` das posições do cliente. ≥ 1.

**`total_canceladas`** — `:75` — `sum(ordens_canceladas)`. ≥ 0.

**`valor_total_investido`** — `:76` — `sum(valor_investido)`; **BRL**; pode ser
negativo.

**`posicoes_descoberto`** — `:77-78` — `sum(CASE WHEN situacao='Vendido a
Descoberto' THEN 1 ELSE 0 END)`; ≥ 0; contagem de ativos vendidos a descoberto.

**`resultado_medio`** — `:79` — `round(avg(resultado_estimado), 2)`; **BRL**;
média por ativo do cliente.

**`taxa_cancelamento_pct`** — `:81-82`
- a) `round(total_canceladas / total_ordens * 100, 2)`
- b) sem ramos
- c) as duas agregações acima
- d) **`total_ordens` nunca é 0** (é soma de `count(*)` de grupos não vazios,
  ≥ 1), então **não há divisão por zero na prática** — mas também **não há
  guarda explícita**
- e) `[0, 100]`
- f) **%**

**`score_credito_norm`** — `:83-84`
- a) `round(score_credito / 850 * 100, 2)`
- b) sem ramos
- c) `gold.posicao_clientes.score_credito` ← `silver.clientes.score_credito`
  ← `bronze.clientes.score_credito` ← `CreditScore` do Kaggle
- d) **`score_credito` null (cliente sem cadastro, ver join `left` acima) →
  `score_credito_norm` null → contamina o `score_risco` inteiro.** Não há
  `coalesce`. O divisor `850` é literal e nunca zero.
- e) `[0, 100]` se `score_credito ∈ [0, 850]`; **valores acima de 850
  produziriam > 100** — não há clamp
- f) adimensional (índice 0–100). O `850` é o teto da escala de score de
  crédito adotado.

**`score_perfil`** — `:85-88`
- a)/b) ramos completos: `perfil_risco == "Arrojado"` → `100`;
  `== "Moderado"` → `60`; **else** → `30`
- c) `perfil_risco` (← `classificar_perfil`, `clientes_kaggle.py:24-30`)
- d) `perfil_risco` null → else → **`30`** (o mesmo valor de `"Conservador"`)
- e) `{100, 60, 30}` — inteiro
- f) pontos (0–100)
- **Semântica invertida a notar:** score **alto** = perfil **Arrojado**. Como o
  `score_risco` final é interpretado como "**maior = menor risco**"
  (`>= 70` → `"Baixo Risco"`, `:105`), esta regra afirma que **ser arrojado
  reduz o risco do cliente**. Ver §7, achado C-12.

**`score_saldo`** — `:89-93`
- a)/b) ramos completos: `faixa_saldo == "Alto"` → `100`; `== "Medio"` → `60`;
  `== "Baixo"` → `30`; **else** → `10`
- c) `faixa_saldo` (← `classificar_saldo`, `clientes_kaggle.py:33-41`)
- d) o else cobre `"Sem saldo"` **e** null → **`10`**
- e) `{100, 60, 30, 10}`
- f) pontos (0–100)

**`score_comportamento`** — `:94-97`
- a)/b) ramos completos: `taxa_cancelamento_pct < 20` → `100`;
  `< 50` → `60`; **else** → `20`
- c) `taxa_cancelamento_pct`
- d) `taxa_cancelamento_pct` nunca null aqui; else cobre `>= 50`
- e) `{100, 60, 20}`
- f) pontos (0–100); limiares em **%** de ordens canceladas

**`score_risco`** — `:98-103`
- a) **`score_risco = round( score_credito_norm × 0.4 + score_perfil × 0.2 +
  score_saldo × 0.2 + score_comportamento × 0.2 , 2)`**
- b) sem ramos (os ramos estão nos 4 componentes)
- c) origem de cada insumo:
  - `score_credito_norm` ← `silver.clientes.score_credito / 850 × 100`
  - `score_perfil` ← `silver.clientes.perfil_risco`
  - `score_saldo` ← `silver.clientes.faixa_saldo`
  - `score_comportamento` ← `silver.ordens.status` (via `ordens_canceladas` /
    `total_ordens`)
- d) **`score_credito_norm` null → toda a soma vira null → `score_risco` null.**
  Os outros 3 componentes nunca são null (todos têm `otherwise`). Não há
  `coalesce` nem valor default.
- e) `[0, 100]` quando todos os insumos existem (mínimo teórico
  `0×0.4 + 30×0.2 + 10×0.2 + 20×0.2 = 12`; máximo `100`); **null** quando o
  cliente não tem cadastro
- f) pontos, escala 0–100. **Maior = melhor** (menor risco).
- **⚠ Divergência com a documentação do repositório: §7, achado C-01.**

**`categoria_risco`** — `:104-108`
- a)/b) ramos completos: `score_risco >= 70` → `"Baixo Risco"`;
  `>= 50` → `"Risco Moderado"`; `>= 30` → `"Risco Alto"`;
  **else** → `"Risco Critico"`
- c) `score_risco`
- d) **`score_risco` null → todas as comparações falsas → `"Risco Critico"`.**
  Um cliente sem cadastro é classificado como **Risco Crítico**, o pior balde —
  falha para o lado conservador aqui, mas silenciosamente.
- e) `{Baixo Risco, Risco Moderado, Risco Alto, Risco Critico}` — **4
  categorias**
- **⚠ §7, achado C-02:** a documentação do repositório descreve **3**.

**`limite_operacional`** — `:109-113`
- a)/b) ramos completos, derivados de `categoria_risco`:
  `"Baixo Risco"` → `500000`; `"Risco Moderado"` → `200000`;
  `"Risco Alto"` → `50000`; **else** (`"Risco Critico"`) → `10000`
- c) `categoria_risco` (que nunca é null)
- d) sem null possível
- e) `{500000, 200000, 50000, 10000}` — inteiro
- f) **BRL** — é o teto por ordem usado em
  `src/gold/fraude.py:68` (`alerta_valor_alto`)

**`data_processamento`** — `:114`.

#### GOLD 3 — `gold.perfil_clientes` (`:121-138`)

**Entrada.** `<...>_silver.clientes` via SQL (`:122-134`).
**Agrupamento:** `perfil_risco, faixa_etaria, score_categoria, pais` (`:132`).

| Campo | a) fórmula | c) origem | d) nulo/zero | e) domínio | f) unidade |
|---|---|---|---|---|---|
| `total_clientes` | `COUNT(*)` (`:125`) | linhas do grupo | — | ≥ 1 | contagem |
| `saldo_medio` | `ROUND(AVG(saldo), 2)` (`:126`) | `silver.clientes.saldo` | nulos ignorados; grupo todo null → null | real | **BRL** |
| `score_medio` | `ROUND(AVG(score_credito), 0)` (`:127`) | `silver.clientes.score_credito` | idem | `[0,850]` | pontos |
| `salario_medio` | `ROUND(AVG(salario_estimado), 2)` (`:128`) | `silver.clientes.salario_estimado` | idem | > 0 | **BRL** *(unidade da fonte Kaggle; ver §11)* |
| `total_churn` | `SUM(CASE WHEN churn THEN 1 ELSE 0 END)` (`:129`) | `silver.clientes.churn` | `churn` null → ELSE → 0 | ≥ 0 | contagem |
| `taxa_churn_pct` | `ROUND(AVG(CASE WHEN churn THEN 1.0 ELSE 0.0 END) * 100, 2)` (`:130`) | idem | null → 0.0 (conta como não-churn) | `[0, 100]` | **%** |

#### GOLD 4 — `gold.ordens_consolidadas` (`:140-154`)

**Entrada.** `<...>_silver.ordens` (`:147`), **sem filtro de status**.
**Agrupamento:** `ticker, perfil_risco, faixa_saldo, tipo, status, ano` (`:148`).

| Campo | fórmula | origem | nulo | domínio | unidade |
|---|---|---|---|---|---|
| `total_ordens` | `COUNT(*)` (`:143`) | linhas | — | ≥ 1 | contagem |
| `volume_total` | `ROUND(SUM(valor_total), 2)` (`:144`) | `silver.ordens.valor_total` | nulos ignorados | ≥ 0 | **BRL** |
| `preco_medio` | `ROUND(AVG(preco), 2)` (`:145`) | `silver.ordens.preco` | idem | `[10,100]` | **BRL** |
| `qtd_media` | `ROUND(AVG(quantidade), 0)` (`:146`) | `silver.ordens.quantidade` | idem | `[100,10000]` | ações |

#### GOLD 5 — `gold.ranking_acoes_perfil` (`:156-170`)

**Entrada.** `<...>_silver.ordens` **filtrada `WHERE status = 'executada'`**
(`:163`) — é a única gold deste job que filtra por status.
**Agrupamento:** `ticker, perfil_risco` (`:164`).
Campos: `total_ordens` = `COUNT(*)`; `volume_total` = `ROUND(SUM(valor_total),2)`
[BRL]; `preco_medio` = `ROUND(AVG(preco),2)` [BRL] (`:159-161`).
Ordenação por `volume_total DESC` (`:165`) — mas **não há coluna de rank
materializada**; a ordem é perdida na leitura do Delta.

---

## 4. GOLD STREAMING — `src/gold/streaming_gold.py`

Todas as 4 funções leem `<...>_silver.streaming` inteira (não incrementalmente)
e cruzam com `<...>_gold.performance_acoes` **restrita ao ano mais recente**
(`WHERE ano = (SELECT MAX(ano) FROM ...)`, `:44,:130,:203,:274`). Todas gravam
com `mode("overwrite")`: as 4 tabelas são **snapshots recalculados**, não
históricos.

### 4.1 `detectar_fraude_streaming` → `gold.fraude_streaming` (`:18-97`)

**Entrada.** `<...>_silver.streaming` (`:37`) + `performance_acoes`
(`ticker, preco_medio, volatilidade`, `:41-45`).
**Saída.** `<...>_gold.fraude_streaming`, 17 colunas (`:79-87`).
**Join** — §8, join #4, broadcast (justificativa no comentário `:48-49`: 9
tickers × 1 ano = 9 linhas).

**`alerta_volume_suspeito`** — `:53-54` — a)/b) `quantidade > 9000` → `True`;
else `False`; c) `silver.streaming.quantidade`; d) null → `False`;
e) `{true,false}`; f) **unidades**.

**`alerta_preco_atipico`** — `:55-57` — a)/b) `preco > 90 OR preco < 12` →
`True`; else `False`; c) `silver.streaming.preco`; d) null → `False`;
e) `{true,false}`; f) **BRL**.
**⚠ Ver §7, C-06 e C-07:** a mesma faixa aparece com **outros valores** na
silver, e os preços simulados pelo producer garantem alerta permanente para
parte dos tickers.

**`alerta_valor_elevado`** — `:58-59`
- a)/b) `valor_total > 500000` → `True`; **else** `False`
- c) `silver.streaming.valor_total` (= `round(preco × quantidade, 2)`,
  `jobs/job_streaming_continuous.py:92`)
- d) null → `False`
- e) `{true, false}`
- f) **BRL — limite fixo**, não o `limite_operacional` por cliente do batch
  (§7, achado C-04). Faz sentido: o fluxo de streaming **não tem
  `hash_cliente`** (schema em `job_streaming_continuous.py:62-70`), logo não há
  como buscar o limite do cliente.

**`alerta_desvio_historico`** — `:60-65`
- a) `abs(preco - preco_medio) > (preco_medio × volatilidade / 100 × 2)` →
  `True`; **else** `False`
- b) 2 ramos
- c) `silver.streaming.preco` × `gold.performance_acoes.preco_medio` e
  `.volatilidade` (do ano mais recente)
- d) **join `left`: ticker do streaming ausente de `performance_acoes` →
  `preco_medio` e `volatilidade` null → comparação null → `otherwise` →
  `False`.** É exatamente o que acontece com `RENT3.SA`, `B3SA3.SA` e
  `HAPV3.SA`, que os producers emitem e o Yahoo não ingere (§7, achado C-03).
  `volatilidade` null (ticker com 1 só observação em `performance`) → também
  `False`.
- e) `{true, false}`
- f) `preco` e `preco_medio` em **BRL**; `volatilidade` em **%** — daí o
  `/100`. O fator **2** é o número de desvios-padrão tolerados.

**`total_alertas`** — `:66-70` — soma dos 4 casts para `int`; nunca null;
domínio `{0,1,2,3,4}`; contagem; **pesos iguais**.

**`score_fraude`** — `:71-75` — `>= 3` → `"Critico"`; `== 2` → `"Alto"`;
`== 1` → `"Medio"`; **else** → `"Normal"`. Idêntico ao batch.

**`requer_revisao`** — `:76-77` — `total_alertas >= 2` → `True`; else `False`.

**`data_processamento`** — `:78`.

### 4.2 `detectar_anomalias_intraday` → `gold.anomalias_intraday` (`:100-163`)

**Entrada.** `<...>_silver.streaming` agregada por `ticker, hora` (`:116-123`)
+ `performance_acoes` (`ticker, preco_medio, volatilidade`, `:127-131`).
**Join** — §8, join #5, broadcast.

**Agregações por (ticker, hora)** — `:119-122`:
`preco_medio_hora` = `round(avg(preco),2)` [BRL];
`valor_total_hora` = `round(sum(valor_total),2)` [BRL];
`volume_hora` = `sum(quantidade)` [ações];
`total_transacoes_hora` = `count("*")` [contagem].

**`desvio_historico_rs`** — `:139-140`
- a) `round(preco_medio × volatilidade / 100, 4)`
- b) sem ramos
- c) `gold.performance_acoes.preco_medio` [BRL] e `.volatilidade` [%]
- d) qualquer null (ticker fora de `performance_acoes`) → null
- e) ≥ 0 ou null
- f) **BRL** — converte a volatilidade percentual em desvio monetário

**`zscore_intraday`** — `:141-146`
- a) `round( (preco_medio_hora - preco_medio) /
  CASE WHEN desvio_historico_rs = 0 THEN 1 ELSE desvio_historico_rs END , 4)`
- b) **2 ramos no denominador** (`when ... == 0 then 1 otherwise ...`)
- c) `preco_medio_hora` (agregado do streaming) e `preco_medio` /
  `desvio_historico_rs` (de `performance_acoes`)
- d) **divisão por zero está guardada**: denominador 0 vira 1 — isto é, o
  z-score passa a ser a diferença de preço **em reais**, não em desvios.
  **Nulo NÃO está guardado**: `desvio_historico_rs` null → o `when` é falso →
  `otherwise` devolve null → resultado null.
- e) real ou null
- f) adimensional quando o denominador é o desvio; **BRL** quando o denominador
  degradou para 1 — **a unidade do campo muda conforme o ramo** (§7, achado
  C-08)

**`anomalia`** — `:147-148` — `abs(zscore_intraday) > 2` → `True`; else
`False`; null → `False`.

**`tipo_anomalia`** — `:149-152` — `> 2` → `"Alta Anormal"`;
`< -2` → `"Queda Anormal"`; **else** → `"Normal"`; null → `"Normal"`.

**`data_processamento`** — `:153`.

### 4.3 `calcular_volume_intraday` → `gold.volume_intraday` (`:166-235`)

**Entrada.** `silver.streaming` agregada por `ticker, hora` (`:185-196`) +
`performance_acoes` (`ticker, volume_medio`, `:200-204`).
**Join** — §8, join #6, broadcast.

**Agregações** — `:188-195`:
`total_transacoes` = `count("*")`; `volume_hora` = `sum(quantidade)` [ações];
`valor_total_hora` = `round(sum(valor_total),2)` [BRL];
`preco_medio_hora` = `round(avg(preco),2)` [BRL];
**`volume_compras`** = `sum(CASE WHEN tipo='compra' THEN quantidade ELSE 0 END)`
(`:192-193`) — o `ELSE 0` garante não-nulo;
**`volume_vendas`** = `sum(CASE WHEN tipo='venda' THEN quantidade ELSE 0 END)`
(`:194-195`).

**`pct_volume_diario`** — `:210-215`
- a) `round( volume_hora /
  CASE WHEN volume_medio = 0 THEN 1 ELSE volume_medio END × 100 , 2)`
- b) **2 ramos no denominador**
- c) `volume_hora` (agregado do streaming, unidades) e
  `gold.performance_acoes.volume_medio` (média **diária** de volume do ativo)
- d) **divisão por zero guardada** (denominador vira 1); **null não guardado**:
  `volume_medio` null → `otherwise` devolve null → resultado null
- e) `[0, +∞)` ou null; pode passar de 100 se a hora superar o dia médio
- f) **%** — "que fração do volume médio diário do ativo foi negociada nesta
  hora"

**`alerta_volume_intraday`** — `:216-219`
- a)/b) ramos completos: `pct_volume_diario > 30` → `"Critico"`;
  `> 15` → `"Alto"`; **else** → `"Normal"`
- c) `pct_volume_diario`
- d) null → **`"Normal"`**
- e) `{Critico, Alto, Normal}`
- f) limiares em **%** do volume médio diário

**`pressao_compradora`** — `:220-223`
- a)/b) ramos completos: `volume_compras > volume_vendas` → `"Compra"`;
  `volume_vendas > volume_compras` → `"Venda"`; **else** → `"Neutro"`
- c) `volume_compras`, `volume_vendas`
- d) nunca null (ambos têm `ELSE 0`); empate (inclusive `0 = 0`) → `"Neutro"`
- e) `{Compra, Venda, Neutro}`

**`data_processamento`** — `:224`.

### 4.4 `calcular_ranking_realtime` → `gold.ranking_acoes_realtime` (`:238-303`)

**Entrada.** `silver.streaming` agregada por `ticker` (`:256-267`) +
`performance_acoes` (`ticker, empresa, setor, preco_medio AS
preco_medio_historico`, `:271-275`).
**Join** — §8, join #7, broadcast.

**Agregações** — `:259-266`: `total_transacoes` = `count("*")`;
`volume_total` = `sum(quantidade)` [ações];
`valor_total` = `round(sum(valor_total),2)` [BRL];
`preco_medio_atual` = `round(avg(preco),2)` [BRL];
`total_compras` = `sum(CASE WHEN tipo='compra' THEN 1 ELSE 0 END)`;
`total_vendas` = `sum(CASE WHEN tipo='venda' THEN 1 ELSE 0 END)`;
`preco_minimo` = `round(min(preco),2)` [BRL];
`preco_maximo` = `round(max(preco),2)` [BRL].

**`variacao_vs_historico_pct`** — `:281-286`
- a) `round( (preco_medio_atual - preco_medio_historico) /
  CASE WHEN preco_medio_historico = 0 THEN 1 ELSE preco_medio_historico END
  × 100 , 2)`
- b) **2 ramos no denominador**
- c) `preco_medio_atual` (streaming) e
  `gold.performance_acoes.preco_medio` (ano mais recente)
- d) **zero guardado**; **null não guardado** → resultado null para ticker fora
  de `performance_acoes`
- e) real ou null
- f) **%**

**`tendencia`** — `:287-290`
- a)/b) ramos completos: `variacao_vs_historico_pct > 1` → `"Alta"`;
  `< -1` → `"Queda"`; **else** → `"Estavel"`
- c) `variacao_vs_historico_pct`
- d) null → **`"Estavel"`**
- e) `{Alta, Queda, Estavel}`
- f) limiar em **%**

**`data_processamento`** — `:291`.

**`rank_volume`** — `:292-293`
- a) `orderBy(desc(valor_total))` seguido de
  `(monotonically_increasing_id() + 1).cast("int")`
- b) sem ramos
- c) `valor_total` agregado
- d) —
- e) inteiro ≥ 1
- f) posição
- **⚠ Confiabilidade:** `monotonically_increasing_id()` **não gera uma sequência
  densa 1..N**. O valor é `(partition_id << 33) | row_number_na_partição`, então
  com mais de uma partição os números saltam (ex.: 1, 2, 8589934593, ...). O
  campo é **monotônico na ordem correta**, mas **não é uma posição de ranking
  legível**. Ver §7, achado C-14.

---

## 5. SCD, OBSERVABILIDADE, QUALIDADE E SEGURANÇA

### 5.1 `src/clients/scd.py` — historização SCD Type 2

**Função genérica `aplicar_scd_type2(spark, df_novos, tabela_uc, chave)`**
(`:12-65`).

**Campos de controle adicionados a todo registro novo** (`:21-24`):
- **`data_inicio`** — a) `lit(now() %Y-%m-%d)` (`:22`); b) —; c) relógio do
  driver; d) n/a; e) data ISO; f) data.
- **`data_fim`** — a) `lit("9999-12-31")` (`:23`); b) —; c) constante
  sentinela; d) n/a; e) `"9999-12-31"` para o registro vigente, ou a data de
  fechamento; f) data.
- **`atual`** — a) `lit(True)` (`:24`); b) —; c) constante; d) n/a;
  e) `{true,false}`; f) booleano.

**Regra de fechamento** (`:32-41`): MERGE com condição
`antigo.<chave> = novo.<chave> AND antigo.atual = true`, e para os casados
`whenMatchedUpdate(data_fim = hoje, atual = false)`.
**Regra de inserção** (`:44-48`): `append` de `df_com_scd` (todos os registros
novos, já marcados como atuais).

**⚠ Consequência de domínio:** o fechamento **não compara atributos**. Toda
execução fecha o registro vigente e insere um novo, **mesmo que nada tenha
mudado**. Com o `pipeline_completo` rodando diariamente às 06:00
(`databricks.yml:780-782`), a tabela cresce em `nº de clientes` linhas por dia,
e **todo registro histórico tem `data_inicio = data_fim` no dia seguinte** —
o histórico produzido é de execuções, não de mudanças de atributo. Ver §7,
achado C-15.

**Primeira carga** (`:53-65`): se a exceção contiver `"is not a Delta table"`
ou `"Table or view not found"`, grava com `overwrite`; **qualquer outra exceção
é relançada** (`:65`).

**Aplicação 1 — `aplicar_scd_clientes`** (`:68-91`)
- Entrada: `<...>_silver.clientes`, 10 colunas (`:74-80`): `id_cliente,
  hash_cliente, sobrenome_masked, perfil_risco, score_credito, faixa_saldo,
  faixa_etaria, score_categoria, ativo, churn`
- Saída: `<...>_silver.clientes_scd`, chave `hash_cliente` (`:82`)
- Retorno: `COUNT(*) WHERE atual = true` (`:84-88`)
- **Atributos historizados** (o "o que mudou" que o negócio pode consultar):
  perfil de investidor, score de crédito, faixa de saldo, faixa etária,
  categoria de score, status de atividade e churn.

**Aplicação 2 — `aplicar_scd_score_risco`** (`:94-117`)
- Entrada: `<...>_gold.score_risco_clientes`, 9 colunas (`:100-106`):
  `hash_cliente, perfil_risco, faixa_saldo, score_credito, score_risco,
  categoria_risco, limite_operacional, num_ativos, total_ordens`
- Saída: `<...>_gold.score_risco_scd`, chave `hash_cliente` (`:108`)
- Retorno: `COUNT(*) WHERE atual = true` (`:110-114`)
- **Historiza a trilha de crédito/risco**: o score, a categoria e o **limite
  operacional** de cada cliente ao longo do tempo — é o que permite auditar
  "qual era o limite deste cliente na data da ordem".

### 5.2 `src/observability/monitoring.py` — métricas de qualidade

**Entrada.** 14 tabelas listadas literalmente em `:90-105` (6 silver + 8 gold).
**Saída.** lista de dicionários → `<...>_gold.observabilidade`
(`jobs/job_observabilidade.py:33-37`, overwrite).

**Campos derivados (9)** — `:54-64`:

**`camada`** — `:22-25`
- a) `tabela_uc.split(".")[1].split("_", 1)[-1]`
- b) sem ramos
- c) o próprio FQN da tabela (ex.: `case_santander.hk_silver.ordens`)
- d) se o FQN tiver menos de 3 partes, `IndexError` — capturado pelo `except`
  geral (`:80-82`) e a tabela é pulada
- e) `{silver, gold}` na prática (a lista de `:90-105` só tem essas duas)
- f) — . **Regra explícita:** remove o prefixo de ambiente para que a métrica
  não mude de nome entre `hk` e `prod` (comentário `:23-24`).

**`tabela`** — `:26` — `tabela_uc.split(".")[2]`, o nome curto.

**`data_verificacao`** — `:57` — `now()` `%Y-%m-%d`.

**`total_registros`** — `:30` — `df.count()`. **Se for 0**, loga
`[ALERTA CRITICO] ... Sem registros!` e **retorna `{}`** (`:32-34`) → a tabela
**não entra** em `gold.observabilidade`. Ou seja, tabela vazia desaparece da
observabilidade em vez de aparecer com zero.

**`total_nulos`** — `:36-38`
- a) `sum( para cada coluna c: sum(cast(c IS NULL as int)) )` — soma de nulos
  **de todas as colunas de todas as linhas**
- b) sem ramos
- c) todas as colunas da tabela monitorada
- d) —
- e) inteiro ≥ 0
- f) contagem de células

**`versao_cdf`** — `:45-48`
- a) `DESCRIBE HISTORY <tabela> LIMIT 1` → campo `version`
- b) `try/except` — **se falhar, `versao_cdf = 0`** (`:47-48`)
- c) log de transações Delta da tabela
- d) exceção → `0` (não null)
- e) inteiro ≥ 0
- f) número de versão Delta
- **Papel de domínio:** é a **marca d'água do CDC**. Os jobs
  `job_streaming_to_gold*.py` leem
  `SELECT COALESCE(MAX(versao_cdf), 0) FROM <gold>.observabilidade WHERE
  tabela = 'streaming'` (`jobs/job_streaming_to_gold_continuous.py:56-60`;
  `jobs/job_streaming_to_gold.py:60-64`) e usam esse número como
  `startingVersion` do Change Data Feed.

**`total_duplicatas`** — `:50`
- a) `total - df.dropDuplicates().count()` — duplicatas considerando **a linha
  inteira**, não uma chave de negócio
- b) sem ramos; c) a tabela monitorada; d) —; e) ≥ 0; f) contagem de linhas

**`qualidade_pct`** — `:51`
- a) **`qualidade_pct = round( (1 - total_nulos / (total_registros ×
  nº_de_colunas)) × 100 , 2)`**
- b) sem ramos
- c) `total_nulos` (`:36-38`), `total_registros` (`:30`) e `len(df.columns)`
- d) **`total_registros = 0` seria divisão por zero — mas o caminho é
  inalcançável**, porque `:32-34` já retornou `{}` nesse caso. Tabela com 0
  colunas: impossível em Delta.
- e) `[0, 100]`
- f) **%** — é uma métrica de **densidade de células preenchidas**, não de
  correção do dado.

**`tempo_seg`** — `:63` — `round((now() - inicio).total_seconds(), 2)`,
segundos de execução do check.

**Alertas** (`:73-76`, apenas log, sem efeito no fluxo):
- `qualidade_pct < 95` → `logger.error("[ALERTA CRITICO] ...")`
- `total_duplicatas > 0` → `logger.warning("[ALERTA] ...")`

### 5.3 `src/quality/data_quality.py` — framework de gates

Ver §9 para o comportamento (bloqueia vs decorativo). Resumo das 7 validações:

| Método | Linha | O que valida | Reprova com |
|---|---|---|---|
| `validate_completeness(df, required_columns)` | `:30-48` | todas as colunas obrigatórias existem em `df.columns` | `DataQualityError("Colunas faltando: ...")` |
| `validate_uniqueness(df, key_column)` | `:50-68` | `groupBy(key).count()` sem nenhum `count > 1` | `DataQualityError` |
| `validate_nulls(df, max_null_percentage=0.05)` | `:70-95` | **para cada coluna**, `nulos/total <= 5%` (default) | `DataQualityError` |
| `validate_schema_drift(df, expected_schema)` | `:97-125` | conjunto de colunas idêntico **e** `dataType.typeName()` igual | `SchemaDriftError` |
| `validate_row_count(df, min_rows=1)` | `:127-144` | `df.count() >= min_rows` | `DataQualityError` |
| `validate_range(df, column, min_val, max_val)` | `:146-165` | nenhum valor fora de `[min,max]` | `DataQualityError` |
| `validate_consistency(df, column, allowed_values)` | `:167-185` | nenhum valor fora da lista permitida | `DataQualityError` |

**Guarda de DataFrame vazio em `validate_nulls`** (`:83-85`): se
`total == 0`, loga warning e **retorna sem validar** — evita a divisão por zero
de `:89` e é a única guarda de divisão explícita do framework.

**`run_all_validations`** (`:187-228`): despacha por chave; validação
desconhecida gera apenas `warning` (`:222-223`); qualquer exceção é logada e
**relançada** (`:224-226`) — o gate é **bloqueante por construção**.

### 5.4 `src/security/hashing.py` — ver §10.

---

## 6. TABELA DE LIMIARES E CONSTANTES DO DOMÍNIO

**96 itens.** Todo número mágico com significado de negócio, onde está e o que
significa.

### 6.1 Classificação de cliente

| # | Constante | Onde | Significado |
|---|---|---|---|
| 1 | `750` | `src/ingestion/clientes_kaggle.py:26` | corte de score de crédito para perfil **Arrojado** |
| 2 | `600` | `:28` | corte para perfil **Moderado**; abaixo → Conservador |
| 3 | `0` | `:35` | saldo exatamente zero → faixa **"Sem saldo"** |
| 4 | `50000` | `:37` | teto da faixa de saldo **Baixo** (BRL) |
| 5 | `150000` | `:39` | teto da faixa de saldo **Medio** (BRL); acima → Alto |
| 6 | `30` | `src/transformation/silver_clientes.py:26` | idade — teto da faixa **Jovem** |
| 7 | `50` | `:27` | idade — teto da faixa **Adulto**; acima → Senior |
| 8 | `750` | `:30` | score de crédito — **Excelente** |
| 9 | `650` | `:31` | score de crédito — **Bom** |
| 10 | `550` | `:32` | score de crédito — **Regular**; abaixo → Ruim |

### 6.2 Score de risco e limite operacional

| # | Constante | Onde | Significado |
|---|---|---|---|
| 11 | `850` | `jobs/job_corretora_analises.py:84` | **teto da escala de score de crédito**, usado para normalizar em 0–100 |
| 12 | `100 / 60 / 30` | `:86-88` | pontos por perfil: Arrojado / Moderado / **else** (Conservador ou null) |
| 13 | `100 / 60 / 30 / 10` | `:90-93` | pontos por faixa de saldo: Alto / Medio / Baixo / **else** (Sem saldo ou null) |
| 14 | `20` | `:95` | taxa de cancelamento (%) abaixo da qual o comportamento vale 100 pontos |
| 15 | `50` | `:96` | taxa de cancelamento (%) abaixo da qual vale 60; acima → 20 |
| 16 | `100 / 60 / 20` | `:95-97` | pontos de comportamento |
| 17 | **`0.4`** | `:100` | **peso do score de crédito** no score de risco |
| 18 | **`0.2`** | `:101` | **peso do perfil de investidor** |
| 19 | **`0.2`** | `:102` | **peso da faixa de saldo** |
| 20 | **`0.2`** | `:103` | **peso do comportamento (cancelamentos)** |
| 21 | `70` | `:105` | score ≥ 70 → **Baixo Risco** |
| 22 | `50` | `:106` | score ≥ 50 → **Risco Moderado** |
| 23 | `30` | `:107` | score ≥ 30 → **Risco Alto**; abaixo → Risco Critico |
| 24 | `500000` | `:110` | **limite operacional (BRL) de Baixo Risco** |
| 25 | `200000` | `:111` | limite operacional (BRL) de Risco Moderado |
| 26 | `50000` | `:112` | limite operacional (BRL) de Risco Alto |
| 27 | `10000` | `:113` | limite operacional (BRL) de Risco Critico (**default**) |

### 6.3 Fraude — batch (`src/gold/fraude.py`)

| # | Constante | Onde | Significado |
|---|---|---|---|
| 28 | `5_000_000` | `:44` | `MAX_LINHAS_BROADCAST` — acima disso, sort-merge em vez de broadcast |
| 29 | `9000` | `:71` | quantidade (ações) acima da qual há **alerta de volume suspeito** |
| 30 | `90` | `:73` | preço (BRL) acima do qual há **alerta de preço atípico** |
| 31 | `12` | `:73` | preço (BRL) abaixo do qual há alerta de preço atípico |
| 32 | `"Conservador"` + `200000` | `:77-78` | **regra de suitability**: perfil conservador com ordem acima de R$200.000 |
| 33 | `3` | `:86` | total de alertas ≥ 3 → `score_fraude = "Critico"` |
| 34 | `2` | `:87` | total de alertas = 2 → `"Alto"` |
| 35 | `1` | `:88` | total de alertas = 1 → `"Medio"`; 0 → `"Normal"` |
| 36 | `2` | `:91` | total de alertas ≥ 2 → `requer_revisao = true` |

### 6.4 Fraude e análise — streaming (`src/gold/streaming_gold.py`)

| # | Constante | Onde | Significado |
|---|---|---|---|
| 37 | `9000` | `:54` | quantidade — alerta de volume suspeito (streaming) |
| 38 | `90` / `12` | `:56` | faixa de preço atípico (BRL) — streaming |
| 39 | **`500000`** | `:59` | **valor total (BRL) por transação** acima do qual há alerta — limite fixo |
| 40 | **`2`** | `:63` | fator multiplicador do desvio histórico (nº de desvios-padrão tolerados) |
| 41 | `100` | `:63,:140,:214,:285` | divisor de conversão de **percentual → fração** (volatilidade e razões) |
| 42 | `3 / 2 / 1` | `:72-74` | cortes de `score_fraude` (idênticos ao batch) |
| 43 | `2` | `:77` | `requer_revisao` |
| 44 | `0` → `1` | `:144-145` | guarda de divisão por zero do `zscore_intraday` |
| 45 | `2` / `-2` | `:148,:150,:151` | limiar de anomalia intraday (\|z\| > 2) |
| 46 | `0` → `1` | `:213-214` | guarda de divisão por zero do `pct_volume_diario` |
| 47 | `30` | `:217` | `pct_volume_diario` (%) acima do qual o volume intraday é **Critico** |
| 48 | `15` | `:218` | `pct_volume_diario` (%) acima do qual é **Alto** |
| 49 | `0` → `1` | `:284-285` | guarda de divisão por zero da variação vs histórico |
| 50 | `1` / `-1` | `:288-289` | limiar (%) de `tendencia` Alta / Queda; entre eles → Estavel |
| 51 | `MAX(ano)` | `:44,:130,:203,:274` | **janela de referência histórica: só o ano mais recente de `performance_acoes`** |

### 6.5 Anomalias e análise macro (batch)

| # | Constante | Onde | Significado |
|---|---|---|---|
| 52 | `2` / `-2` | `src/gold/anomalias.py:27,:29,:30` | limiar de z-score para anomalia de preço diário |
| 53 | `7*86400` | `src/gold/bcb_analise.py:36` | janela de **7 dias** (em segundos) para média móvel |
| 54 | `30*86400` | `:37` | janela de **30 dias** para volatilidade da Selic |
| 55 | `365*86400` | `:38` | janela de **12 meses** para IPCA acumulado |
| 56 | `5` | `:59` | variação cambial (%) vs média 7d acima da qual o alerta é **Critico** |
| 57 | `2` | `:60` | variação cambial (%) acima da qual o alerta é **Alto** |
| 58 | `5` | `:63` | IPCA acumulado 12m (%) acima do qual a inflação é **Critico** |
| 59 | `3` | `:64` | IPCA acumulado 12m (%) acima do qual é **Alto** |
| 60 | `2` | `src/gold/world_bank_analise.py:48` | crescimento do PIB (%) para **Crescimento Alto** |
| 61 | `0` | `:49` | crescimento do PIB (%) para **Crescimento Moderado** |
| 62 | `-2` | `:50` | crescimento do PIB (%) para **Queda Moderada**; abaixo → Queda Severa |
| 63 | `-1` | `:53` | variação do desemprego (p.p.) para **Melhorando (Rápido)** |
| 64 | `0` | `:54` | variação do desemprego (p.p.) para **Melhorando** |
| 65 | `1` | `:55` | variação do desemprego (p.p.) para **Piorando**; acima → Piorando (Rápido) |
| 66 | `2` e `0` | `:59` | combinação de `cenario_macro` = **Expansao Economica** |
| 67 | `0` e `0` | `:63` | combinação = **Desaceleracao** |
| 68 | `0` e `1` | `:67` | combinação = **Recessao**; resto → Transicao |
| 69 | `-2` / `15` | `:73` | PIB (%) ou desemprego (%) que geram `alerta_risco = "Alto"` |
| 70 | `0` / `10` | `:77` | PIB (%) ou desemprego (%) que geram `alerta_risco = "Moderado"` |
| 71 | `90*86400` | `src/gold/correlacao_acoes_cambio.py:68` | **janela móvel de 90 dias** para a correlação ação × câmbio |
| 72 | `0.5` | `:79` | correlação acima da qual o ativo é **Alta Positiva (Exportadora)** |
| 73 | `0.2` | `:80` | correlação para **Moderada Positiva** |
| 74 | `-0.5` | `:81` | correlação abaixo da qual é **Alta Negativa (Importadora)** |
| 75 | `-0.2` | `:82` | correlação para **Moderada Negativa**; resto → Baixa Correlacao |
| 76 | `5` e `1` | `:86-87` | ação move > 5% enquanto câmbio move < 1% → **desacoplamento** |
| 77 | `0.3` e `2` | `:93` | correlação > 0,3 com câmbio subindo > 2% → **Comprar** |
| 78 | `-0.3` e `2` | `:97` | correlação < -0,3 com câmbio subindo > 2% → **Vender** |

### 6.6 Streaming — camada silver

| # | Constante | Onde | Significado |
|---|---|---|---|
| 79 | **`8000`** | `jobs/job_streaming_continuous.py:94` (e `jobs/job_streaming.py:88`) | quantidade acima da qual `alerta_volume = "Volume Alto"` |
| 80 | **`5000`** | `:95` / `:89` | quantidade acima da qual `alerta_volume = "Volume Medio"` |
| 81 | **`80`** | `:98` / `:92` | preço (BRL) acima do qual `alerta_preco = "Preco Alto"` |
| 82 | **`15`** | `:99` / `:93` | preço (BRL) abaixo do qual `alerta_preco = "Preco Baixo"` |
| 83 | `processingTime='1 minute'` | `:111` | latência-alvo do micro-batch |
| 84 | `cloudFiles.maxFilesPerTrigger = 1` | `:77` | throttling do Auto Loader |

### 6.7 Simulação e geração de dado

| # | Constante | Onde | Significado |
|---|---|---|---|
| 85 | **`42`** | `src/ingestion/ordens_simuladas.py:22` | `ORDENS_SEED` — seed de reprodutibilidade das ordens |
| 86 | `1000` | `:23` | `QTD_CLIENTES_AMOSTRA` — clientes sorteados por execução |
| 87 | `1..10` | `:62` | ordens geradas por cliente |
| 88 | `10 .. 100` | `:63` | faixa de preço sorteado (BRL) |
| 89 | `100 .. 10000` | `:64` | faixa de quantidade sorteada (ações) |
| 90 | `2024-01-01` + `0..457` | `:65` | janela de datas das ordens simuladas |
| 91 | **`42`** | `scripts/eventhub_producer.py:20` e `scripts/eventhub_producer_advanced.py:38` | seed dos producers de streaming |
| 92 | `0.95 .. 1.05` | `producer_advanced.py:72` | variação de ±5% sobre o preço-base do ticker |
| 93 | mapa `precos_base` (2,5 a 180,0) | `producer_advanced.py:64-69` | preço-base por ticker (BRL) |

### 6.8 Qualidade, retenção e operação

| # | Constante | Onde | Significado |
|---|---|---|---|
| 94 | **`0.05`** | `src/quality/data_quality.py:70` | máximo de nulos por coluna (5%) — default do gate |
| 95 | **`95`** | `src/observability/monitoring.py:73` | `qualidade_pct` abaixo de 95% → log de alerta crítico |
| 96 | `168 HOURS` | `jobs/job_observabilidade.py:61` | janela de retenção do `VACUUM` (7 dias) |

### 6.9 Constantes de infraestrutura com efeito de domínio (complemento)

| Constante | Onde | Significado |
|---|---|---|
| `"2y"` | `src/ingestion/yahoo_finance.py:50` | histórico de cotações extraído |
| `0.5s` | `:58` | pausa fixa entre tickers |
| séries `11`, `1`, `433` | `src/ingestion/bcb.py:158,161,164` | Selic, câmbio USD/BRL, IPCA |
| `01/04/2021` – `01/04/2026` | `:29-30` | janela obrigatória da API SGS |
| `timeout=30`, `max_retries=3`, `2**tentativa` | `:39,:59,:68` | política de rede do BCB |
| `per_page=30`, `timeout=10` | `src/ingestion/world_bank.py:46-47` | política de rede do World Bank |
| `NY.GDP.MKTP.KD.ZG`, `SL.UEM.TOTL.ZS` | `:80-81` | indicadores PIB e desemprego |
| rate limits `15/45/90/5` (hk) e `30/120/300/20` (prod) req/min | `src/config/environment.py:90-95,105-110` | teto de chamadas por API e ambiente |
| `60` | `src/ingestion/api_wrapper.py:36,46,62` | janela de rate limit (s) e default de limite |
| `data_retention_days` 30 (hk) / 90 (prod) | `environment.py:96,111` | retenção declarada por ambiente |
| `"9999-12-31"` | `src/clients/scd.py:23` | sentinela de registro vigente no SCD |
| `0 0 6 * * ?` | `databricks.yml:780` | pipeline diário às 06:00 (America/Sao_Paulo) |
| `0 */5 * * * ?` | `databricks.yml:282` | streaming→gold a cada 5 minutos |

---

## 7. REGRAS CONTRADITÓRIAS OU DUPLICADAS

**13 achados.** Cada um confirmado nos dois pontos do código.

### C-01 — 🔴 `score_risco`: a fórmula documentada não é a implementada

| | Componentes | Onde |
|---|---|---|
| **Implementado** | `score_credito_norm ×0.4` + **`score_perfil` ×0.2** + **`score_saldo` ×0.2** + `score_comportamento ×0.2` | `jobs/job_corretora_analises.py:98-103` |
| **Documentado** | `score_credito ×0.40` + **`score_atividade` ×0.20** ("frequência de uso da plataforma") + **`score_diversif` ×0.20** ("diversificação de carteira") + `score_comportam ×0.20` | `notebooks/case_presentation.py:592-595` |

Os pesos batem; **dois dos quatro componentes não existem no código**. Não há
nenhuma coluna `score_atividade` nem `score_diversif` em lugar nenhum do
repositório. O que o código realmente pondera em 20% cada é o **perfil de
investidor declarado** e a **faixa de saldo** — dois atributos de cadastro, não
de comportamento. Quem ler o notebook para explicar o score a um regulador vai
descrever um modelo que o pipeline não implementa.

### C-02 — 🔴 `categoria_risco`: 4 categorias no código, 3 na documentação

| | Faixas | Onde |
|---|---|---|
| **Implementado** | `>=70` Baixo Risco → **R$500.000**; `>=50` Risco Moderado → **R$200.000**; `>=30` Risco Alto → **R$50.000**; **else Risco Critico → R$10.000** | `jobs/job_corretora_analises.py:104-113` |
| **Documentado** | `70–100` Baixo Risco → R$500.000; **`40–70`** Risco Moderado → R$200.000; **`0–40`** "Alto Risco" → R$50.000 | `notebooks/case_presentation.py:598-600` |

Três divergências simultâneas: **(a)** o corte de Risco Moderado é **50** no
código e **40** na documentação — um cliente com score 45 é "Risco Alto"
(limite R$50.000) no sistema e "Risco Moderado" (limite R$200.000) na
documentação; **(b)** a categoria **`"Risco Critico"` e o limite de R$10.000
não são documentados em lugar nenhum**; **(c)** o rótulo difere:
`"Risco Alto"` no código (`:107`) vs `"Alto Risco"` na consulta SQL do notebook
(`case_presentation.py:634`, `WHERE s.categoria_risco = 'Alto Risco'`) — **essa
consulta retorna zero linhas sempre**, porque o valor gravado é `"Risco Alto"`.

### C-03 — 🔴 Universo de tickers: producers emitem 3 ativos que o pipeline não conhece

| Fonte | Tickers | Onde |
|---|---|---|
| Extração Yahoo / simulação de ordens | `PETR4, VALE3, ITUB4, BBDC4, ABEV3, MGLU3, WEGE3, BBAS3, SANB11` (9) | `src/config/settings.py:2-12`; `src/ingestion/ordens_simuladas.py:25-28`; mapa `empresa`/`setor` em `src/transformation/silver_acoes.py:43-60` |
| Producers de streaming | `PETR4, VALE3, ITUB4, BBDC4, BBAS3, WEGE3, **RENT3**, MGLU3, **B3SA3**, **HAPV3**` (10) | `scripts/eventhub_producer.py:27-30`; `scripts/eventhub_producer_advanced.py:45-48` |

**Duas listas diferentes.** `ABEV3` e `SANB11` estão no pipeline e não nos
producers; `RENT3`, `B3SA3` e `HAPV3` estão nos producers e **não** no
pipeline. Consequência mensurável: os 4 joins de `streaming_gold.py` são `left`
contra `performance_acoes`, que só tem os 9 tickers do Yahoo → para esses 3
ativos, `preco_medio`, `volatilidade`, `volume_medio`, `empresa` e `setor` vêm
**null**, o que faz `alerta_desvio_historico` = `False` (`:60-65`),
`zscore_intraday` = null → `anomalia` = `False` (`:141-148`),
`pct_volume_diario` = null → `alerta_volume_intraday` = `"Normal"`
(`:210-219`) e `variacao_vs_historico_pct` = null → `tendencia` = `"Estavel"`
(`:281-290`). **30% dos ativos do fluxo de streaming são estruturalmente
invisíveis à detecção de anomalia e desvio.**

### C-04 — 🟠 "Valor elevado": limite por cliente no batch, limite fixo no streaming

| Regra | Fórmula | Onde |
|---|---|---|
| `alerta_valor_alto` (batch) | `valor_total > limite_operacional` — **R$500.000 / 200.000 / 50.000 / 10.000 conforme o cliente** | `src/gold/fraude.py:67-69` |
| `alerta_valor_elevado` (streaming) | `valor_total > 500000` — **fixo** | `src/gold/streaming_gold.py:58-59` |

Mesmo conceito de negócio ("operação de valor incompatível"), duas
implementações com **até 50× de diferença** no limiar para o mesmo cliente. O
motivo é estrutural — o schema de streaming
(`jobs/job_streaming_continuous.py:62-70`) **não tem `hash_cliente`**, então não
há como consultar o limite do cliente — mas o efeito é que o mesmo cliente de
Risco Crítico dispara alerta a partir de R$10.000 no batch e só a partir de
R$500.000 no streaming.

### C-05 — 🟠 Duas classificações do mesmo `score_credito`, com cortes diferentes

| Campo | Cortes | Onde |
|---|---|---|
| `perfil_risco` | **750** / **600** / else | `src/ingestion/clientes_kaggle.py:26-30` |
| `score_categoria` | **750** / **650** / **550** / else | `src/transformation/silver_clientes.py:30-33` |

Ambos derivam da mesma coluna. Batem em 750, divergem no segundo corte
(**600 vs 650**) e o `score_categoria` tem um quarto balde que o `perfil_risco`
não tem. Um cliente com score 620 é **"Moderado"** e **"Regular"** ao mesmo
tempo — as duas taxonomias contam histórias diferentes sobre o mesmo cliente, e
**apenas o `perfil_risco` entra no `score_risco`** (`job_corretora_analises.py:86-88`).

### C-06 — 🟠 "Volume suspeito": 8.000/5.000 na silver, 9.000 na gold

| Regra | Limiar | Onde |
|---|---|---|
| `alerta_volume` (silver.streaming) | `> 8000` → "Volume Alto"; `> 5000` → "Volume Medio" | `jobs/job_streaming_continuous.py:93-96` |
| `alerta_volume_suspeito` (gold.fraude_streaming) | `> 9000` | `src/gold/streaming_gold.py:53-54` |
| `alerta_volume_suspeito` (gold.deteccao_fraude) | `> 9000` | `src/gold/fraude.py:70-71` |

**Três limiares na mesma linhagem.** A tabela `silver.streaming` grava
`alerta_volume = "Volume Alto"` para uma transação de 8.500 ações, e a gold
derivada **dessa mesma linha** grava `alerta_volume_suspeito = false`. Os dois
campos coexistem em tabelas consultadas pelo mesmo dashboard. Não há nenhuma
constante compartilhada — os três números são literais inline.

### C-07 — 🟠 "Preço atípico": faixa `[15, 80]` na silver, `[12, 90]` na gold

| Regra | Faixa normal | Onde |
|---|---|---|
| `alerta_preco` (silver.streaming) | `[15, 80]` — fora disso "Preco Alto"/"Preco Baixo" | `jobs/job_streaming_continuous.py:97-100` |
| `alerta_preco_atipico` (gold.fraude_streaming) | `[12, 90]` | `src/gold/streaming_gold.py:55-57` |
| `alerta_preco_atipico` (gold.deteccao_fraude) | `[12, 90]` | `src/gold/fraude.py:72-74` |

Mesma incoerência de C-06, no eixo de preço. Uma transação a R$85 é
`"Preco Alto"` na silver e **não atípica** na gold.

**Agravante de domínio:** os producers geram preços-base de **R$2,50 (MGLU3)**
a **R$180,00 (WEGE3)** com ±5% (`producer_advanced.py:64-72`). Contra a faixa
`[12, 90]`, isso significa que **toda** transação de `WEGE3` (~R$171–189) e
**toda** de `MGLU3` (~R$2,37–2,63) dispara `alerta_preco_atipico`
permanentemente, e `B3SA3` (base R$12,00, faixa ~R$11,40–12,60) fica oscilando
em torno do corte. A regra de preço atípico foi calibrada para a faixa
`uniform(10,100)` das **ordens simuladas** (`ordens_simuladas.py:63`), não para
os preços dos **producers de streaming**.

### C-08 — 🟡 Z-score de anomalia: mesmo limiar, fórmulas e guardas diferentes

| | Denominador | Guarda de zero | Onde |
|---|---|---|---|
| Batch (`gold.anomalias`) | `stddev(variacao_diaria_pct)` sobre toda a história do ticker | **nenhuma** — `std = 0` ou null → z null → `anomalia = false` | `src/gold/anomalias.py:23-27` |
| Intraday (`gold.anomalias_intraday`) | `desvio_historico_rs` = `preco_medio × volatilidade / 100` | `CASE WHEN = 0 THEN 1` | `src/gold/streaming_gold.py:139-148` |

O limiar `|z| > 2` e os rótulos (`"Alta Anormal"`, `"Queda Anormal"`,
`"Normal"`) são idênticos, mas **o z-score não é a mesma grandeza**: no batch é
o desvio da *variação percentual diária*; no intraday é o desvio do *preço em
reais*. E a guarda de divisão por zero existe só num dos dois — no intraday,
quando ela dispara, `zscore_intraday` deixa de ser adimensional e passa a ser
**uma diferença em BRL comparada contra o limiar 2**, ou seja, "preço da hora
desviou mais de R$2,00 do histórico" vira "anomalia". A unidade do campo muda
sem aviso conforme o ramo.

### C-09 — 🟡 Posição de carteira computa ordens canceladas e pendentes

`jobs/job_corretora_analises.py:44-49` calcula `quantidade_liquida`,
`total_comprado` e `total_vendido` sobre **todas** as linhas de
`silver.ordens`, sem filtrar `status`. As mesmas linhas geram
`ordens_canceladas` em `:52`. Ou seja: **uma ordem cancelada aumenta a posição
do cliente e simultaneamente é contada como cancelada.**

A contraprova de que isso é inconsistência e não decisão está no mesmo arquivo:
`gold.ranking_acoes_perfil` (`:163`) filtra explicitamente
`WHERE status = 'executada'`, e `gold.ordens_consolidadas` (`:141-150`) mantém
`status` como **dimensão de agrupamento**. Três tratamentos diferentes de
`status` em cinco tabelas do mesmo job. Com o simulador sorteando os três status
uniformemente (`ordens_simuladas.py:82`), **≈2/3 das ordens que compõem a
posição nunca foram executadas**.

### C-10 — 🟡 `valor_investido` e `resultado_estimado` são o mesmo número invertido

```
valor_investido    = round(total_comprado - total_vendido, 2)   (:54)
resultado_estimado = round(total_vendido - total_comprado, 2)   (:55)
```
`resultado_estimado ≡ -valor_investido`, exatamente. Dois campos gravados em
`gold.posicao_clientes` que carregam informação idêntica com nomes que sugerem
conceitos diferentes. Além disso, `resultado_estimado` **não é um resultado
financeiro**: não há preço de mercado na conta, logo não há marcação a mercado
nem P&L. Ele é propagado para `score_risco_clientes.resultado_medio` (`:79`),
onde herda a mesma ambiguidade.

### C-11 — 🟡 Limiares de fraude calibrados contra o próprio simulador

Os alertas de fraude são disparados pela mecânica do gerador, não por
comportamento anômalo:

| Regra | Limiar | Distribuição do gerador | Taxa de disparo estrutural |
|---|---|---|---|
| `alerta_volume_suspeito` | `quantidade > 9000` (`fraude.py:71`) | `randint(100, 10000)` (`ordens_simuladas.py:64`) | ≈ **10%** das ordens |
| `alerta_preco_atipico` | `preco > 90 or < 12` (`fraude.py:73`) | `uniform(10, 100)` (`:63`) | ≈ **12,2%** das ordens |

Não é um bug de código, mas é uma regra de negócio cuja calibragem **só faz
sentido para o dado sintético**. Se `bronze.ordens` passar a receber ordens
reais, os dois limiares perdem significado — a faixa `[12, 90]` foi escolhida
para recortar as caudas de uma distribuição uniforme `[10, 100]`, não a faixa
de preços real da B3 (onde WEGE3 negocia acima de R$90 o tempo todo — ver C-07).

### C-12 — 🟡 `score_perfil` premia o perfil arrojado num score de risco

`jobs/job_corretora_analises.py:85-88` atribui **100 pontos ao perfil
Arrojado** e **30 ao Conservador**. Como `score_risco` alto significa
**Baixo Risco** (`:105`), a regra afirma que **ser arrojado reduz o risco do
cliente**.

Isso contradiz a regra de fraude do mesmo pipeline: `src/gold/fraude.py:75-79`
trata `perfil_risco == "Conservador"` com operação grande como
**incompatibilidade de suitability** — ou seja, lá o perfil conservador é um
sinal de risco quando desalinhado. E contradiz a própria origem do perfil:
`perfil_risco` deriva do **score de crédito** (`clientes_kaggle.py:24-30`), que
já entra no `score_risco` com peso 0,4 via `score_credito_norm`. **O score de
crédito é contado duas vezes**: uma diretamente (40%) e outra através do
`perfil_risco` (20%), somando **60% de peso efetivo** para a mesma variável de
origem — não os 40% que a documentação declara.

### C-13 — 🟡 `desemprego_variacao_pct` não é percentual

`src/gold/world_bank_analise.py:43-46` calcula
`desemprego - lag(desemprego)`, uma **diferença absoluta em pontos
percentuais**, enquanto o campo irmão `pib_variacao_pct` (`:38-42`) calcula uma
**razão percentual**. Os dois nomes terminam em `_pct` e são consumidos lado a
lado nas regras de `cenario_macro` (`:57-70`) e `tendencia_desemprego`
(`:52-56`), onde os limiares `-1`, `0`, `1` só fazem sentido lidos como p.p.
Um consumidor que interprete `desemprego_variacao_pct = -1` como "desemprego
caiu 1%" está errando por uma ordem de grandeza.

### C-14 — 🟡 `rank_volume` não é uma posição de ranking

`src/gold/streaming_gold.py:292-293` usa
`(monotonically_increasing_id() + 1).cast("int")` após um `orderBy`. Essa
função **não produz uma sequência densa 1..N**: o valor é
`(id_da_partição << 33) | número_da_linha_na_partição`, então com mais de uma
partição os valores saltam para a casa dos bilhões. O `cast("int")` sobre um
valor > 2³¹ produz **overflow silencioso** (valor negativo ou truncado). A
ordem relativa é preservada, mas o campo **não é legível como "1º, 2º, 3º"** —
que é exatamente o que o nome e a docstring (`:246`, "posicao no ranking")
prometem. A implementação correta seria `row_number() over (order by
valor_total desc)`.

### C-15 — 🟡 SCD Type 2 sem comparação de atributos

`src/clients/scd.py:32-48`: o MERGE fecha o registro vigente com condição
apenas `antigo.chave = novo.chave AND antigo.atual = true` — **não compara os
atributos**. Toda execução fecha o registro atual (`data_fim = hoje`,
`atual = false`) e insere um novo idêntico.

Com o `pipeline_completo` diário (`databricks.yml:780-782`), as tabelas
`silver.clientes_scd` e `gold.score_risco_scd` crescem em `nº de clientes`
linhas **por dia**, e cada versão histórica tem
`data_inicio = data_fim` (fechada no dia seguinte à criação). A promessa de
domínio do SCD Type 2 — "quando este cliente mudou de Conservador para
Arrojado" — **não é entregue**: a tabela registra execuções do job, não
mudanças de atributo. Uma consulta de "clientes que mudaram de perfil" precisa
comparar `perfil_risco` entre versões consecutivas manualmente.

### 7.1 Duplicação de código com regra idêntica (sem divergência de valor)

Não são contradições, mas são pontos onde a mesma regra existe duas vezes e vai
divergir na próxima edição:

| Regra | Local A | Local B |
|---|---|---|
| Transformação completa de `silver.streaming` (7 campos, incluindo os limiares 8000/5000/80/15) | `jobs/job_streaming_continuous.py:88-101` | `jobs/job_streaming.py:82-95` |
| Job inteiro de streaming→gold (leitura CDC + 4 chamadas gold) | `jobs/job_streaming_to_gold_continuous.py:55-98` | `jobs/job_streaming_to_gold.py:58-105` |
| Bloco de score/limite/categoria | `jobs/job_corretora_analises.py:98-113` | (replicado apenas em prosa, `notebooks/case_presentation.py:592-600`) |
| `get_salt()` | `src/security/hashing.py:12-17` | `src/config/secrets.py:71-73` (`get_salt`) |
| Lista de paths ADLS | `src/config/settings.py:15-33` (`get_paths`) | strings inline em cada `src/ingestion/*` e `src/transformation/*` |

`jobs/job_streaming.py` e `jobs/job_streaming_to_gold.py` **não aparecem em
nenhuma task do `databricks.yml`** — são cópias órfãs. Isso é o pior cenário
para divergência: a versão órfã não é exercitada, então uma correção aplicada
só à versão viva passa despercebida.

### 7.2 Contradição entre teste e implementação

`tests/test_data_quality.py:25-33,:57,:84-92` exige que `bronze.clientes`
tenha as colunas **`nome`** e **`score_risco`** (esta última tipada `integer`).
`src/ingestion/clientes_kaggle.py:91-101` produz **16 colunas e nenhuma das
duas**: não há coluna `nome` (o sobrenome vira `sobrenome_masked`, hasheado) e
o campo de score chama-se **`score_credito`**. `score_risco` só existe em
`gold.score_risco_clientes`, criado três camadas adiante.

Os três testes (`test_bronze_clientes_completeness`,
`test_silver_clientes_no_nulls`, `test_schema_drift_detection`) **falham sempre
que a tabela existe**. `tests/conftest.py:39-52` só pula quando a tabela **não
existe** — o `skip` foi deliberadamente movido para fora dos asserts
(comentário `:43-47`). Portanto: ou o CI nunca roda com as tabelas presentes, ou
esses três testes estão vermelhos. **O contrato de schema declarado nos testes
não é o schema que a ingestão produz.**

### 7.3 Divergência entre docstring e comportamento

`src/gold/streaming_gold.py:26` documenta a regra 3 como
`"alerta_valor_elevado : valor_total > R$500.000 por transacao"` — confere com
`:59`. Mas `:24-25` documenta as regras 1 e 2 como
`"quantidade > 9.000"` e `"preco > R$90 ou < R$12"` — corretas para a gold,
**e é exatamente por isso que a divergência com a silver (C-06, C-07) passa
despercebida**: cada arquivo documenta corretamente a si mesmo, e ninguém
documenta a linhagem inteira.

### 7.4 Divergências entre `INVENTARIO.md` (`f7265c7`) e o código (`c2a8811`)

| # | Afirmação do INVENTARIO | Estado em `c2a8811` |
|---|---|---|
| **D-01** | `INVENTARIO.md` §2.4: `src/gold/fraude.py:35-38` monta nome de tabela a partir de um `DataFrame` vazio, levanta `AttributeError` toda execução, `use_broadcast` fica sempre `True` e o ramo sort-merge é inalcançável | **Corrigido.** `src/gold/fraude.py:44-56` usa `MAX_LINHAS_BROADCAST = 5_000_000` e `df_score.count()`; o `except` agora força `use_broadcast = False` (fallback seguro). **Os dois ramos do `if` são alcançáveis.** Os sites de join migraram de `:46/:50` para **`:59/:63`**. |
| **D-02** | `INVENTARIO.md` §5.1 lista os joins de fraude em `:46` e `:50` | Linhas reais: **`:59`** (broadcast) e **`:63`** (sort-merge). |

Também vale registrar que `src/observability/monitoring.py:40-48` **grava**
`versao_cdf` hoje (via `DESCRIBE HISTORY`), embora o comentário do próprio
arquivo (`:42-44`) narre, no passado, que a coluna "nunca era gravada". O
comentário descreve o bug corrigido, não o estado atual — a marca d'água do CDC
**funciona** no código lido.

---

## 8. RELAÇÕES — 8 sites de join, 7 joins lógicos

> **O 9º match de grep, `src/pipeline/dynamic_pipeline.py:279`, é
> `", ".join(self.jobs.keys())`** — string join do Python montando a linha
> `from jobs import ...` de um DAG gerado. **Não é join de dado** e não está
> documentado como tal. Confirmado: é o único `join` daquele arquivo.

| # | Site(s) | Esquerda | Direita | Chave | Tipo | Estratégia | Cardinalidade esperada |
|---|---|---|---|---|---|---|---|
| **1** | `jobs/job_corretora_analises.py:61` | `df_posicao` — `silver.ordens` (`:32`) agregada por `(hash_cliente, ticker)` (`:42`) | `df_clientes` — `silver.clientes`, 4 colunas (`:34-37`) | `hash_cliente` | `left` | **`F.broadcast(df_clientes)` explícito** | **N:1** — muitas posições por cliente × 1 linha de cadastro. Esquerda ≈ 1.000 clientes × até 9 tickers ≈ ≤ 9.000 linhas; direita = 1 linha por cliente do Kaggle (10.000). `left` preserva posições de clientes sem cadastro, com `perfil_risco`/`faixa_saldo`/`score_credito` **null** → `score_risco` null → `categoria_risco = "Risco Critico"` |
| **2** | `src/gold/fraude.py:59` **e** `:63` — **um único join lógico com duas estratégias** | `df_ordens` — `silver.ordens` completa (`:23`) | `df_score` — `gold.score_risco_clientes`, 4 colunas (`:24-28`) | `hash_cliente` | `left` (nos dois ramos) | **decidida em runtime** (`:46-56`): se `df_score.count() < 5.000.000` → `F.broadcast(df_score)` (`:59-60`); senão, ou se a contagem falhar → **sort-merge** sem broadcast (`:61-63`) | **N:1** — muitas ordens por cliente × 1 linha de score. Esquerda ≈ 1.000–10.000 ordens; direita = 1 linha por cliente. Ordem sem score correspondente mantém `limite_operacional` null → `alerta_valor_alto = false` |
| **3** | `src/gold/correlacao_acoes_cambio.py:56` | `df_acoes_diarias` — `silver.acoes` agregada por `(date, ticker)` (`:42-50`) | `df_bcb` — `silver.bcb` filtrada `indicador = 'cambio_usd_brl'`, com `data→date`, `valor→cambio` (`:34-38`) | `date` | `left` | **nenhuma dica**; o Spark decide (a direita é pequena — ~1.200 dias úteis — e provavelmente vira broadcast por AQE, mas **não é declarado**) | **N:1** — 9 tickers por data × 1 cotação por data. Seguido de `dropna(subset=["cambio"])` (`:57`), que **converte o `left` em `inner` na prática**: dias de pregão sem cotação de câmbio são descartados |
| **4** | `src/gold/streaming_gold.py:52` | `df_stream` — `silver.streaming` completa (`:37`) | `df_perf` — `gold.performance_acoes` do `MAX(ano)`, 3 colunas (`:41-45`) | `ticker` | `left` | **`F.broadcast(df_perf)` explícito**; justificativa no comentário `:48-49` (9 linhas) | **N:1** — muitas transações por ticker × 1 linha de benchmark. Direita = **9 linhas**. Tickers `RENT3/B3SA3/HAPV3` não casam (C-03) → `preco_medio`/`volatilidade` null |
| **5** | `src/gold/streaming_gold.py:138` | `df_hora` — `silver.streaming` agregada por `(ticker, hora)` (`:113,116-123`) | `df_perf` — `performance_acoes` do `MAX(ano)`, 3 colunas (`:127-131`) | `ticker` | `left` | **`F.broadcast(df_perf)`**; comentário `:135` estima esquerda ≤ 216 linhas (9 tickers × 24h) | **N:1** — ≤ 24 horas por ticker × 1 benchmark. Direita = 9 linhas |
| **6** | `src/gold/streaming_gold.py:209` | `df_vol` — `silver.streaming` agregada por `(ticker, hora)` (`:182,185-196`) | `df_perf` — `performance_acoes` do `MAX(ano)`, colunas `ticker, volume_medio` (`:200-204`) | `ticker` | `left` | **`F.broadcast(df_perf)`** | **N:1** — ≤ 216 linhas × 9 linhas |
| **7** | `src/gold/streaming_gold.py:280` | `df_rank` — `silver.streaming` agregada por `ticker` (`:253,256-267`) | `df_perf` — `performance_acoes` do `MAX(ano)`, colunas `ticker, empresa, setor, preco_medio_historico` (`:271-275`) | `ticker` | `left` | **`F.broadcast(df_perf)`** | **1:1** — 1 linha por ticker de cada lado; ≤ 10 × 9 linhas |

**Observações transversais:**

- **Nenhum join é `inner`.** Todos são `left`, o que significa que **nenhuma
  linha da esquerda é perdida por falta de correspondência** — o custo é que
  toda a lógica a jusante precisa tratar null, e como visto nas §3–4, **em todos
  os casos o null cai no `otherwise`, ou seja, no ramo "sem alerta"**. O
  desenho falha para o lado permissivo em fraude e para o lado conservador em
  `categoria_risco`.
- **Broadcast é declarado em 6 dos 8 sites.** Os dois sem dica são
  `correlacao_acoes_cambio.py:56` (deixado ao otimizador) e o ramo sort-merge
  deliberado de `fraude.py:63`.
- **As duas chaves de relacionamento do modelo** são `hash_cliente` (joins 1 e
  2, mais os MERGEs de `src/utils/delta.py` e do SCD) e `ticker` (joins 4–7).
  Não há chave estrangeira declarada — Delta não impõe integridade
  referencial.

---

## 9. VALIDAÇÕES — o que bloqueia e o que é decorativo

### 9.1 Gates que BLOQUEIAM (abortam o job)

| # | Onde é invocado | O que valida | O que acontece ao reprovar |
|---|---|---|---|
| **G-1** | `src/transformation/silver_acoes.py:78-82` | `completeness`: colunas `date, ticker, close, volume` presentes · `row_count`: `min_rows = 1` · `nulls`: nenhuma coluna acima de **5%** de nulos | `DataQualityError` propagada por `run_all_validations` (`data_quality.py:224-226`) → **exceção sobe até o `main()` do job → task Databricks falha** e todos os sucessores (`t3_anomalias`, `t3_performance`, `t3_acoes_cambio`) não executam |
| **G-2** | `src/transformation/silver_bcb.py:48-51` | `completeness`: `data, indicador, valor` · `row_count`: `min_rows = 1` | idem — aborta `t2_silver_bcb`, bloqueando `t3_bcb` e `t3_acoes_cambio` |
| **G-3** | `src/transformation/silver_world_bank.py:45-48` | `completeness`: `ano, indicador, valor` · `row_count`: `min_rows = 1` | idem — aborta `t2_silver_world_bank`, bloqueando `t3_world_bank` |
| **G-4** | `src/transformation/silver_world_bank.py:20-25` | existe coluna `ano` **ou** `data` no bronze | `raise Exception("Coluna de ano não encontrada no Bronze!")` — **aborta antes de qualquer escrita** |
| **G-5** | `src/security/hashing.py:14-17` | o secret `salt` é recuperável do Key Vault | `ValueError` → **a ingestão de clientes aborta**. Garante que nunca se grava PII com hash sem salt |
| **G-6** | `src/utils/delta.py:35-44` | a exceção do MERGE é reconhecidamente "tabela não existe" | se **não** for, `raise` — erro real de escrita **não é engolido** |
| **G-7** | `src/clients/scd.py:53-65` | mesma lógica do G-6 para o SCD | `raise e` para qualquer erro que não seja primeira carga |
| **G-8** | `src/config/environment.py:55-56` | `ENVIRONMENT ∈ {hk, prod}` | `ValueError` — nenhum job sobe com ambiente inválido |
| **G-9** | `src/config/tables.py:44-45` | `layer ∈ {bronze, silver, gold}` em `schema_fqn` | `ValueError` |

**⚠ Ponto crítico de ordem em G-1/G-2/G-3:** **o gate roda DEPOIS da escrita.**
Em `silver_acoes.py` a escrita é `:66-71` e o gate é `:78-82`; em
`silver_bcb.py`, escrita `:37-41` / gate `:48-51`; em `silver_world_bank.py`,
escrita `:34-38` / gate `:45-48`. Nos três casos o comentário afirma
*"Gate de qualidade ANTES de publicar"* (`silver_acoes.py:74`,
`silver_bcb.py:44`, `silver_world_bank.py:41`) — **mas o dado ruim já está
gravado no path Delta quando a exceção sobe**. O que o gate efetivamente impede
é (a) o `register_external_table` da linha seguinte (`:89` / `:58` / `:55`),
logo a tabela do Unity Catalog não é criada na primeira execução ruim, e (b) a
execução das tasks gold sucessoras. Mas se a tabela **já existia** de uma
execução anterior, ela agora aponta para o path com o dado reprovado. O gate
**bloqueia o pipeline, não a publicação do dado**.

### 9.2 Validações DECORATIVAS (logam e seguem)

| # | Onde | O que verifica | O que acontece |
|---|---|---|---|
| **D-1** | `src/observability/monitoring.py:73-74` | `qualidade_pct < 95` | `logger.error("[ALERTA CRITICO] ...")` — **a métrica é gravada normalmente em `gold.observabilidade` e o job continua**. Nenhum consumidor lê esse limiar. |
| **D-2** | `src/observability/monitoring.py:75-76` | `total_duplicatas > 0` | `logger.warning` — sem efeito |
| **D-3** | `src/observability/monitoring.py:32-34` | tabela com 0 registros | loga `[ALERTA CRITICO]` e **retorna `{}`** — a tabela some do relatório de observabilidade em vez de aparecer com zero. O job segue. |
| **D-4** | `src/observability/monitoring.py:80-82` | qualquer exceção ao monitorar uma tabela | loga e retorna `{}` — **uma tabela inexistente não derruba o job de observabilidade** |
| **D-5** | `jobs/job_observabilidade.py:58-64` | `OPTIMIZE` + `VACUUM RETAIN 168 HOURS` em 10 tabelas | cada falha é apenas logada via `info` (`:63-64`) — **manutenção Delta que falha passa como sucesso** |
| **D-6** | `jobs/job_lakehouse_monitoring.py:51-56` | criação de monitor por tabela | `"already exists"` → busca o existente; **qualquer outro erro é logado com `info` e ignorado** (`:55-56`). O job termina "com sucesso" mesmo sem criar monitor nenhum |
| **D-7** | `jobs/job_unity_catalog.py:74-75`, `:137-138`, `:159-160` | escrita bronze, Liquid Clustering, habilitação de CDF | todas as três com `except Exception` que só loga. **Se o CDF não for habilitado, o CDC dos jobs de streaming silenciosamente cai no fallback de full scan** (`job_streaming_to_gold_continuous.py:73-78`) |
| **D-8** | `src/ingestion/yahoo_finance.py:60-61` | falha por ticker | logada; o laço continua. **Extração parcial é indistinguível de extração completa a jusante** |
| **D-9** | `src/ingestion/bcb.py:167-171` | séries que falharam | as vazias são filtradas e as demais gravadas; só retorna 0 se **todas** falharem. **Uma silver de BCB sem a série `ipca` faz `bcb_analise.py:52` falhar com `UNRESOLVED_COLUMN`** — a falha é adiada para outro job |
| **D-10** | `src/ingestion/world_bank.py:75-78` | erro de dado por indicador | logado; o indicador vira vazio |
| **D-11** | `src/ingestion/bcb.py:189-190` | erro na gravação do Bronze | **`except` retorna 0** — o job de extração termina com sucesso mesmo sem gravar nada |
| **D-12** | `src/observability/monitoring.py:45-48` | `DESCRIBE HISTORY` para a marca d'água CDC | falha → `versao_cdf = 0` → na próxima execução o CDC lê **do começo** |
| **D-13** | `jobs/job_streaming_to_gold*.py:73-78` / `:80-85` | leitura via Change Data Feed | qualquer exceção → **full scan da tabela inteira**, e as 4 gold são recalculadas sobre tudo. O `df_cdf` calculado no `try` **nunca é usado**: as 4 funções gold releem `silver.streaming` completa (`streaming_gold.py:37,113,182,253`). **O CDC serve apenas como contador para decidir se o job roda** (`:80-82`), não para restringir o que é processado. |
| **D-14** | `src/quality/data_quality.py:222-223` | chave de validação desconhecida em `run_all_validations` | `warning` — **uma validação com nome errado no dicionário é silenciosamente ignorada** |
| **D-15** | `src/ingestion/api_wrapper.py:112-121` | `safe_api_call` | retorna `None` em vez de propagar — **sem call site no repositório** |

### 9.3 Capacidade do framework que nunca é exercitada

Das 7 validações de `DataQualityValidator`, **apenas 3 têm call site em código
executável**: `completeness`, `row_count` e `nulls` (e `nulls` só em
`silver_acoes.py:81`). Não há **nenhuma** invocação de:

- `validate_uniqueness` (`:50-68`) — nenhuma chave de negócio é validada como
  única em produção, apesar de `hash_cliente` e `id_ordem` serem chaves de
  MERGE
- `validate_schema_drift` (`:97-125`) — apesar de a classe `SchemaDriftError`
  existir e de todos os writes usarem `mergeSchema=true`, que **aceita** drift
  silenciosamente
- `validate_range` (`:146-165`) — nenhum limite de sanidade em `preco`,
  `quantidade`, `score_credito` ou `saldo`
- `validate_consistency` (`:167-185`) — nenhum domínio fechado é validado, nem
  os óbvios (`tipo ∈ {compra, venda}`, `status ∈ {executada, cancelada,
  pendente}`, `categoria_risco ∈ {...}`)

Confirmado por varredura: os únicos importadores de `DataQualityValidator` são
os três `src/transformation/silver_*.py` e `tests/test_data_quality_framework.py`.

### 9.4 Validação na origem (fora do framework)

`src/ingestion/bcb.py:62-127` implementa uma cadeia própria de **8 etapas** de
validação HTTP/JSON (detalhada na §1.4), independente do
`DataQualityValidator`. É a validação de entrada mais rigorosa do repositório e
**não usa o framework** — outra duplicação conceitual.

`scripts/eventhub_producer_advanced.py:97-109` valida o schema da transação
antes do envio (campos obrigatórios, `preco` numérico, `quantidade` inteira).

---

## 10. DADOS SENSÍVEIS / LGPD

### 10.1 O que `src/security/hashing.py` protege

| Função | Linha | Aplicada a | Onde é chamada |
|---|---|---|---|
| `hash_with_salt(data, salt=None)` | `:20-45` | qualquer string | núcleo; chamada pelas demais |
| `hash_customer_id(customer_id)` | `:48-53` | identificador do cliente | **`src/ingestion/clientes_kaggle.py:83`** → coluna `hash_cliente` |
| `hash_surname(surname)` | `:56-61` | sobrenome | **`src/ingestion/clientes_kaggle.py:84`** → coluna `sobrenome_masked` |
| `hash_email(email)` | `:64-68` | e-mail | **nenhum call site** — código morto |
| `anonymize_customer_row(row, salt)` | `:71-90` | dicionário; hasheia `CustomerId`, `Surname`, `Email` | **nenhum call site** — código morto |
| `generate_random_salt(length=32)` | `:93-104` | geração de salt (`secrets.token_hex`) | **nenhum call site em produção**; replicado em `scripts/generate_salt.py:10-20` |

**Confirmado por varredura:** os únicos consumidores do módulo em código de
produção são as duas chamadas de `src/ingestion/clientes_kaggle.py:83-84`.
Nenhum outro módulo aplica anonimização.

### 10.2 Mecanismo e garantia real

**Mecanismo** (`:39-43`):
1. concatena `data + salt` — **salt como sufixo**, sem separador
2. codifica em UTF-8
3. `hashlib.sha256(...).hexdigest()` → 64 caracteres hexadecimais

**Origem do salt** (`:12-17` → `src/config/secrets.py:32-37,71-73`): secret de
nome `salt` lido do Key Vault via `dbutils.secrets.get(scope=<key_vault>,
key="salt")`, onde `<key_vault>` = `kv-case-santander-<env>`
(`src/config/environment.py:79`). **O valor nunca aparece no código nem em
log** — só o nome da chave e o escopo. Gerado por `secrets.token_hex(32)`
(`scripts/generate_salt.py:20`), 64 caracteres hex.

**Garantia real — o que é verdadeiro:**
- ✅ **Irreversível por inversão direta.** SHA-256 é one-way; não existe
  operação que devolva a entrada a partir do digest, mesmo com o salt.
- ✅ **Resistente a rainbow table.** É a função declarada do salt na docstring
  (`:33`) e a garantia que o teste `test_salt_diferente_muda_o_hash`
  (`tests/test_hashing.py:36-38`) fixa.
- ✅ **Determinístico e estável.** `tests/test_hashing.py:28-29,47-50` fixam o
  algoritmo — trocar a fórmula invalidaria todos os hashes já gravados, e o
  `hash_cliente` é chave de MERGE, join e SCD em todo o lakehouse.
- ✅ **Falha explícita sem salt.** `get_salt` levanta `ValueError` (`:16-17`);
  `tests/test_hashing.py:53-59` garante que os helpers não devolvem um hash
  sem salt silenciosamente.

**Garantia real — o que NÃO é verdadeiro:**
- ⚠️ **Não é anonimização irreversível no sentido da LGPD; é
  pseudonimização.** O hash é **determinístico**: mesma entrada + mesmo salt =
  mesmo digest, sempre. Quem tiver o salt (acesso ao Key Vault) e uma lista de
  candidatos pode **recalcular e casar** — é um ataque de dicionário, não de
  inversão. Isso é decisivo para `CustomerId`, cujo espaço é pequeno e
  enumerável (inteiros), e para `Surname`, cujo espaço é um dicionário de
  sobrenomes de algumas dezenas de milhares de entradas. **Com o salt em mãos,
  ambos os campos são reidentificáveis em minutos.** A docstring do módulo
  (`:5`) e a de `hash_with_salt` (`:32`) afirmam *"Impossível reverter mesmo
  com acesso ao salt"* — verdadeiro para inversão criptográfica, **enganoso**
  para reidentificação por força bruta.
- ⚠️ **Sem `hmac` nem alongamento de chave.** É `sha256(data || salt)` simples,
  não HMAC-SHA256 nem uma KDF (PBKDF2/scrypt/Argon2). SHA-256 é rápido por
  projeto, o que favorece justamente o ataque de dicionário acima.
- ⚠️ **Salt global, não por registro.** Um único salt para toda a base
  (`:35-36`: se não informado, busca o mesmo secret). Isso é o que **permite**
  o join por `hash_cliente` — é uma escolha de arquitetura necessária, mas
  significa que comprometer um salt compromete a base inteira.

### 10.3 🔴 Achado LGPD: `id_cliente` carrega o identificador em claro

`src/ingestion/clientes_kaggle.py:82`:
```
df["id_cliente"] = df["CustomerId"].apply(lambda x: f"CLI{x}")
```
Na **linha seguinte** (`:83`) o mesmo `CustomerId` é hasheado para
`hash_cliente`. Ou seja, **o identificador original é gravado em claro na mesma
tabela que o seu hash**, com um prefixo `CLI` de 3 caracteres.

**Propagação confirmada:**
- `bronze.clientes` — `id_cliente` está na lista de colunas selecionadas
  (`:92`)
- `silver.clientes` — herda todas as colunas do bronze
  (`src/transformation/silver_clientes.py:24`, `spark.table(...)` sem
  `select`)
- `silver.clientes_scd` — `id_cliente` é explicitamente selecionado
  (`src/clients/scd.py:76`)

Efeito prático: **a anonimização de `hash_cliente` é anulada** dentro da própria
tabela. Não é preciso nem quebrar o hash — basta ler a coluna ao lado e remover
o prefixo `CLI`. O `sobrenome_masked` continua protegido (o sobrenome não é
gravado em claro em lugar nenhum), mas a ligação hash↔cliente está exposta em
três tabelas.

### 10.4 Outros dados pessoais tratados (não anonimizados, por desenho)

Colunas de `bronze.clientes` / `silver.clientes` que são dado pessoal ou
sensível e **não passam por hashing** (`clientes_kaggle.py:91-101`):
`pais` (`Geography`), `genero` (`Gender`), `idade` (`Age`), `saldo` (`Balance`),
`salario_estimado` (`EstimatedSalary`), `score_credito` (`CreditScore`),
`num_produtos`, `anos_cliente`, `ativo`, `churn`.

Isoladamente são atributos; em conjunto formam um **quase-identificador**
(país + gênero + idade + faixa de saldo + score) capaz de singularizar
indivíduos em bases pequenas. Não há generalização, k-anonimato nem supressão
implementados. `gold.perfil_clientes`
(`jobs/job_corretora_analises.py:122-134`) agrega por
`(perfil_risco, faixa_etaria, score_categoria, pais)` **sem piso de contagem** —
`total_clientes` pode legitimamente ser `1`, publicando o saldo e o salário
médios de **um único indivíduo**.

**Mitigante de fato:** a fonte é um dataset público de Kaggle
(`mathchi/churn-for-bank-customers`, `:21`), não a base real do banco. O risco
é de **arquitetura** — se a mesma ingestão for apontada para dado real, os
achados 10.3 e 10.4 passam de teóricos a efetivos.

### 10.5 Higiene de segredos no código lido

- Secrets sempre via `dbutils.secrets.get(scope=..., key=...)`
  (`src/config/secrets.py:32-37`) — **7 chaves** por nome:
  `client-id`, `tenant-id`, `client-secret`, `storage-account`,
  `kaggle-username`, `kaggle-key`, `salt` (`:40-73`).
- ⚠️ `scripts/eventhub_producer.py:23` contém um **placeholder** de connection
  string (`"YOUR_EVENT_HUB_..."`), não um valor real; a versão avançada lê da
  variável de ambiente `EVENTHUB_CONNECTION_STRING`
  (`scripts/eventhub_producer_advanced.py:41`). Registrado como observação de
  higiene, não como vazamento.
- ⚠️ `src/config/environment.py:26-35` traz um caminho de Workspace default que
  embute o **e-mail do autor** — dado pessoal em código-fonte, ainda que de
  baixo risco.

---

## 11. CAMPOS E PONTOS [NÃO CONFIRMADO]

| # | Item | O que não foi possível confirmar | O que faltou |
|---|---|---|---|
| **NC-1** | Moeda de `saldo` e `salario_estimado` (`bronze/silver.clientes`, e por consequência `gold.perfil_clientes.saldo_medio` e `.salario_medio`) | **[NÃO CONFIRMADO]** que sejam BRL. O código apenas renomeia `Balance` e `EstimatedSalary` do CSV do Kaggle (`clientes_kaggle.py:96-101`) sem nenhuma conversão nem declaração de moeda. O dataset `mathchi/churn-for-bank-customers` é de um banco europeu (a coluna `Geography` traz França/Alemanha/Espanha). O pipeline **trata como BRL de fato** ao compará-los com limites em reais. | Documentação da moeda de origem ou uma conversão explícita no código. Nada no repositório declara a unidade. |
| **NC-2** | Timezone de `data_processamento`, `data_extracao`, `data_verificacao`, `data_inicio`, `processado_em` | **[NÃO CONFIRMADO]**. Todos usam `datetime.now()` sem timezone (ex.: `src/gold/fraude.py:20`, `src/clients/scd.py:18`, `src/observability/monitoring.py:57`, `jobs/job_streaming_continuous.py:101`). O relógio é o do driver Databricks. Os *schedules* declaram `America/Sao_Paulo` (`databricks.yml:781,283`), mas isso governa o disparo, não o `now()` do processo. | Configuração de timezone do cluster ou uso de `current_timestamp()` com timezone declarado. Não há nada disso no repositório. |
| **NC-3** | `ipca_acumulado_12m` (`src/gold/bcb_analise.py:52-53`) | O nome diz "acumulado", mas a implementação é `sum(ipca)` — **soma aritmética** de variações mensais, não acumulação composta (`∏(1+i)−1`). **[NÃO CONFIRMADO]** se a soma simples é a intenção de negócio (aproximação aceita para inflação baixa) ou um erro de fórmula. | Especificação de negócio do indicador. Não há comentário, teste nem documento que fixe a fórmula pretendida. |
| **NC-4** | `pib_variacao_pct` (`src/gold/world_bank_analise.py:38-42`) | O indicador de origem `NY.GDP.MKTP.KD.ZG` **já é** a taxa de crescimento anual do PIB (%). O campo calcula a variação percentual **dessa taxa** — uma derivada de segunda ordem. **[NÃO CONFIRMADO]** se é intencional. Os limiares de `tendencia_pib` (`> 2` → "Crescimento Alto", `:48`) leem como se o campo fosse o crescimento do PIB em si, não a variação da taxa. | Especificação do indicador. A escolha do código do World Bank confirma o significado da fonte, mas nada documenta o significado do derivado. |
| **NC-5** | Grade de datas de `cambio_variacao_pct` (`src/gold/correlacao_acoes_cambio.py:58-64`) | O `lag` é particionado por `ticker`, então a "variação de ontem" é relativa à **data anterior daquele ticker**, não ao dia anterior de calendário. Se todos os 9 tickers tiverem exatamente as mesmas datas, é equivalente; se algum tiver buraco de pregão, o cálculo diverge para aquele ticker. **[NÃO CONFIRMADO]** se as grades coincidem. | Uma inspeção do dado real em `silver.acoes` (não disponível neste ambiente — sem acesso a cluster). O filtro `volume > 0` de `silver_acoes.py:63` **pode** criar buracos por ticker. |
| **NC-6** | Volume efetivo de linhas em todas as cardinalidades da §8 | As cardinalidades estão derivadas dos **parâmetros do gerador** (1.000 clientes × 1–10 ordens; 9 tickers; ≤ 24 horas) e das listas literais, não de contagem real. **[NÃO CONFIRMADO]** por leitura do dado. | Acesso ao lakehouse. Este agente não executa Spark nem tem credenciais Databricks — por desenho. |

**Pontos deliberadamente NÃO documentados como regra de dado:**
- `src/pipeline/dynamic_pipeline.py:279` — `", ".join(...)` é geração de código
  Python, **não** join de dados (§8).
- `src/health/health_check.py` — três checks de conectividade
  (Databricks/Storage/Key Vault, `:30-70`) **sem call site em jobs**; não
  carrega regra de domínio.
- `jobs/job_streaming.py` e `jobs/job_streaming_to_gold.py` — cópias órfãs
  (§7.1); suas regras são idênticas às versões vivas e estão documentadas nas
  §2.5/§4 pela versão que roda.

---

## APÊNDICE — Contagem dos campos derivados documentados (208)

| Camada / tabela | Campos derivados | §|
|---|---|---|
| `bronze.clientes` | 8 | 1.1 |
| `bronze.ordens` | 10 | 1.2 |
| `bronze.acoes` | 3 | 1.3 |
| `bronze.bcb` | 4 | 1.4 |
| `bronze.world_bank` | 6 | 1.5 |
| `silver.acoes` | 8 | 2.1 |
| `silver.bcb` | 6 | 2.2 |
| `silver.world_bank` | 3 | 2.3 |
| `silver.clientes` | 3 | 2.4 |
| `silver.ordens` | 4 | 2.5 |
| `silver.streaming` | 7 | 6.6 / `job_streaming_continuous.py:88-101` |
| `gold.anomalias` | 6 | 3.1 |
| `gold.performance_acoes` | 9 | 3.2 |
| `gold.indicadores_bcb` | 12 | 3.3 |
| `gold.contexto_macroeconomico` | 10 | 3.4 |
| `gold.acoes_vs_cambio` | 12 | 3.5 |
| `gold.deteccao_fraude` | 8 | 3.6 |
| `gold.posicao_clientes` | 10 | 3.7 |
| `gold.score_risco_clientes` | 15 | 3.7 |
| `gold.perfil_clientes` | 6 | 3.7 |
| `gold.ordens_consolidadas` | 4 | 3.7 |
| `gold.ranking_acoes_perfil` | 3 | 3.7 |
| `gold.fraude_streaming` | 8 | 4.1 |
| `gold.anomalias_intraday` | 9 | 4.2 |
| `gold.volume_intraday` | 10 | 4.3 |
| `gold.ranking_acoes_realtime` | 12 | 4.4 |
| `gold.observabilidade` | 9 | 5.2 |
| SCD (`silver.clientes_scd` + `gold.score_risco_scd`) | 3 | 5.1 |
| **Total** | **208** | |
