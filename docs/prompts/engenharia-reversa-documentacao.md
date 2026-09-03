# Prompt Package — Documentação por Engenharia Reversa

Conjunto de prompts que instancia 5 agentes para reconstruir, a partir do código,
duas documentações do pipeline: uma **funcional** (para quem decide) e uma
**técnica** (para quem mantém), com diagrama relacional e stack real.

O ponto central deste pacote é o **critério de parada objetivo**: o Agente Revisor
não julga "está bom?", ele confere a cobertura contra um inventário extraído do
próprio código. Enquanto faltar tabela, transformação ou join, ele devolve o
trabalho com a lacuna nomeada.

---

## Entregáveis

| Arquivo | Público | Conteúdo |
|---|---|---|
| `docs/DOCUMENTACAO_FUNCIONAL.md` | Negócio, gestão, avaliador | O que cada tabela final responde, de onde vem o dado, que decisão suporta |
| `docs/DOCUMENTACAO_TECNICA.md` | Engenharia | Linhagem completa, transformações campo a campo, joins, schemas, stack, operação |

Ambos com diagramas Mermaid (renderizam nativamente no GitHub).

---

## Inventário de referência (critério de aceite)

Extraído do código em 2026-09-02. **Regenere antes de usar** — se o número mudou,
o pacote está desatualizado:

```bash
# Tabelas por camada -- ATENCAO: falso negativo conhecido.
# So acha literais: devolve 2 bronze, nao 6. As outras 4 (acoes, bcb,
# world_bank, kafka) sao gravadas por variavel de loop em
# jobs/job_unity_catalog.py:71 -- f"{SCHEMA_BRONZE}.{tabela}" -- e escapam
# do grep. Complemente com a varredura de paths abaixo.
grep -rhno "SCHEMA_\(BRONZE\|SILVER\|GOLD\)}\.[a-z_0-9]*" --include=*.py jobs/ src/ \
  | sed 's/^[0-9]*://' | sed 's/}\./ /' | sort -u

# Bronze gravadas por variavel: pega pelos paths ADLS
grep -rhno "abfss://bronze@[^\"]*" --include=*.py src/ jobs/ \
  | sed 's/.*net\///' | sed 's/[\/"].*//' | sort -u

# Joins -- retorna 9; exclua os 2 falsos positivos descritos acima
grep -rn "\.join(" --include=*.py src/ jobs/ | grep -v "^\s*#"
```

**Cobertura mínima exigida — 30 tabelas:**

- **Bronze (6):** `acoes`, `bcb`, `world_bank`, `kafka` (paths ADLS) · `clientes`, `ordens` (tabelas UC)
- **Silver (7):** `acoes`, `bcb`, `world_bank`, `streaming`, `clientes`, `ordens`, `clientes_scd`
- **Gold (17):** `performance_acoes`, `anomalias`, `acoes_vs_cambio`, `deteccao_fraude`,
  `indicadores_bcb`, `contexto_macroeconomico`, `observabilidade`, `posicao_clientes`,
  `score_risco_clientes`, `perfil_clientes`, `ordens_consolidadas`, `ranking_acoes_perfil`,
  `score_risco_scd`, `fraude_streaming`, `anomalias_intraday`, `volume_intraday`,
  `ranking_acoes_realtime`

**Joins — 7 joins de dado em 4 módulos**, distribuídos assim:

| Arquivo | Joins lógicos |
|---|---|
| `src/gold/streaming_gold.py` | 4 |
| `src/gold/fraude.py` | 1 |
| `src/gold/correlacao_acoes_cambio.py` | 1 |
| `jobs/job_corretora_analises.py` | 1 |

O grep bruto por `.join(` retorna **9 ocorrências**. Duas são falsos positivos e
precisam ser excluídas — é assim que a contagem chega a 7:

1. `src/pipeline/dynamic_pipeline.py:279` — é `", ".join(...)`, o **`str.join` do
   Python** dentro de uma f-string que gera código de DAG. Não é join de DataFrame.
2. `src/gold/fraude.py:46` e `:50` — são os **dois ramos do mesmo `if/else`**
   (`:45`, broadcast vs sort-merge). É **um** join lógico, contado uma vez.

O revisor rejeita documentação que trate qualquer uma das duas como join de dado,
e rejeita documentação que descreva a de `fraude.py` como dois joins distintos.

---|---|
| `src/gold/streaming_gold.py` | 4 |
| `src/gold/fraude.py` | 2 |
| `src/gold/correlacao_acoes_cambio.py` | 1 |
| `jobs/job_corretora_analises.py` | 1 |

O grep bruto retorna 9: a nona ocorrência está em `src/pipeline/dynamic_pipeline.py`
e é join de geração de DAG, **não de dado**. Deve ficar fora da linhagem — o revisor
rejeita documentação que a inclua como transformação.

---

## Orquestração

Agentes 1 a 4 rodam **em paralelo** (escopos disjuntos, sem colisão de escrita — cada um
entrega um rascunho separado). O Agente 5 roda depois, consolida e devolve lacunas.
Repita 4→5 até o revisor retornar `APROVADO`.

```
        ┌── Agente 1: Linhagem ────────┐
        ├── Agente 2: Transformações ──┤
(paralelo)                              ├──> Agente 5: Revisor ──> APROVADO?
        ├── Agente 3: Modelo de Dados ─┤         │                    │ não
        └── Agente 4: Stack & Infra ───┘         └────< lacunas >─────┘
```

**Regra comum a todos os agentes** (cole no início de cada prompt):

> Trabalhe apenas sobre o que o código comprova. Toda afirmação sobre dado,
> transformação ou dependência deve vir com `arquivo:linha`. Se não conseguir
> confirmar algo, escreva `[NÃO CONFIRMADO]` e diga o que faltou — não preencha
> com suposição plausível. Não invente nome de tabela, coluna ou função.
> Repositório: `c:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master`

---

## Agente 1 — Linhagem

```
Você é o AGENTE DE LINHAGEM. Reconstrua o grafo de dados completo do pipeline
por engenharia reversa, sem confiar em documentação existente (ela pode estar
desatualizada — confira contra o código).

[REGRA COMUM]

Entregue:

1. TABELA DE LINHAGEM, uma linha por tabela/path, com estas colunas:
   camada | objeto | produtor (arquivo:linha) | consumidores (arquivo:linha) |
   formato | modo de escrita (append/overwrite/merge) | particionamento

2. Cubra os 30 objetos do inventário de referência:
   - Bronze: acoes, bcb, world_bank, kafka, clientes, ordens
   - Silver: acoes, bcb, world_bank, streaming, clientes, ordens, clientes_scd
   - Gold: performance_acoes, anomalias, acoes_vs_cambio, deteccao_fraude,
     indicadores_bcb, contexto_macroeconomico, observabilidade, posicao_clientes,
     score_risco_clientes, perfil_clientes, ordens_consolidadas,
     ranking_acoes_perfil, score_risco_scd, fraude_streaming, anomalias_intraday,
     volume_intraday, ranking_acoes_realtime

3. Atenção a uma dualidade real deste projeto: parte do dado vive como PATH ADLS
   (abfss://) e parte como TABELA Unity Catalog, e alguns objetos são os dois
   (path registrado como tabela externa via register_external_table em
   src/config/tables.py). Deixe explícito, por objeto, qual é o caso.

4. Os nomes de schema têm prefixo de ambiente (hk_/prod_) resolvido em
   src/config/tables.py. Documente o padrão uma vez e use a forma genérica
   <catalog>.<env>_camada.tabela no resto.

5. DIAGRAMA MERMAID (flowchart LR) do fluxo fonte -> bronze -> silver -> gold,
   agrupando por camada com subgraph. Inclua as fontes externas reais
   (Yahoo Finance, BCB, World Bank, Kaggle, Event Hub).

6. ÓRFÃOS: liste objetos escritos e nunca lidos, e lidos sem produtor. Se não
   houver, diga explicitamente que não há.

Formato: Markdown. Comece pela tabela, depois o diagrama, depois os órfãos.
```

---

## Agente 2 — Transformações e Joins

```
Você é o AGENTE DE TRANSFORMAÇÕES. Documente a lógica de negócio implementada,
campo a campo. Este é o núcleo da documentação técnica.

[REGRA COMUM]

Entregue:

1. Para CADA módulo de transformação (src/transformation/, src/gold/,
   src/clients/scd.py, src/observability/monitoring.py) e cada job em jobs/ que
   contenha lógica própria:
   - entrada(s) e saída
   - cada coluna derivada: nome, expressão, e a REGRA DE NEGÓCIO em português
     (ex.: "alerta_volume: 'Volume Alto' quando quantidade > 8000")
   - agregações e window functions: chave de partição, ordenação, frame
   - filtros e deduplicações, com o efeito sobre o volume

2. SEÇÃO DEDICADA A JOINS — são 7 joins de DADO, assim distribuídos:
   src/gold/streaming_gold.py (4), src/gold/fraude.py (1),
   src/gold/correlacao_acoes_cambio.py (1), jobs/job_corretora_analises.py (1).
   Para cada join: tabelas, chave, tipo (inner/left/...), estratégia
   (broadcast?), e cardinalidade esperada.
   O grep por ".join(" retorna 9. Dois são falsos positivos e NÃO devem ser
   documentados como join de dado:
   - src/pipeline/dynamic_pipeline.py:279 é `", ".join(...)`, str.join do
     Python gerando código de DAG
   - src/gold/fraude.py:46 e :50 são os dois ramos do mesmo if/else (:45),
     broadcast vs sort-merge. Documente como UM join com duas estratégias.

3. LIMIARES E CONSTANTES DE NEGÓCIO em uma tabela própria (valores de corte de
   fraude, faixas de score de risco, thresholds de anomalia, janelas temporais).
   São eles que um avaliador vai querer questionar.

4. REGRAS DE QUALIDADE: onde src/quality/data_quality.py é invocado, o que cada
   gate valida e o que acontece quando reprova.

5. ANONIMIZAÇÃO/LGPD: o que src/security/hashing.py protege, onde é aplicado, e
   qual a garantia (reversível ou não).

Formato: Markdown, uma subseção por módulo. Priorize precisão sobre prosa.
```

---

## Agente 3 — Modelo de Dados e Diagrama Relacional

```
Você é o AGENTE DE MODELO DE DADOS. Reconstrua o schema de cada tabela final e
o modelo relacional entre elas.

[REGRA COMUM]

Entregue:

1. DICIONÁRIO DE DADOS das 17 tabelas gold e das 7 silver. Para cada:
   - coluna, tipo, origem (herdada de qual tabela/coluna, ou expressão que a cria)
   - significado de negócio
   - chave (natural/negócio) quando identificável
   Deduza os tipos das expressões PySpark (cast, lit, round, when...) e dos
   StructType declarados. Marque como [INFERIDO] o que não estiver explícito.

2. DIAGRAMA RELACIONAL em Mermaid erDiagram, ligando as tabelas pelas chaves
   realmente usadas nos joins (ex.: hash_cliente, ticker, data). Não invente
   relacionamento que o código não exerce — se duas tabelas nunca se cruzam,
   não ligue.

3. TABELAS SCD: src/clients/scd.py implementa Slowly Changing Dimension.
   Documente as colunas de controle, a semântica de versionamento e qual tipo de
   SCD é de fato (compare o código com a definição formal — se divergir, diga).

4. GRANULARIDADE: para cada tabela gold, uma frase dizendo o que é uma linha
   ("uma linha por cliente por dia", "uma linha por ticker").

Formato: Markdown. Dicionário em tabelas, depois o erDiagram.
```

---

## Agente 4 — Stack e Operação

```
Você é o AGENTE DE STACK E INFRAESTRUTURA. Documente a plataforma real, com
versões extraídas dos arquivos — nunca de memória.

[REGRA COMUM]

Entregue:

1. STACK com versões reais, citando o arquivo de onde saiu cada uma:
   requirements.txt, setup.py, databricks.yml (spark_version, node_type_id),
   terraform/main.tf (required_providers), requirements-airflow.txt.

2. INFRAESTRUTURA AZURE a partir de terraform/: cada recurso, sua função no
   pipeline e como se conectam. Cubra ADLS Gen2 (e por que is_hns_enabled),
   Key Vault + secret scope, Event Hub + Capture, Databricks workspace,
   Access Connector, Unity Catalog (metastore/catalog/schemas), RBAC.
   Diagrama Mermaid da arquitetura de infra.

3. ORQUESTRAÇÃO: o grafo de 19 tasks de pipeline_completo em databricks.yml e o
   DAG Airflow equivalente em dags/dag_pipeline_santander.py. Explique que o DAG
   é GERADO por scripts/sync_airflow_from_databricks.py a partir do bundle, e o
   que isso implica (editar o DAG à mão é sobrescrito).

4. AMBIENTES: como hk e prod se isolam (storage account distinto, prefixo de
   schema, state Terraform por ambiente). Cite src/config/environment.py e
   src/config/tables.py.

5. OPERAÇÃO: como sobe (scripts/deploy_infra.sh, duas etapas e por quê), como
   derruba (scripts/destroy_infra.sh), o que é pré-requisito manual
   (subscription, service principal, Databricks Account Admin).

6. CI/CD: os 4 workflows em .github/workflows/, o que cada um dispara e valida.

Formato: Markdown, com os dois diagramas Mermaid.
```

---

## Agente 5 — Revisor (loop)

```
Você é o AGENTE REVISOR. Não escreva documentação: audite a que recebeu e
decida se está completa. Seja rigoroso — sua função é impedir que documentação
incompleta ou inventada passe.

Você recebeu os rascunhos dos Agentes 1 a 4. Confira cada item abaixo CONTRA O
CÓDIGO do repositório, não contra o texto dos rascunhos.

CHECKLIST DE COBERTURA (objetivo, sem margem para julgamento):

[ ] As 30 tabelas do inventário aparecem, cada uma com produtor e consumidor
    identificados por arquivo:linha
[ ] Os 7 joins de dado estão documentados com chave e tipo (4 em
    streaming_gold.py, 1 em fraude.py, 1 em correlacao_acoes_cambio.py,
    1 em job_corretora_analises.py); o `str.join` de dynamic_pipeline.py:279
    NÃO aparece como linhagem; e o join de fraude.py está descrito como UM
    join com duas estratégias, não como dois joins
[ ] Nenhuma tabela/coluna citada na documentação deixa de existir no código
    (verifique por amostragem agressiva — pelo menos 10 nomes, incluindo os
    menos óbvios)
[ ] Os diagramas Mermaid têm sintaxe válida e refletem as dependências reais
[ ] Versões da stack conferem com os arquivos citados
[ ] Regras de negócio com limiar numérico batem com o valor no código
[ ] A dualidade path ADLS vs tabela Unity Catalog está explícita
[ ] O prefixo de ambiente (hk_/prod_) está explicado
[ ] Afirmações não confirmadas estão marcadas como tal, e não apresentadas
    como fato

SAÍDA — exatamente um destes dois formatos:

  APROVADO
  seguido de um resumo de 5 linhas do que foi verificado.

  REPROVADO
  seguido de uma lista de lacunas, cada uma com: agente responsável (1-4),
  o item do checklist que falhou, e o que exatamente falta ou está errado
  (com arquivo:linha da evidência).

Não aprove com ressalva. Se há lacuna, é REPROVADO.
```

---

## Prompt de consolidação (após APROVADO)

```
Consolide os rascunhos aprovados em dois arquivos finais:

docs/DOCUMENTACAO_FUNCIONAL.md
  Público: negócio e avaliação. Sem código. Responde:
  que perguntas de negócio o pipeline responde; que tabela responde cada uma;
  de onde vem o dado; com que frequência atualiza; que decisão suporta.
  Inclua o diagrama de fluxo (Agente 1) simplificado.

docs/DOCUMENTACAO_TECNICA.md
  Público: engenharia. Estrutura:
  1. Visão geral e stack (Agente 4)
  2. Arquitetura de infraestrutura + diagrama (Agente 4)
  3. Linhagem completa + diagrama de fluxo (Agente 1)
  4. Modelo de dados + diagrama relacional (Agente 3)
  5. Transformações por camada, joins e regras de negócio (Agente 2)
  6. Orquestração e ambientes (Agente 4)
  7. Operação: deploy, teardown, CI/CD (Agente 4)
  8. Limitações conhecidas e pontos não confirmados

Regras: nenhuma afirmação sem respaldo no código; itens [NÃO CONFIRMADO]
migram para a seção 8, nunca são apagados nem promovidos a fato.
```

---

## Como invocar no Claude Code

```
Leia docs/prompts/engenharia-reversa-documentacao.md e execute o pacote:
dispare os Agentes 1 a 4 em paralelo, depois o Revisor. Repita o ciclo
4→5 até APROVADO, e então consolide nos dois arquivos finais.
```

Para virar slash command, copie este arquivo para `.claude/commands/doc-reversa.md`
e invoque com `/doc-reversa`.
