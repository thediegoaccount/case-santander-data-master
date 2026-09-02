# ADR 0004 — `databricks.yml` como fonte única, DAG do Airflow gerado

**Status:** Aceito · 2026-09
**Implementação:** [`scripts/sync_airflow_from_databricks.py`](../../scripts/sync_airflow_from_databricks.py) · [`docs/airflow-databricks-sync.md`](../airflow-databricks-sync.md)

## Contexto

O projeto opera dois orquestradores com papéis distintos: **Databricks Workflow** em produção
(agendado às 06:00) e **Apache Airflow em Docker** para desenvolvimento e para demonstrar como
o pipeline seria orquestrado por uma organização com infraestrutura Airflow ou multi-cloud.

Ambos precisam representar o mesmo grafo: 19 tasks e suas dependências. Mantidos à mão, os dois
divergem — é questão de tempo até alguém adicionar uma task em um e esquecer do outro, e a
divergência só aparece quando o pipeline falha em produção.

Esse risco não é hipotético neste repositório: uma dependência declarada para uma task
inexistente (`t_sql_streaming`) passou despercebida justamente por estar duplicada entre o
`databricks.yml` e o DAG, quebrando o job de validação no CI.

## Decisão

O `databricks.yml` (Databricks Asset Bundle) é a **fonte única da verdade** do grafo de tasks.
O DAG do Airflow é **gerado** a partir dele por `scripts/sync_airflow_from_databricks.py`,
que lê o job `pipeline_completo`, mapeia cada `task_key` para o script correspondente e
emite as dependências.

O arquivo gerado carrega no cabeçalho: *"NÃO EDITE MANUALMENTE — alterações devem ser feitas
em databricks.yml"*. O CI valida a sincronia e falha se o DAG commitado divergir do que o
gerador produz.

## Consequências

**Positivas**
- Impossível divergir entre os dois orquestradores: um é derivado do outro
- Os **mesmos** `jobs/*.py` executam nos dois ambientes — zero duplicação de código de negócio
- Adicionar uma task é uma edição em um lugar só
- A validação no CI transforma divergência de configuração em erro de build, não em incidente

**Negativas — o custo real desta escolha**
- O DAG gerado é limitado ao que o gerador sabe expressar: não há `TaskGroup`, sensores,
  branching ou qualquer recurso do Airflow que não exista no modelo do Asset Bundle.
  O Airflow é usado como executor de grafo, não com todo o seu vocabulário
- O mapeamento `task_key → arquivo` vive num dicionário dentro do gerador; uma task nova exige
  editar o script além do YAML — acoplamento que não é óbvio para quem chega
- Um bug no gerador se propaga para o DAG inteiro

## Alternativas consideradas

**Manter os dois à mão** — descartada. É a origem do problema, e já produziu uma falha real.

**Airflow como fonte única, gerando o Asset Bundle** — descartada. O Databricks Workflow é o
orquestrador de produção; a fonte da verdade deve ser o artefato que governa produção, não o
de desenvolvimento.

**Um formato neutro gerando os dois** — descartada por excesso de indireção. Introduziria um
terceiro dialeto para manter, e o `databricks.yml` já é declarativo e legível.
