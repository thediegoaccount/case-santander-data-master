# ADR 0002 — Serving via lakehouse, sem banco relacional intermediário

**Status:** Aceito · 2026-09
**Supersede:** a carga em Azure SQL Database presente até o commit anterior

## Contexto

A arquitetura original replicava as tabelas Gold para um Azure SQL Database (`sqldb-case-santander`)
como *serving layer*, através de cinco jobs de carga (`job_carga_sql_*.py`) executados ao final
do pipeline diário, mais cinco jobs standalone equivalentes.

Ao revisar o consumo real, constatou-se que **nenhum consumidor estava conectado ao banco**.
Não havia dashboard, aplicação ou relatório lendo de `dbo.*` — os dados eram copiados e não lidos.

O padrão two-tier (lake para processamento, banco relacional para consumo) resolve um problema
concreto: engines de BI que não falam Delta, ou consultas OLTP que exigem latência de milissegundos
e alta concorrência. Nenhum dos dois se aplicava aqui: o consumo previsto é analítico
(dashboards, Genie AI, exploração ad-hoc), e o Databricks SQL Warehouse atende esse perfil
lendo Delta diretamente.

## Decisão

Removemos a camada relacional. As tabelas `case_santander.gold.*` em Delta são o produto final,
consumidas via Unity Catalog por SQL Warehouse, dashboards e Genie AI.

## Consequências

**Positivas**
- Elimina 10 jobs (5 no pipeline + 5 standalone) e ~240 linhas de configuração em `databricks.yml`
- Uma cópia de dados a menos: acaba a janela em que o Delta e o SQL divergem entre execuções
- Um serviço a menos para provisionar, versionar, monitorar e pagar
- Governança unificada: RBAC, lineage e auditoria só no Unity Catalog, em vez de dois modelos
  de permissão para manter em sincronia
- Remove a dependência do secret `sql-connection-string` e do driver JDBC

**Negativas — o custo real desta escolha**
- **Latência de consulta é maior**: SQL Warehouse tem tempo de inicialização (segundos a minutos
  se estiver frio), enquanto um banco relacional sempre ligado responde imediatamente. Para
  dashboard executivo com expectativa de resposta instantânea, isso é sentido
- **Concorrência alta é mais cara** no modelo lakehouse do que num banco relacional dimensionado
- Ferramentas de BI legadas que só falam ODBC/JDBC tradicional exigem o conector Databricks,
  o que pode não estar disponível em todo ambiente corporativo
- Perde-se a capacidade de servir consultas pontuais por chave primária com latência de OLTP —
  o lakehouse não é feito para isso

## Alternativas consideradas

**Manter o SQL Database ativo** — descartada. Manter infraestrutura, custo e código para uma
camada sem consumidor é dívida técnica pura. A decisão pode ser revertida quando houver
um consumidor real com requisito que o lakehouse não atenda.

**Manter os jobs de carga como execução sob demanda** — descartada. Código que não roda em
nenhum agendamento apodrece: não é testado, não é observado, e dá falsa impressão de
capacidade instalada.

**Materialized views no Unity Catalog** — não avaliada nesta rodada. É o caminho natural
caso a latência de consulta se torne o gargalo antes de haver necessidade de um banco relacional.

## Quando revisitar

Três gatilhos: (1) surgir consumidor com requisito de latência sub-segundo; (2) requisito de
concorrência alta e sustentada, onde o custo do warehouse supere o de um banco dedicado;
(3) integração com ferramenta corporativa que não suporte o conector Databricks.
