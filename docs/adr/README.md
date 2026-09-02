# Architecture Decision Records (ADR)

Registro das decisões de arquitetura deste projeto: **o que** foi decidido, **por quê**,
**o que se perdeu** com a escolha e **o que foi descartado**.

Cada ADR é imutável depois de aceito. Mudou de ideia? Cria-se um novo ADR que supersede o
anterior — o histórico da decisão é tão relevante quanto a decisão vigente.

## Índice

| # | Decisão | Status | Trade-off central |
|---|---|---|---|
| [0001](0001-arquitetura-lambda.md) | Arquitetura Lambda em vez de Kappa | Aceito | Duplicação de lógica batch/streaming em troca de menor risco e fontes tratadas na sua natureza |
| [0002](0002-serving-lakehouse.md) | Serving via lakehouse, sem banco relacional | Aceito | Perde-se latência de dashboard sub-segundo; ganha-se uma cópia de dados a menos para manter |
| [0003](0003-job-clusters.md) | Job clusters efêmeros em vez de cluster always-on | Aceito | +33% de tempo de execução por −$2.050/mês |
| [0004](0004-databricks-yml-fonte-unica.md) | `databricks.yml` como fonte única, DAG do Airflow gerado | Aceito | Perde-se flexibilidade de escrever DAG à mão; elimina-se divergência entre orquestradores |
| [0005](0005-scd-type-2.md) | SCD Type 2 em perfil de risco e score | Aceito | Storage e complexidade de query maiores em troca de auditabilidade histórica |
| [0006](0006-auto-loader-cdc.md) | Auto Loader + Change Data Feed na ingestão incremental | Aceito | Estado de checkpoint a gerenciar em troca de escala e custo de leitura |

## Formato

Cada registro segue a mesma estrutura:

- **Contexto** — a situação e as forças em jogo quando a decisão foi tomada
- **Decisão** — o que foi escolhido, na voz ativa
- **Consequências** — o que melhora **e o que piora**; um ADR sem custo declarado é propaganda
- **Alternativas consideradas** — o que foi descartado e por quê
