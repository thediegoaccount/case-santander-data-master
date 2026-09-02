# ADR 0005 — SCD Type 2 em perfil de risco e score de crédito

**Status:** Aceito · 2026-09
**Implementação:** [`src/clients/scd.py`](../../src/clients/scd.py) · job `t9_scd`

## Contexto

Atributos de cliente como perfil de risco, score de crédito e faixa de saldo **mudam ao longo
do tempo**, e essa mudança é a informação relevante — não apenas o valor atual.

Duas perguntas de negócio dependem disso e não podem ser respondidas por uma tabela que só
guarda o estado presente:

1. *"Este cliente já era classificado como Conservador quando a ordem foi executada?"* —
   avaliação de adequação (suitability), com implicação regulatória
2. *"Quantos clientes migraram de Moderado para Agressivo no último trimestre?"* — análise de
   deriva de perfil da carteira

Sobrescrever o registro a cada carga (SCD Type 1) destrói a resposta para ambas.

## Decisão

Aplicamos **SCD Type 2** em duas tabelas:

| Tabela | Atributos versionados |
|---|---|
| `silver.clientes_scd` | `perfil_risco`, `score_credito`, `faixa_saldo`, `faixa_etaria`, `score_categoria`, `ativo`, `churn` |
| `gold.score_risco_scd` | score ponderado e limite operacional |

O versionamento usa três colunas de controle e um `MERGE` do Delta Lake: quando um atributo
monitorado muda, a linha vigente é fechada (`data_fim` = hoje, `atual` = false) e uma nova
linha é aberta.

```
hash_cliente | perfil_risco | data_inicio | data_fim   | atual
abc123       | Conservador  | 2024-01-01  | 2024-06-01 | false
abc123       | Moderado     | 2024-06-01  | 9999-12-31 | true
```

A convenção `data_fim = 9999-12-31` para a linha vigente evita `NULL` em predicados de
intervalo — consultas por data usam `BETWEEN` sem tratamento especial.

## Consequências

**Positivas**
- Reconstituição do estado de qualquer cliente em qualquer data passada
- Trilha de auditoria nativa para decisões de risco, sem sistema externo
- Análise de transição de perfil torna-se uma query, não um projeto

**Negativas — o custo real desta escolha**
- **Toda consulta precisa filtrar `WHERE atual = true`** para obter o estado corrente. Esquecer
  o filtro multiplica linhas silenciosamente em joins e infla agregações — é a fonte de erro
  mais comum neste padrão
- Crescimento de storage proporcional à volatilidade dos atributos, não ao número de clientes
- Toda carga é um `MERGE`, mais caro que um `overwrite`
- A granularidade é diária: duas mudanças no mesmo dia colapsam em uma

## Alternativas consideradas

**SCD Type 1 (sobrescrever)** — descartada. Mais barato e simples, mas elimina exatamente a
informação que motivou o requisito.

**Delta Time Travel** — descartada como mecanismo primário. O Delta já versiona as tabelas e
permite consultar versões anteriores, mas a retenção é operacional (governada por `VACUUM`,
7 dias neste projeto) e a granularidade é a da transação, não a da mudança de atributo de
negócio. Serve para recuperar de erro, não como histórico de negócio de longo prazo.

**Tabela de auditoria separada** — descartada. Exigiria join adicional em toda consulta
histórica, sem ganho sobre o SCD2 já suportado nativamente pelo `MERGE` do Delta.

## Escopo deliberadamente limitado

SCD2 **não** foi aplicado a `silver.acoes`, `silver.bcb` ou `silver.world_bank`. Séries
temporais de mercado já são imutáveis por natureza — cada linha é um fato datado, não um
atributo que muda. Versionar seria redundante.
