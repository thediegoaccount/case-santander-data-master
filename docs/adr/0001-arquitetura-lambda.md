# ADR 0001 — Arquitetura Lambda em vez de Kappa

**Status:** Aceito · 2026-09
**Análise completa:** [`docs/architecture-kappa-vs-lambda.md`](../architecture-kappa-vs-lambda.md)

## Contexto

O pipeline consome cinco fontes com naturezas temporais distintas:

| Fonte | Natureza | Frequência real |
|---|---|---|
| Yahoo Finance, BCB, World Bank, Kaggle | Batch (APIs REST de consulta) | diária a anual |
| Azure Event Hub | Streaming (Kafka) | contínua |

As perguntas de negócio também se dividem: análise histórica (performance por setor/ano,
correlação com câmbio, SCD de perfil de risco) convive com detecção de fraude e anomalias
intraday, que precisam responder em minutos.

A arquitetura Kappa propõe tratar tudo como stream — inclusive o histórico, reprocessado
a partir do log de eventos. A Lambda mantém batch e streaming como caminhos separados que
convergem na camada de serving.

## Decisão

Adotamos **arquitetura Lambda**: camada batch (`pipeline_completo`, diário às 06:00) e
camada de velocidade (`streaming_continuous` + `streaming_to_gold_continuous`, 24/7),
ambas escrevendo na camada Gold do mesmo lakehouse.

O motivo determinante não é preferência estética: **quatro das cinco fontes não são streams**.
APIs REST de consulta histórica (a série SGS do Banco Central, o indicador anual do World Bank)
não emitem eventos — transformá-las em stream exigiria fabricar um log de eventos artificial,
adicionando um componente sem contrapartida analítica.

## Consequências

**Positivas**
- Cada fonte é tratada na sua natureza; nenhuma camada de conversão artificial
- Batch Spark é mais simples de operar e depurar que streaming stateful
- Falha na camada de streaming não compromete o fechamento diário, e vice-versa
- Esforço de implementação estimado em 2–3 dias, contra 4–6 semanas para re-arquitetar em Kappa

**Negativas — o custo real desta escolha**
- **Duplicação de lógica**: as regras de fraude existem em duas implementações — `gold.deteccao_fraude`
  (batch) e `gold.fraude_streaming` (tempo real). Mudança de regra exige alterar os dois caminhos,
  com risco de divergência silenciosa
- Duas superfícies operacionais para monitorar, com perfis de falha diferentes
- Reconciliação entre batch e streaming é responsabilidade do consumidor da camada Gold

**Mitigação adotada:** as regras compartilham o módulo `src/gold/fraude.py`, reduzindo — mas não
eliminando — a duplicação. O acoplamento restante está nas janelas de agregação, que diferem
por natureza (diária vs. intraday).

## Alternativas consideradas

**Kappa puro** — descartada. Exigiria um log de eventos como fonte única da verdade e o
reprocessamento do histórico como replay de stream. Para fontes que são APIs de consulta,
isso significa construir e manter uma camada de captura que não existe hoje. O custo de
re-arquitetura (4–6 semanas) não se justifica sem um requisito que a Lambda não atenda.

**Batch puro** — descartada. Detecção de fraude e anomalias intraday perderiam sentido com
latência de 24 horas.

## Quando revisitar

Se as fontes batch forem substituídas por feeds de eventos (ex.: contratação de um provedor
de cotações em tempo real), a justificativa central desta decisão deixa de valer e o Kappa
volta à mesa.
