# ADR 0003 — Job clusters efêmeros em vez de cluster always-on

**Status:** Aceito · 2026-09
**Detalhamento:** [`CLUSTER_CONFIG.md`](../../CLUSTER_CONFIG.md) · relatório da migração em [`archive/JOB_CLUSTER_UPGRADE.md`](../archive/JOB_CLUSTER_UPGRADE.md)

## Contexto

A configuração inicial usava um cluster interativo compartilhado (`existing_cluster_id`) ligado
continuamente, atendendo todas as tasks do pipeline. O padrão é confortável no desenvolvimento —
o cluster está sempre quente, cada execução começa em segundos — mas cobra por hora ligada,
não por trabalho realizado.

O pipeline roda **uma vez por dia**, às 06:00, e leva pouco mais de uma hora. O cluster
permanecia ligado nas outras ~23 horas.

## Decisão

Cada task declara seu próprio `new_cluster` no `databricks.yml`, dimensionado ao seu perfil de
trabalho, criado na execução e encerrado ao final. Instâncias SPOT com `idle_timeout_minutes`
curto. Os workers são parametrizados por ambiente (`${var.gold_workers}`, `${var.sql_workers}`),
permitindo clusters menores em homologação.

## Consequências

**Positivas**
- Custo mensal de **~$2.500–3.000 para ~$130**, pagando por execução em vez de por disponibilidade
- Dimensionamento por perfil: uma task de agregação Gold não é obrigada a usar o mesmo cluster
  de uma extração leve de API
- Isolamento de falhas: um job que estoura memória não derruba o cluster dos demais
- Ambientes hk e prod escalam de forma independente pelo mesmo arquivo

**Negativas — o custo real desta escolha**
- **Tempo total de execução sobe de ~70 para ~93 minutos (+33%)**, por causa dos 3–5 minutos de
  provisionamento de cluster por task
- Instâncias SPOT podem ser recuperadas pelo provedor no meio da execução; mitigado com
  `max_retries: 2`, mas é uma fonte de instabilidade que não existia antes
- Depuração interativa fica menos conveniente — não há cluster quente esperando
- Mais superfície de configuração: cada task tem seu bloco de cluster para manter

**Por que o trade-off é aceitável aqui:** o pipeline é agendado e não tem consumidor esperando
o resultado em tempo real. Terminar às 07:33 em vez de 07:10 não muda nenhuma decisão de negócio,
e economiza ~$24.600/ano. Se houvesse SLA de entrega apertado, a conta seria outra.

## Alternativas consideradas

**Cluster pools** — não adotada nesta rodada. Reduziriam o overhead de inicialização mantendo
instâncias pré-aquecidas, atacando exatamente os +33% de tempo. É a evolução natural se o tempo
de execução virar problema, ao custo de reintroduzir pagamento por disponibilidade.

**Serverless compute** — não avaliada. Elimina a gestão de cluster, mas o modelo de custo
precisa ser comparado com o de job clusters SPOT para este volume.

**Manter always-on** — descartada. Pagar 24h por 1h de uso não se justifica em nenhum cenário
deste projeto.
