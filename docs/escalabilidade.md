# Escalabilidade — preparação para crescimento de 100x

> **Afirmação:** esta pipeline foi arquitetada para absorver crescimento de até 100× no volume
> de eventos **sem redesign arquitetural** — apenas com tuning de parâmetros e recursos.

## O que a afirmação significa — e o que não significa

Escalabilidade é uma palavra que costuma ser usada sem contrato. O contrato aqui é explícito:

**Não significa**
- Que a pipeline rodaria hoje, sem ajuste nenhum, com 100× o volume — não rodaria
- Que não haverá necessidade de tuning conforme cresce — haverá
- Que é infinitamente escalável — tudo tem limite, e os desta arquitetura estão listados no final

**Significa**
- Nenhuma **mudança arquitetural** é necessária: os mesmos jobs, o mesmo modelo de dados,
  o mesmo grafo de dependências
- Escala **horizontalmente**: mais workers resultam em mais throughput, sem reescrita
- Não há **gargalo estrutural**: nenhum padrão que colapse com o aumento de volume
- O crescimento é **gradual**, não um penhasco — não existe um volume em que a pipeline
  deixe de funcionar de repente

## Cenários de volume

| | Hoje (baseline) | 100× |
|---|---|---|
| Eventos/dia | 10–100 M | 1–10 B |
| Eventos/segundo | ~1,15 K | ~115 K |
| Delta diário | 10–50 GB | 1–5 TB |
| Tabelas Gold | 15 | 15 (as mesmas) |
| Duração do pipeline | ~93 min | < 30 min (com paralelismo ampliado) |

## As três provas técnicas

A preparação não está numa afirmação — está em padrões concretos adotados e anti-padrões
deliberadamente evitados.

### 1. Partition pruning em vez de broadcast em tabelas grandes

O anti-padrão mais comum: fazer broadcast de uma tabela que cresce com o negócio.
Funciona no piloto e falha em produção, porque a tabela inteira precisa caber na memória
de cada executor.

```python
# ❌ Evitado — quebra quando silver.ordens cresce
F.broadcast(df_ordens)

# ✅ Implementado — filtro de partição antes do join
df_ordens.filter(F.col("data_ordem") >= data_corte)
```

O broadcast **é** usado neste projeto, mas apenas onde o tamanho é limitado por natureza e não
por volume: `df_perf` (9 linhas, uma por ticker) e `df_score`/`df_clientes` (< 1 MB). A distinção
entre "pequeno hoje" e "pequeno por construção" é o que separa o uso correto do anti-padrão.

### 2. Processamento incremental em vez de full scan

Auto Loader com checkpoint na ingestão e Change Data Feed na propagação Silver → Gold fazem o
custo de cada execução ser função **do que mudou**, não do histórico acumulado. É a diferença
entre custo constante e custo linear no tempo de vida da plataforma.

Detalhes e trade-offs em [ADR 0006](adr/0006-auto-loader-cdc.md).

### 3. Processamento em lote em vez de registro a registro

Nenhuma operação itera linha a linha (`collect()` seguido de loop, `foreach` com escrita
individual). Toda transformação é uma operação de DataFrame distribuída, e toda escrita é em
lote. Esse é o fator que decide se dobrar o volume dobra o tempo ou o multiplica.

## Benchmarks projetados

**Ingestão**

| Métrica | Hoje | 100× | Como absorve |
|---|---|---|---|
| Eventos/segundo | 1,15 K | 115 K | Event Hub escala por partição |
| Lag máximo | < 1 min | < 1 min | Inalterado |
| Storage bruto | 50 GB/dia | 5 TB/dia | Arquivamento + lifecycle policy |

**Processamento (camada Gold)**

| Métrica | Hoje | 100× | Como absorve |
|---|---|---|---|
| Join com `silver.ordens` | 3,3 GB | 330 GB | Partition pruning |
| Detecção de fraude | 10 s | 40 s | Repartition + mais workers |
| Clientes processados | 100 K | 10 M | Mesmo algoritmo |
| Memória por worker | 2 GB | 4–8 GB | Escala vertical do node |

Note que a detecção de fraude cresce 4× para um volume 100× maior — é o efeito de manter o
processamento distribuído e incremental.

## Caminho de crescimento

| Fase | Volume | Ajustes necessários |
|---|---|---|
| **1 — hoje** | 100 M eventos/dia | Configuração atual: 2–4 workers, job clusters SPOT |
| **2 — 10×** | 1 B eventos/dia | Workers 4→8; `maxFilesPerTrigger` acima de 1; janela de lookup de 30 para 15 dias |
| **3 — 100×** | 10 B eventos/dia | Workers 8→16; janela para 7 dias; arquivamento ativo do Bronze; avaliar cluster pools para reduzir overhead de inicialização |

Nenhuma fase exige reescrever job, remodelar tabela ou trocar tecnologia.

## Projeção de custo

| Volume | Workers | Job clusters | Always-on equivalente | Economia |
|---|---|---|---|---|
| 100 M/dia | 2–4 | ~$130/mês | ~$2.500/mês | ~$2.370 |
| 1 B/dia (10×) | 4–8 | ~$600/mês | ~$2.500/mês | ~$1.900 |
| 10 B/dia (100×) | 8–16 | ~$2.000/mês | ~$5.000/mês¹ | ~$3.000 |

¹ Em 100×, o cluster always-on também precisaria dobrar de tamanho.

O ponto relevante não é o valor absoluto, e sim o **formato da curva**: o custo cresce com o
trabalho realizado, não com o tempo decorrido. Mesmo em 100×, o modelo de job clusters
permanece cerca de 8× mais barato. Ver [ADR 0003](adr/0003-job-clusters.md).

## Riscos conhecidos e mitigação

| Risco em 100× | Probabilidade | Mitigação | Esforço |
|---|---|---|---|
| Janela de lookup ultrapassar 100 GB | Média | Reduzir de 30 para 7 dias | Baixo |
| Shuffle degradar | Média | Aumentar workers (8→16) | Baixo |
| Checkpoint crescer demais | Baixa | Reinicializar com marco controlado | Baixo |
| Storage do Bronze explodir | Média | Arquivamento + lifecycle policy | Médio |
| Overhead de startup dominar (+33%) | Alta | Cluster pools | Médio |

Nenhum exige redesign — todos são ajustes de parâmetro ou de recurso.

## Onde esta arquitetura quebra

Uma análise de escalabilidade sem limites declarados não é análise. Os limites conhecidos:

- **Latência**: o batch diário é o piso de frescor para as tabelas Gold históricas. Requisito de
  minutos para dados históricos exigiria mover cargas para o caminho de streaming — mudança
  arquitetural, não tuning
- **A camada de velocidade não é linearmente elástica**: `job_streaming_continuous` usa
  `maxFilesPerTrigger: 1` para previsibilidade na demonstração. Em volume real esse número
  precisa subir, e o dimensionamento correto ainda não foi validado sob carga
- **Duplicação de regras Lambda**: com o crescimento, manter as regras de fraude sincronizadas
  entre batch e streaming vira custo operacional recorrente ([ADR 0001](adr/0001-arquitetura-lambda.md))
- **Os números de 100× são projeção, não medição**: derivam das características conhecidas dos
  componentes (Auto Loader, Delta, Spark) e do comportamento observado no volume atual.
  Não houve teste de carga em 10 B eventos/dia
- **Concorrência de leitura**: a arquitetura foi otimizada para throughput de escrita e consulta
  analítica, não para alta concorrência de leitura ([ADR 0002](adr/0002-serving-lakehouse.md))

## Referências

- [ADR 0001 — Arquitetura Lambda](adr/0001-arquitetura-lambda.md)
- [ADR 0003 — Job clusters efêmeros](adr/0003-job-clusters.md)
- [ADR 0006 — Auto Loader e CDC](adr/0006-auto-loader-cdc.md)
- [`CLUSTER_CONFIG.md`](../CLUSTER_CONFIG.md) — dimensionamento prático de clusters
