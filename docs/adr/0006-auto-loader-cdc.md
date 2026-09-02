# ADR 0006 — Auto Loader e Change Data Feed para ingestão incremental

**Status:** Aceito · 2026-09
**Implementação:** [`jobs/job_streaming.py`](../../jobs/job_streaming.py) · [`jobs/job_unity_catalog.py`](../../jobs/job_unity_catalog.py) · [`docs/cdc-clientes-implementation.md`](../cdc-clientes-implementation.md)

## Contexto

Dois problemas de incrementalidade distintos, no mesmo pipeline:

**1. Ler arquivos novos do Bronze.** O Event Hub deposita arquivos continuamente no ADLS.
Descobrir "o que chegou desde a última execução" listando o diretório funciona com centenas
de arquivos e degrada com milhões — o custo da listagem cresce com o total acumulado, não
com o que é novo.

**2. Propagar mudanças de Silver para Gold.** As tabelas Gold derivam de `silver.streaming`,
`silver.ordens` e `silver.clientes`. Reprocessar a tabela Silver inteira a cada execução para
recalcular o Gold desperdiça computação proporcional ao histórico, quando o que mudou foi
uma fração.

## Decisão

**Auto Loader (`cloudFiles`)** para a ingestão de arquivos no Bronze — rastreamento incremental
com checkpoint, inferência e evolução de schema automáticas:

```python
.format("cloudFiles")
.option("cloudFiles.format", "parquet")
.option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
```

**Delta Change Data Feed (CDF)** nas tabelas Silver que alimentam o Gold — leitura por versão,
processando apenas as linhas inseridas, atualizadas ou removidas desde o último ponto.

## Consequências

**Positivas**
- O custo de descoberta de arquivos passa a ser função do que é novo, não do acumulado —
  é o que permite a afirmação de escala do case sustentar volume 100×
- Evolução de schema não quebra a ingestão: coluna nova é absorvida em vez de derrubar o job
- Gold recalculado sobre o delta de mudanças, não sobre a tabela inteira
- CDF distingue `insert` de `update` e `delete`, viabilizando lógica incremental correta

**Negativas — o custo real desta escolha**
- **Estado externo ao dado**: checkpoints e `schemaLocation` passam a ser artefatos críticos.
  Perder o checkpoint significa reprocessar tudo ou pular dados; apagá-lo "para limpar" é um
  incidente, não uma faxina. É a armadilha operacional deste padrão
- Reprocessar um período exige intervenção deliberada (mover o checkpoint), não é só rodar de novo
- CDF aumenta o storage da tabela: as mudanças são persistidas além dos dados atuais
- Acoplamento a recursos proprietários do Databricks/Delta — Auto Loader não existe em Spark
  open source

**Mitigação adotada:** `job_streaming.py` usa `cloudFiles.maxFilesPerTrigger: 1`, limitando o
lote por gatilho e tornando o comportamento previsível na demonstração — em produção com volume
real, esse número deve subir.

## Alternativas consideradas

**Listar o diretório e filtrar por data de modificação** — descartada. É o padrão que o Auto
Loader existe para substituir; o custo de listagem no object storage cresce com o total de
arquivos e vira o gargalo antes de qualquer processamento.

**Partição por data de ingestão com leitura da partição do dia** — descartada como mecanismo
único. Funciona para o caso feliz, mas não trata chegada tardia nem reprocessamento parcial,
e não distingue update de insert.

**Full refresh diário do Gold** — descartada. Simples e sem estado, mas o custo cresce com o
histórico. Aceitável no volume atual; insustentável na premissa de escala do case.
