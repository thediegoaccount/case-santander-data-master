# Arquivo

Documentos de **registro histórico**: relatórios de refatorações concluídas e listas de tarefas
de um momento específico do projeto. Não descrevem o estado atual da arquitetura e não são
mantidos atualizados.

Ficam versionados porque registram o processo de construção — útil para entender *como* o
projeto chegou onde chegou — mas não devem ser usados como referência do sistema em produção.

| Documento | O que registra |
|---|---|
| `JOB_CLUSTER_UPGRADE.md` | Relatório da migração de cluster always-on para job clusters. A decisão e seus trade-offs vivem em [ADR 0003](../adr/0003-job-clusters.md) |
| `security-audit.md` | Auditoria de segurança de uma rodada específica |
| `logging-implementation.md` | Registro da substituição de `print()` por logging estruturado |
| `syspath-configuration.md` | Registro da padronização de `sys.path` nos jobs |
| `improvements-implementation.md` | Registro de melhorias implementadas em uma rodada |
| `databricks-pending-tasks.md` | Lista de pendências de um momento do projeto — parcialmente obsoleta |

> Estes arquivos citam jobs de carga em Azure SQL Database (`job_carga_sql_*.py`), removidos do
> projeto — ver [ADR 0002](../adr/0002-serving-lakehouse.md). As referências foram mantidas
> intactas por serem registro histórico: editá-las falsificaria o que de fato ocorreu.

**Para a documentação vigente, ver [`docs/`](../).**
