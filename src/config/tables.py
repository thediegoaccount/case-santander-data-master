"""
Nomes qualificados de tabelas e schemas do Unity Catalog.

Fonte única da verdade. Antes, 219 referências hardcoded a
`case_santander.bronze/silver/gold` espalhadas por 24 arquivos ignoravam o
`schema_prefix` de src/config/environment.py — hk e prod escreviam nas
MESMAS tabelas, enquanto os paths ADLS eram isolados por storage account.
Metadados compartilhados apontando para dados de storages diferentes é a
pior combinação possível.

Uso:

    from src.config.tables import SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD

    spark.sql(f"SELECT * FROM {SCHEMA_SILVER}.<nome_da_tabela>")

Os nomes são resolvidos na importação, a partir de ENVIRONMENT.

Nota: o exemplo acima usa `<nome_da_tabela>` de propósito, nunca um nome real.
Como todo job importa este módulo, um nome real escrito aqui é contado como
leitura daquela tabela por qualquer varredura de linhagem, e ela passa a
aparecer como dependência de praticamente todos os jobs. Mantenha o
placeholder ao editar este docstring.
"""

from .environment import EnvironmentConfig

_config = EnvironmentConfig.get_config()

CATALOG: str = _config["catalog"]
SCHEMA_PREFIX: str = _config["schema_prefix"]

# case_santander.hk_bronze / case_santander.prod_bronze
SCHEMA_BRONZE: str = f"{CATALOG}.{SCHEMA_PREFIX}bronze"
SCHEMA_SILVER: str = f"{CATALOG}.{SCHEMA_PREFIX}silver"
SCHEMA_GOLD: str = f"{CATALOG}.{SCHEMA_PREFIX}gold"

SCHEMAS = (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD)


def schema_fqn(layer: str) -> str:
    """Nome qualificado do schema de uma camada ('bronze'|'silver'|'gold')."""
    mapa = {"bronze": SCHEMA_BRONZE, "silver": SCHEMA_SILVER, "gold": SCHEMA_GOLD}
    if layer not in mapa:
        raise ValueError(f"Camada inválida: {layer}. Opções: {sorted(mapa)}")
    return mapa[layer]


def table_fqn(layer: str, name: str) -> str:
    """Nome qualificado de uma tabela: catalog.<prefixo><camada>.<nome>."""
    return f"{schema_fqn(layer)}.{name}"


def register_external_table(spark, layer: str, name: str, path: str) -> str:
    """
    Registra o path Delta como tabela externa no Unity Catalog.

    Resolve a fratura entre as duas convenções de storage do projeto: as
    transformações silver gravavam só em path (`.save(...)`), mas os
    consumidores gold liam via `FROM case_santander.silver.X` — tabela que
    ninguém criava. Três tasks gold morriam com TABLE_OR_VIEW_NOT_FOUND.

    Quem escreve o dado registra a tabela, então a ordem fica correta por
    construção — sem depender de um job de catálogo rodar antes.
    """
    fqn = table_fqn(layer, name)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn(layer)}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{path}'")
    return fqn
