"""
Helpers Delta compartilhados.
"""

from delta.tables import DeltaTable

from src.config.logging import info


def merge_ou_cria(spark, df, fqn: str, chave: str, contexto: str = "merge_ou_cria") -> str:
    """
    Faz MERGE (upsert) do DataFrame na tabela, ou cria na primeira carga.

    O bloco try/MERGE/except-primeira-carga estava copiado quatro vezes em
    jobs/job_clientes_ordens.py, uma para cada tabela. Centralizado aqui para
    que os jobs por tabela fiquem finos e a semântica de upsert seja idêntica
    em todos.

    Args:
        df: DataFrame de origem
        fqn: nome qualificado da tabela destino
        chave: coluna de casamento (target.<chave> = source.<chave>)

    Returns:
        o fqn da tabela escrita
    """
    try:
        alvo = DeltaTable.forName(spark, fqn)
        alvo.alias("target") \
            .merge(df.alias("source"), f"target.{chave} = source.{chave}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        info(contexto, f"[OK] {fqn} atualizado via MERGE (chave: {chave})")
    except Exception as e:
        # Primeira carga: a tabela ainda não existe. Qualquer outro erro sobe —
        # engolir exceção aqui esconderia falha real de escrita.
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            df.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(fqn)
            info(contexto, f"[OK] {fqn} primeira carga")
        else:
            raise

    return fqn
