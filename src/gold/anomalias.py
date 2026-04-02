from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime


def detectar_anomalias(spark: SparkSession, storage_account: str) -> int:
    data_hoje   = datetime.now().strftime("%Y-%m-%d")
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/acoes/"
    gold_path   = f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/"

    print("Detectando anomalias...")

    df = spark.read.format("delta").load(silver_path) \
        .dropDuplicates(["date", "ticker"])

    window_ticker = Window.partitionBy("ticker")

    df_anomalias = df \
        .withColumn("media_variacao", F.avg("variacao_diaria_pct").over(window_ticker)) \
        .withColumn("std_variacao",   F.stddev("variacao_diaria_pct").over(window_ticker)) \
        .withColumn("zscore",
            F.round((F.col("variacao_diaria_pct") - F.col("media_variacao")) / F.col("std_variacao"), 4)) \
        .withColumn("anomalia",
            F.when(F.abs(F.col("zscore")) > 2, True).otherwise(False)) \
        .withColumn("tipo_anomalia",
            F.when(F.col("zscore") > 2,  "Alta Anormal")
            .when(F.col("zscore") < -2, "Queda Anormal")
            .otherwise("Normal")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .select(
            "date", "ticker", "empresa", "setor",
            "open", "close", "volume",
            "variacao_diaria_pct", "zscore",
            "anomalia", "tipo_anomalia",
            "data_processamento"
        )

    df_anomalias.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(gold_path)

    total = df_anomalias.filter(F.col("anomalia") == True).count()
    print(f"Gold anomalias gravado: {df_anomalias.count()} registros ({total} anomalias)")
    return total
