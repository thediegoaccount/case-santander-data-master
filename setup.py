from setuptools import setup, find_packages

setup(
    name="case-santander",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "yfinance>=0.2.37",
        "requests>=2.31.0",
        "azure-eventhub>=5.15.1",
        # 15.4 nao casa com o cluster (spark_version 14.3.x em
        # databricks.yml). Databricks Connect exige que a major.minor do
        # cliente case com a DBR do cluster.
        "databricks-connect==14.3.*",
        "databricks-sdk>=0.20.0",
        "pandas>=2.0.0",
        "delta-spark>=3.1.0",
        "PyYAML>=6.0",
        "pytest>=7.4.0",
    ],
    python_requires=">=3.11",
    # python_wheel_task do databricks.yml resolve o entry_point pelos
    # metadados do wheel. Sem esta secao, `entry_point: job_x` nao resolve
    # e o job falha ao carregar.
    entry_points={
        "console_scripts": [
            "job_bronze_clientes = jobs.job_bronze_clientes:main",
            "job_exportar_amostra_streaming = jobs.job_exportar_amostra_streaming:main",
            "job_bronze_ordens = jobs.job_bronze_ordens:main",
            "job_silver_clientes = jobs.job_silver_clientes:main",
            "job_silver_ordens = jobs.job_silver_ordens:main",
            "job_corretora_analises = jobs.job_corretora_analises:main",
            "job_extracao_acoes = jobs.job_extracao_acoes:main",
            "job_extracao_bcb = jobs.job_extracao_bcb:main",
            "job_extracao_world_bank = jobs.job_extracao_world_bank:main",
            "job_gold_acoes_vs_cambio = jobs.job_gold_acoes_vs_cambio:main",
            "job_gold_anomalias = jobs.job_gold_anomalias:main",
            "job_gold_bcb = jobs.job_gold_bcb:main",
            "job_gold_fraude = jobs.job_gold_fraude:main",
            "job_gold_performance = jobs.job_gold_performance:main",
            "job_gold_world_bank = jobs.job_gold_world_bank:main",
            "job_lakehouse_monitoring = jobs.job_lakehouse_monitoring:main",
            "job_observabilidade = jobs.job_observabilidade:main",
            "job_scd = jobs.job_scd:main",
            "job_silver_acoes = jobs.job_silver_acoes:main",
            "job_silver_bcb = jobs.job_silver_bcb:main",
            "job_silver_world_bank = jobs.job_silver_world_bank:main",
            "job_streaming_continuous = jobs.job_streaming_continuous:main",
            "job_streaming_to_gold_continuous = jobs.job_streaming_to_gold_continuous:main",
            "job_unity_catalog = jobs.job_unity_catalog:main",
            "job_unity_catalog_schemas = jobs.job_unity_catalog_schemas:main",
        ]
    },
)
