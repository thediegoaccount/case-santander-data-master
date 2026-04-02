from setuptools import setup, find_packages

setup(
    name="case-santander",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "yfinance>=0.2.37",
        "requests>=2.31.0",
        "azure-eventhub>=5.15.1",
        "databricks-connect==15.4",
        "databricks-sdk>=0.20.0",
        "pytest>=7.4.0",
    ],
    python_requires=">=3.11",
)
