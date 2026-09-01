"""
Logging Centralizado para Jobs Databricks

Configura logging estruturado com níveis, timestamps e formatação.
Substitui print() por logging.info(), logging.error(), etc.
"""

import logging
import sys
from datetime import datetime


def setup_logging(job_name: str):
    """
    Configura logging para o job
    
    Args:
        job_name: Nome do job para identificação nos logs
    """
    # Configurar logger raiz
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Handler para stdout (Databricks logs)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    # Formato: [JOB_NAME] LEVEL [TIMESTAMP] Mensagem
    formatter = logging.Formatter(
        fmt=f"[{job_name}] %(levelname)s [%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger


def get_logger(job_name: str):
    """
    Retorna logger configurado para o job
    
    Args:
        job_name: Nome do job
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(job_name)
    
    # Configurar se ainda não configurado
    if not logger.handlers:
        return setup_logging(job_name)
    
    return logger


# Funções de conveniência para logging

def info(job_name: str, message: str):
    """Log em nível INFO"""
    logger = get_logger(job_name)
    logger.info(message)


def warning(job_name: str, message: str):
    """Log em nível WARNING"""
    logger = get_logger(job_name)
    logger.warning(message)


def error(job_name: str, message: str):
    """Log em nível ERROR"""
    logger = get_logger(job_name)
    logger.error(message)


def debug(job_name: str, message: str):
    """Log em nível DEBUG"""
    logger = get_logger(job_name)
    logger.debug(message)


def critical(job_name: str, message: str):
    """Log em nível CRITICAL"""
    logger = get_logger(job_name)
    logger.critical(message)
