"""
Wrapper para APIs com isolamento por ambiente e rate limiting
"""

import time
import logging
from typing import Dict, Callable
from functools import wraps
from datetime import datetime

from src.config.environment import get_config, get_env, is_production

logger = logging.getLogger(__name__)


class APIRateLimiter:
    """Rate limiter por API e ambiente"""

    def __init__(self):
        self.request_counts: Dict[str, Dict[str, int]] = {}
        self.last_reset: Dict[str, datetime] = {}

    def _get_key(self, api_name: str) -> str:
        """Gera chave única por API e ambiente"""
        env = get_env()
        return f"{env}_{api_name}"

    def _reset_if_needed(self, key: str):
        """Reseta contador se passou 1 minuto"""
        if key not in self.last_reset:
            self.last_reset[key] = datetime.now()
            self.request_counts[key] = 0
            return

        elapsed = (datetime.now() - self.last_reset[key]).total_seconds()
        if elapsed >= 60:
            self.request_counts[key] = 0
            self.last_reset[key] = datetime.now()

    def check_rate_limit(self, api_name: str) -> bool:
        """Verifica se request pode ser feito"""
        key = self._get_key(api_name)
        self._reset_if_needed(key)

        config = get_config()
        limit = config["api_rate_limit"].get(api_name, 60)

        if self.request_counts[key] >= limit:
            logger.warning(f"[{get_env().upper()}] Rate limit atingido para {api_name}: {limit}/min")
            return False

        self.request_counts[key] += 1
        logger.info(f"[{get_env().upper()}] API {api_name}: {self.request_counts[key]}/{limit} requests/min")
        return True

    def wait_if_needed(self, api_name: str):
        """Aguarda se rate limit atingido"""
        key = self._get_key(api_name)
        self._reset_if_needed(key)

        config = get_config()
        limit = config["api_rate_limit"].get(api_name, 60)

        if self.request_counts[key] >= limit:
            wait_time = 60 - (datetime.now() - self.last_reset[key]).total_seconds()
            if wait_time > 0:
                logger.warning(f"[{get_env().upper()}] Aguardando {wait_time:.1f}s para {api_name}")
                time.sleep(wait_time)
                self.request_counts[key] = 0
                self.last_reset[key] = datetime.now()


# Instância global do rate limiter
rate_limiter = APIRateLimiter()


def api_call(api_name: str, func: Callable, *args, **kwargs):
    """
    Wrapper para chamadas de API com rate limiting e logging por ambiente

    Args:
        api_name: Nome da API (yahoo_finance, bcb, world_bank, kaggle)
        func: Função a ser executada
        *args, **kwargs: Argumentos da função

    Returns:
        Resultado da função
    """
    env = get_env()
    config = get_config()

    # Log de ambiente
    logger.info(f"[{env.upper()}] Iniciando chamada API: {api_name}")
    logger.info(f"[{env.upper()}] Ambiente isolado: {config['storage_account']}")
    logger.info(f"[{env.upper()}] Catalog: {config['catalog']}")

    if is_production():
        logger.warning(f"[{env.upper()}] *** PRODUÇÃO *** - Dados reais serão afetados")

    # Rate limiting
    rate_limiter.wait_if_needed(api_name)

    try:
        result = func(*args, **kwargs)
        logger.info(f"[{env.upper()}] API {api_name} executada com sucesso")
        return result
    except Exception as e:
        logger.error(f"[{env.upper()}] Erro na API {api_name}: {str(e)}")
        raise


def safe_api_call(api_name: str, func: Callable, *args, **kwargs):
    """
    Wrapper seguro que retorna None em caso de erro (não levanta exceção)
    Útil para pipelines onde falha em uma API não deve interromper outras
    """
    try:
        return api_call(api_name, func, *args, **kwargs)
    except Exception as e:
        logger.error(f"[{get_env().upper()}] API {api_name} falhou silenciosamente: {str(e)}")
        return None


# Decorator para rate limiting
def with_rate_limit(api_name: str):
    """Decorator para aplicar rate limiting em funções"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return api_call(api_name, func, *args, **kwargs)

        return wrapper

    return decorator
