"""
Retry Logic Framework

Implementa retry logic com backoff exponencial para operações externas.
"""

import time
from functools import wraps
from typing import Callable, Any, Type
from src.config.logging import info, error, warning


class RetryError(Exception):
    """Erro após todas as tentativas de retry"""
    pass


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Callable = None
):
    """
    Decorator para retry com backoff exponencial
    
    Args:
        max_attempts: Número máximo de tentativas
        delay: Delay inicial em segundos
        backoff: Fator de multiplicação do delay (exponencial)
        exceptions: Exceções que triggeram retry
        on_retry: Função callback a ser chamada em cada retry
    
    Example:
        @retry(max_attempts=3, delay=1, backoff=2, exceptions=(ConnectionError,))
        def extract_api_data():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        error(func.__name__, f"Retry esgotado após {max_attempts} tentativas")
                        raise RetryError(f"Retry esgotado após {max_attempts} tentativas") from e
                    
                    warning(func.__name__, f"Tentativa {attempt}/{max_attempts} falhou: {str(e)}")
                    warning(func.__name__, f"Retry em {current_delay}s...")
                    
                    if on_retry:
                        on_retry(attempt, e)
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def retry_on_connection_error(max_attempts: int = 3):
    """Retry específico para erros de conexão"""
    return retry(
        max_attempts=max_attempts,
        delay=1.0,
        backoff=2.0,
        exceptions=(ConnectionError, TimeoutError)
    )


def retry_on_http_error(max_attempts: int = 3, status_codes: tuple = (500, 502, 503, 504)):
    """Retry específico para erros HTTP"""
    from requests.exceptions import HTTPError
    
    def check_http_error(e: Exception) -> bool:
        if isinstance(e, HTTPError):
            return e.response.status_code in status_codes
        return False
    
    return retry(
        max_attempts=max_attempts,
        delay=1.0,
        backoff=2.0,
        exceptions=(HTTPError,)
    )


class RetryHandler:
    """Handler avançado de retry com callbacks"""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
    
    def on_retry_callback(self, attempt: int, exception: Exception):
        """Callback chamado em cada retry"""
        error(self.job_name, f"Retry {attempt}: {str(exception)}")
    
    def on_success_callback(self, result: Any):
        """Callback chamado em sucesso"""
        info(self.job_name, "Operação concluída com sucesso")
    
    def on_failure_callback(self, exception: Exception):
        """Callback chamado em falha final"""
        error(self.job_name, f"Operação falhou após retries: {str(exception)}")
