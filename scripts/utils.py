import functools
import logging

# Configura el sistema de logs para mostrar mensajes con hora, nivel e información
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def log_step(func):
 #Decorador que registra el inicio y fin de la ejecución de una función.
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Muestra un mensaje antes de ejecutar la función
        logger.info(f"Iniciando {func.__name__}()")
        
        result = func(*args, **kwargs)
        
        # Muestra un mensaje al finalizar la ejecución
        logger.info(f"Finalizó {func.__name__}()")
        return result

    return wrapper

