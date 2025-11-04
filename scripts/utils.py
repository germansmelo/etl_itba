import functools
import logging
from typing import Callable, TypeVar

Func = TypeVar("Func", bound=Callable)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def log_step(func: Func) -> Func:
    """Decorator that logs start and end of a function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting {func.__name__}()")
        result = func(*args, **kwargs)
        logger.info(f"Finished {func.__name__}()")
        return result
    return wrapper  # type: ignore
