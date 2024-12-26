import sys

from loguru import logger
from tidy_tools.functions import merge
from tidy_tools.functions import reader

__all__ = ["reader", "merge"]
__version__ = "0.3.1"


LOG_FORMAT: str = "{time:HH:mm:ss} | <level>{level:<8}</level> | {message}"
logger.remove()
logger.add(sys.stderr, format=LOG_FORMAT)
