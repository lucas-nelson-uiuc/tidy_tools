import sys

from loguru import logger

logger.remove()
logger.add(sys.stderr, format="{time:HH:mm} | <level>{level}</level> | {message}")


def _logger(message: str, level: str = "info") -> None:
    if not hasattr(logger, level):
        raise ValueError(f"Logger does not have {level=}")
    getattr(logger, level)(message)
