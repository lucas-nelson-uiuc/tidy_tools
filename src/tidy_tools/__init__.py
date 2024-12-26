import sys

from loguru import logger


__version__ = "0.3.1"


# upon import:
#   - remove existing logger
#   - update logging format
#   - log welcome message for documentation
LOG_FORMAT: str = "{time:HH:mm:ss} | <level>{level:<8}</level> | {message}"
logger.remove()
logger.add(sys.stderr, format=LOG_FORMAT)
logger.info(
    f"Tidy Tools imported: {__version__}. See https://lucas-nelson-uiuc.github.io/tidy_tools/ for more details."
)
