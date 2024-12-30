import sys

from loguru import logger
from tidy_tools.parser import LOG_FORMAT


__version__ = "0.3.3"


# upon import:
#   - remove existing logger
#   - update logging format
#   - log welcome message for documentation
logger.remove()
logger.add(sys.stderr, format=LOG_FORMAT)
logger.info(
    f"Tidy Tools imported: {__version__}. See https://lucas-nelson-uiuc.github.io/tidy_tools/ for more details."
)
