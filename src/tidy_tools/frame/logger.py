import sys
from pathlib import Path
from typing import TextIO

from attrs import define
from attrs import field
from loguru import logger


LOG_FORMAT: str = "{time:HH:mm:ss} | <level>{level:<8}</level> | {message}"


@define
class TidyLogHandler:
    sink: str | Path | TextIO = field(default=sys.stderr)
    level: str = field(default="INFO")
    format: str = field(default=LOG_FORMAT)
    diagnose: bool = field(default=False)
    catch: bool = field(default=False)


@define(kw_only=True)
class TidyFileHandler(TidyLogHandler):
    def __attrs_post_init__(self):
        self.sink = Path(self.sink).resolve()
        if self.sink.exists():
            logger.info(f"Removing existing file: {self.sink.name}")
            self.sink.unlink()
        if self.sink.suffix != ".log":
            raise ValueError("File must end with '.log' suffix")


@define(kw_only=True)
class TidyMemoHandler(TidyFileHandler):
    serialize: bool = field(default=True)

    def __attrs_post_init__(self):
        self.sink = Path("_memos/log").joinpath(self.sink)
