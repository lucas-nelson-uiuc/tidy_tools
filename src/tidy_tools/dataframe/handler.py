import sys
from pathlib import Path
from typing import TextIO

import attrs
from attrs import define
from attrs import field
from loguru import logger
from tidy_tools.parser import LOG_FORMAT
from tidy_tools.parser import LOG_PATTERN


@define
class TidyLogHandler:
    """
    Generic log handler for system error streams.

    Attributes
    ----------
    sink : str | Path | TextIO
        Destination for receiving logging messages.
    level : str
        Minimum level to trace in logs. See `loguru` for more details.
    format : str
        Template used for logged messages.
    diagnose : bool
        Whether the exception trace should display the variables values
        to eases the debugging.
    catch : bool
        Whether errors occurring while sink handles logs messages should
        be automatically caught. If True, an exception message is displayed
        on sys.stderr but the exception is not propagated to the caller,
        preventing your app to crash.
    """

    sink: str | Path | TextIO = field(default=sys.stderr)
    level: str = field(default="INFO")
    format: str = field(default=LOG_FORMAT)
    diagnose: bool = field(default=False)
    catch: bool = field(default=False)

    @property
    def pattern(self, pattern: str = LOG_PATTERN) -> str:
        """
        Pattern for logging instance.

        Parameters
        ----------
        pattern : str
            Template used for parsing logged messages.

        Returns
        -------
        str
            Pattern for logging instance.
        """
        return pattern

    def save(self) -> dict:
        """
        Return configurations as dict.

        Returns
        -------
        dict
            Collection of attributes for context.
        """
        return attrs.asdict(self)

    def summarize(self) -> dict:
        """
        Summarize contents at `source`.

        Returns
        -------
        dict
            Statistics of contents at `source`.
        """
        return dict()


@define(kw_only=True)
class TidyFileHandler(TidyLogHandler):
    """Log handler for file streams."""

    def __attrs_post_init__(self):
        self.sink = Path(self.sink).resolve()
        if self.sink.exists():
            logger.info(f"Removing existing file: {self.sink.name}")
            self.sink.unlink()
        if self.sink.suffix != ".log":
            raise ValueError("File must end with '.log' suffix")


@define(kw_only=True)
class TidyMemoHandler(TidyFileHandler):
    """
    Log handler for serialized streams.

    Attributes
    ----------
    serialize : bool
        Whether the logged message and its records should be first converted
        to a JSON string before being sent to the sink.
    """

    serialize: bool = field(default=True)

    def __attrs_post_init__(self):
        self.sink = Path("_memos/log").joinpath(self.sink)
