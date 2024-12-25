import sys
from pathlib import Path
from typing import TextIO

from attrs import define
from attrs import field
from loguru import logger


LOG_FORMAT: str = "{time:HH:mm:ss} | <level>{level:<8}</level> | {message}"


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


@define(kw_only=True)
class TidyFileHandler(TidyLogHandler):
    """
    Log handler for file streams.

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
    serialize : bool
        Whether the logged message and its records should be first converted
        to a JSON string before being sent to the sink.
    """

    serialize: bool = field(default=True)

    def __attrs_post_init__(self):
        self.sink = Path("_memos/log").joinpath(self.sink)
