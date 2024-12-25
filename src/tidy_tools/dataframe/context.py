import json
from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager

import attrs
from attrs import define
from attrs import field
from tidy_tools.dataframe.handler import TidyLogHandler


@define
class TidyContext:
    """
    Parameters supported by TidyDataFrame contextual operations.

    Attributes
    ----------
    name : str
        Name of DataFrame.
    count : bool
        Whether to perform count operations.
    display : bool
        Whether to perform display operations.
    limit : int
        Default all display operations to display only `limit` rows.
    log_handlers : list[TidyLogHandler]
        Sequence of TidyLogHandler instances to configure for TidyDataFrame.

    Examples
    --------
    >>> # assuming PySpark DataFrame is loaded
    >>> spark_data = ...
    >>>
    >>> # default configuration
    >>> default_context = TidyContext()
    >>> default_dataframe = TidyDataFrame(spark_data, default_context)
    >>>
    >>> # simple contextual configuration
    >>> basic_context = TidyContext(
    >>>     name="ContextDataFrame",
    >>>     count=False,
    >>>     limit=10
    >>> )
    >>> basic_dataframe = TidyDataFrame(spark_data, basic_context)
    >>>
    >>> # attaching log handlers
    >>> logging_context = TidyContext(
    >>>     name="LoggingHandlers",
    >>>     log_handlers=[
    >>>         TidyLogHandler(),
    >>>         TidyFileHandler("example.log"),
    >>>         TidyMemoHandler("serialized_example.log")
    >>>     ]
    >>> )
    >>> logging_dataframe = TidyDataFrame(spark_data, logging_context)
    """

    name: str = field(default="TidyDataFrame")
    count: bool = field(default=True)
    display: bool = field(default=True)
    limit: int = field(default=10)
    log_handlers: list[TidyLogHandler] = field(default=[TidyLogHandler()])

    @classmethod
    def load(cls, context: str | Path | dict) -> "TidyContext":
        """
        Create TidyContext from pre-configured context.

        Parameters
        ----------
        context : str | Path | dict
            Reference to object containing TidyContext attributes. If `str` or
            `Path`, the contents are loaded from the path provided. Once parsed
            from the path (or passed if a `dict`), a new TidyContext instance
            will be created.

        Returns
        -------
        TidyContext
            Instance of TidyContext configured with provided parameters.
        """
        if isinstance(context, (str, Path)):
            with open(context, "r") as fp:
                context = json.load(fp)
        return TidyContext(**context)

    def save(self) -> dict:
        """
        Save attributes as serialized JSON object.

        Returns
        -------
        dict
            Attributes of `TidyContext` instance as dictionary.
        """
        return attrs.asdict(self)

    def save_to_file(self, filepath: str | Path) -> None:
        """
        Save attributes to `filepath`.

        Parameters
        ----------
        filepath : str | Path
            File to save attributes. This file can be loaded using the
            `TidyContext.load(<filepath>)` method to create copies of the same
            instance.

        Returns
        -------
        None
            Stores output to file.
        """
        if not isinstance(filepath, Path):
            filepath = Path(filepath).resolve()
        filepath.write_text(self.save())


@contextmanager
def tidyworkflow(save: str | bool = False, **parameters) -> ContextManager:
    context = TidyContext(**parameters)
    try:
        yield context
    finally:
        if not save:
            pass
        if isinstance(save, bool):
            return attrs.asdict(context)
        if isinstance(save, str):
            file = Path(save).resolve()
            file.write_text(attrs.asdict(context))
