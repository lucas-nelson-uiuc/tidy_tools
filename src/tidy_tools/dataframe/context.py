import json
from pathlib import Path

import attrs
from attrs import define
from attrs import field
from loguru import logger
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

    def save(self, filepath: str | Path | None = None) -> dict | None:
        """
        Save attributes as serialized object.

        Parameters
        ----------
        filepath : str | Path | None
            Optional path to save context configuration. This file can be
            loaded using the `TidyContext.load(<filepath>)` method to
            deterministically create copies of the same instance.

        Returns
        -------
        dict | None
            If no `filepath` is provided, the attributes of `TidyContext`
            instance as dictionary. Else, write configurations to `filepath`.

        Raises
        ------
        Exception
            If there is an error while writing to `filepath`.
        """
        context = attrs.asdict(self)
        if filepath is None:
            return context
        if not isinstance(filepath, Path):
            filepath = Path(filepath).resolve()
        try:
            with open(filepath, "w") as fp:
                json.dump(self.save(), fp)
            logger.success(f"Context stored at: {filepath}")
        except Exception as e:
            logger.error(f"Error writing context to: {filepath}")
            raise e
