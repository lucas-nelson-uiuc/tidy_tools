from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager

import attrs
from attrs import define
from attrs import field
from tidy_tools.dataframe.handler import TidyLogHandler


@define
class TidyContext:
    """Parameters supported by TidyDataFrame contextual operations."""

    name: str = field(default="TidyDataFrame")
    count: bool = field(default=True)
    display: bool = field(default=True)
    limit: int = field(default=10)
    log_handlers: list[TidyLogHandler] = field(default=[TidyLogHandler()])

    def save(self) -> dict:
        return attrs.asdict(self)

    def save_to_file(self, filepath: str | Path) -> None:
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
