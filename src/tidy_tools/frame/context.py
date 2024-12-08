from pathlib import Path

import attrs
from attrs import define
from attrs import field
from tidy_tools.frame.logger import TidyLogHandler


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
