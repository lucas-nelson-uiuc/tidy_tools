import re

from attrs import define
from attrs import field


@define
class Regex:
    name: str = field(converter=(str.strip, str.lower))
    pattern: str = field()

    @pattern.validator
    def is_compiled_regex(self, attribute, value):
        try:
            re.compile(value)
        except Exception as e:
            raise e

    def construct_group(self) -> str:
        """
        Return name and pattern as captured group expression.

        Returns
        -------
        str
            Named capture group for regular expression object.
        """
        return rf"(?P<{self.name}>{self.pattern})"


def construct_pattern(*components: Regex, separator: str) -> str:
    """
    Return pattern containing group components.

    Parameters
    ----------
    *components : Regex
        Regular expressions to include in pattern.
    separator : str
        Value to separate components in pattern.

    Returns
    -------
    str
        Pattern created by `components` separated by `separator`.
    """
    return separator.join(map(Regex.construct_group, components))


# define regular expressions used in logging statements
SEPARATOR = Regex("separator", r"\s+\|\s+")
TIME = Regex("time", r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}")
LEVEL = Regex("level", r"[a-zA-Z]+")

# construct CONTENT as product of OPERATION and MESSAGE (as defined in TidyDataFrame)
OPERATION = Regex("operation", r"[a-z]+")
MESSAGE = Regex("message", r".*")
CONTENT = Regex(
    "content", rf"#>\s{OPERATION.construct_group()}:\s{MESSAGE.construct_group()}"
)

# define format, pattern for loguru-based functions
LOG_FORMAT: str = "{time:YYYY-MM-DD HH:mm:ss} | <level>{level:<8}</level> | {message}"
LOG_PATTERN: str = construct_pattern(TIME, LEVEL, CONTENT, separator=SEPARATOR.pattern)
