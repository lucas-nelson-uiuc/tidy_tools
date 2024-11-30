from attrs import define
from tidy_tools.frame._types import Functions
from tidy_tools.frame._types import Objects


@define
class TidyWorkFlow:
    input: Objects
    funcs: Functions
