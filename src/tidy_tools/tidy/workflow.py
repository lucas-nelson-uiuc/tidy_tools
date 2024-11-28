from attrs import define

from tidy_tools.tidy._types import Functions, Objects


@define
class TidyWorkFlow:
    input: Objects
    funcs: Functions
