# from typing import Any
# import inspect
# import functools

# from attrs import define, field

# from tidy_tools.tidy._logger import _logger
# from tidy_tools.tidy._types import Functions, Objects


# def identity(obj: Any) -> Any:
#     """Return input object as is."""
#     return obj


# def metadata_factory() -> dict:
#     return dict(
#         name="No name provided",
#         description="No description provided"
#     )

# @define
# class TidyWorkFlow:
#     input: Objects
#     funcs: Functions
#     preprocess: Functions = field(default=identity)
#     postprocess: Functions = field(default=identity)
#     metadata: dict = field(factory=metadata_factory)

#     def run(self):
#         input = map(self.preprocess, self.input)
#         result = functools.reduce(
#             lambda init, func: transform(init, func),
#             self.funcs,
#             self.input
#         )
#         output = self.preprocess(result)
#         return output

    
#     def metadata(self):
#         return {
#             func.__name__: inspect.getdoc(func)
#             for func in self.funcs
#         }
