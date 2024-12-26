import functools
from typing import Any
from typing import Callable


def pipe(instance: Any, *functions: Callable) -> Any:
    """
    Apply arbitrary number of functions to `instance` in succession.

    Parameters
    ----------
    instance : Any
        Scalar object.
    *functions : Callable
        Functions.

    Returns
    -------
    Any
        Result of applying all function(s) to `instance`. Does not necessarily
        need to be the same type as it was at the start of the pipeline.

    Examples
    --------
    >>> # works with unary function
    >>> add_two = lambda x: x + 2
    >>>
    >>> # works with partial functions
    >>> add_n = lambda x, n: x + n
    >>>
    >>> # works with closures
    >>> def add_n(n: int) -> Callable:
    >>>     def closure(x):
    >>>         return x + n
    >>>     return closure
    >>>
    >>> result = pipe(12, add_two, add_n(10), add_n(-4))
    >>> assert result == 20
    """
    return functools.reduce(lambda init, func: func(init), functions, instance)


def compose(*functions: Callable) -> Callable:
    """
    Define and store pipeline as object to be executed.

    Unlike `pipe`, `compose` will not evaluate when initialized.
    These are two separate steps. See Examples for more details.

    Parameters
    ----------
    *functions : Callable
        Arbitrary number of functions to chain together.

    Returns
    -------
    Callable
        Nested function in order of function(s) passed.

    Examples
    --------
    >>> # works with unary function
    >>> add_two = lambda x: x + 2
    >>>
    >>> # works with partial functions
    >>> add_n = lambda x, n: x + n
    >>>
    >>> # works with closures
    >>> def add_n(n: int) -> Callable:
    >>>     def closure(x):
    >>>         return x + n
    >>>     return closure
    >>>
    >>> summation = compose(add_two, add_n(10), add_n(-4))
    >>> assert summation(12) == 20
    """
    return lambda instance: pipe(instance, *functions)
