from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Sequence

import attrs
from attrs import converters
from attrs import validators
from loguru import logger


def remove_none(*elements: Any) -> Sequence:
    """
    Remove instances of `None` from `elements`.

    Parameters
    ----------
    *elements : Any
        Object of any type that may or may not be `None`.

    Returns
    -------
    Sequence
        Collection of items not identical to `None`.
    """
    return [elem for elem in elements if elem is not None]


def transform_model(reference) -> Callable:
    """
    Coerce model according to `reference` model.

    Parameters
    ----------
    reference : TidyDataModel
        Model to apply updates.

    Returns
    -------
    Callable
        Field transformer hook compatible with attrs classes.
    """

    def closure(cls, fields: list[attrs.Attribute]) -> list[attrs.Attribute]:
        """
        Create queue of fields to transform.

        Parameters
        ----------
        cls : TidyDataModel
            Attrs class.
        fields : list[attrs.Attribute]
            Fields from `cls`.

        Returns
        -------
        list[attrs.Attribute]
            Merged sequence of fields between `cls` and `reference`.
        """

        reference_fields = attrs.fields(reference)
        fields_transformed = []

        for fld in fields:
            if fld.name not in attrs.fields_dict(cls):
                logger.warning(
                    "Ignoring `{fld.name}` - not defined in inherited model."
                )
                continue

            if hasattr(
                reference, fld.alias or ""
            ):  # empty string to prevent error in hasattr
                field_reference = getattr(reference_fields, fld.alias)
                fields_transformed.append(
                    fld.evolve(
                        default=fld.default or field_reference.default,
                        converter=converters.pipe(
                            *remove_none(field_reference.converter, fld.converter)
                        ),
                        validator=validators.and_(
                            *remove_none(field_reference.validator, fld.validator)
                        ),
                        type=fld.type or field_reference.type,
                        metadata=field_reference.metadata | fld.metadata,
                    )
                )
            else:
                fields_transformed.append(fld)
        return fields_transformed

    return closure
