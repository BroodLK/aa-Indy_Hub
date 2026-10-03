"""Settings helpers for indy_hub."""

from __future__ import annotations

# Standard Library
import logging
from typing import Any

# Django
from django.conf import settings

logger = logging.getLogger(__name__)


def clean_setting(
    name: str,
    default_value: Any,
    min_value: int | None = None,
    max_value: int | None = None,
    required_type: type | None = None,
    choices: list | None = None,
) -> Any:
    """Validate a setting value and fall back to a safe value when invalid."""

    if default_value is None and not required_type:
        raise ValueError("You must specify a required_type for None defaults")

    if not required_type:
        required_type_2 = type(default_value)
    else:
        required_type_2 = required_type

    if not isinstance(required_type_2, type):
        raise TypeError("required_type must be a type when defined")

    if min_value is None and issubclass(required_type_2, int):
        min_value = 0

    if issubclass(required_type_2, int) and default_value is not None:
        if min_value is not None and default_value < min_value:
            raise ValueError("default_value can not be below min_value")
        if max_value is not None and default_value > max_value:
            raise ValueError("default_value can not be above max_value")

    if not hasattr(settings, name):
        return default_value

    dirty_value = getattr(settings, name)

    if dirty_value is None or (
        isinstance(dirty_value, required_type_2)
        and (min_value is None or dirty_value >= min_value)
        and (max_value is None or dirty_value <= max_value)
        and (choices is None or dirty_value in choices)
    ):
        return dirty_value

    if (
        isinstance(dirty_value, required_type_2)
        and min_value is not None
        and dirty_value < min_value
    ):
        logger.warning(
            "Setting %s is invalid. Using minimum value for now: %s",
            name,
            min_value,
        )
        return min_value

    if (
        isinstance(dirty_value, required_type_2)
        and max_value is not None
        and dirty_value > max_value
    ):
        logger.warning(
            "Setting %s is invalid. Using maximum value for now: %s",
            name,
            max_value,
        )
        return max_value

    logger.warning(
        "Setting %s is invalid. Using default for now: %s",
        name,
        default_value,
    )
    return default_value
