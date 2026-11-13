"""Canonical JSON for the versioned paper-engine journal."""

import json
from dataclasses import asdict
from decimal import Decimal
from enum import StrEnum
from typing import cast, get_args, get_origin, get_type_hints

from .contracts import AccountResult, AccountState, Cut, MarketRules

Record = AccountResult | AccountState | Cut | MarketRules


def _decimal(value: object) -> str:
    if not isinstance(value, Decimal):
        raise TypeError(f"Unsupported journal value: {type(value).__name__}")
    return str(value)


def _check(value: object, annotation: object) -> None:
    if annotation in (str | None, Decimal | None):
        if value is None:
            return
        _check(value, get_args(annotation)[0])
        return
    if get_origin(annotation) is tuple and isinstance(value, tuple):
        item_type, ellipsis = get_args(annotation)
        if ellipsis is Ellipsis:
            for item in value:
                _check(item, item_type)
            return
    if type(value) is annotation:
        if isinstance(annotation, type):
            for name, field_type in get_type_hints(annotation).items():
                _check(getattr(value, name), field_type)
        return
    raise ValueError(f"Invalid journal value for {annotation}")


def validate(value: Record) -> None:
    if type(value) not in get_args(Record):
        raise ValueError("Unsupported journal record")
    _check(value, type(value))


def encode(value: Record) -> str:
    validate(value)
    return json.dumps(asdict(value), default=_decimal, separators=(",", ":"), sort_keys=True)


def _decode(value: object, annotation: object) -> object:
    if annotation == (str | None):
        if value is None or type(value) is str:
            return value
    elif annotation == (Decimal | None):
        return None if value is None else _decode(value, Decimal)
    elif annotation is Decimal and isinstance(value, str):
        return Decimal(value)
    elif annotation in (bool, int, str) and type(value) is annotation:
        return value
    elif isinstance(annotation, type) and issubclass(annotation, StrEnum):
        if isinstance(value, str):
            return annotation(value)
    elif get_origin(annotation) is tuple and isinstance(value, list):
        item_type, ellipsis = get_args(annotation)
        if ellipsis is Ellipsis:
            return tuple(_decode(item, item_type) for item in value)
    elif isinstance(annotation, type) and isinstance(value, dict):
        hints = get_type_hints(annotation)
        if value.keys() == hints.keys():
            return annotation(**{
                name: _decode(value[name], field_type) for name, field_type in hints.items()
            })
    raise ValueError(f"Invalid journal value for {annotation}")


def decode[T: Record](raw: str, annotation: type[T]) -> T:
    return cast(T, _decode(json.loads(raw), annotation))
