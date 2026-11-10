"""Frozen entry configuration options shared by strategy consumers."""

from __future__ import annotations

from itertools import product

REBOUND_NET_TARGET = "rebound_net_target"
# Candle highs are excluded from the supported strategy set for this experiment.
ENTRY_OPTIONS = {
    "policy": ("trend", "rebound", REBOUND_NET_TARGET),
    "selection": ("all", "active"),
    "allocation": ("equal", "priority"),
}


def valid_entry_options(config: object) -> bool:
    return (
        isinstance(config, dict)
        and config.keys() == ENTRY_OPTIONS.keys()
        and all(config[key] in choices for key, choices in ENTRY_OPTIONS.items())
    )


def strategies() -> list[dict[str, str]]:
    """Enumerate distinct configurations the current engine actually supports."""
    return [
        dict(zip(ENTRY_OPTIONS, values, strict=True))
        for values in product(*ENTRY_OPTIONS.values())
    ]
