"""Write legacy Korean comparison events using the preserved paper JSON format."""

from collections.abc import Callable
from datetime import datetime
from typing import Any, TextIO

from papertrade.jsonio import paper_json


def write_event(
    events: TextIO, clock: Callable[[], datetime], kind: str, **fields: Any
) -> None:
    events.write(paper_json({"kind": kind, "recorded_at": clock(), **fields}) + "\n")
