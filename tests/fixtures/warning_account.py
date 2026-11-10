"""Importable warning fixture for process-backed account tests."""

import warnings
from typing import Any

from tools.async_layered_paper import AsyncLayeredPaper


class WarningAccount(AsyncLayeredPaper):
    def _process(self, event: dict[str, Any]) -> None:
        if self.pending:
            warnings.warn("synthetic account diagnostic", UserWarning, stacklevel=2)
        super()._process(event)
