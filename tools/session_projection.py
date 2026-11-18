"""Record the local availability clock once per shared journal entry."""

from __future__ import annotations

import hashlib
import math
import os
import struct
from pathlib import Path


class ProjectionClock:
    """The nth eight-byte timestamp belongs to the nth verified journal entry.

    One reader owns this private receipt file under the session lock. Appends survive
    a process restart. A batch is synced before exposing its account protection or status.
    """

    _record = struct.Struct(">d")

    def __init__(self, path: Path, *, replay: bool) -> None:
        self.replay = replay
        self.index = 0
        self.last = 0.0
        self.digest = hashlib.sha256()
        self.stream = path.open("rb" if replay else "a+b", buffering=0)
        self.length = os.fstat(self.stream.fileno()).st_size
        if self.length % self._record.size:
            self.stream.close()
            raise ValueError("Truncated projection clock")
        self.stream.seek(0)
        self.dirty = False

    def cut(self, index: int, now: float) -> float:
        if index != self.index + 1:
            raise ValueError("Projection clock sequence differs from journal")
        if self.index * self._record.size < self.length:
            raw = self.stream.read(self._record.size)
            (stamp,) = self._record.unpack(raw)
        else:
            if self.replay:
                raise ValueError("Missing recorded projection availability")
            stamp = max(now, self.last)
            raw = self._record.pack(stamp)
            if self.stream.write(raw) != len(raw):
                raise OSError("Short projection clock write")
            self.length += len(raw)
            self.dirty = True
        if not math.isfinite(stamp) or stamp < self.last:
            raise ValueError("Invalid projection clock order")
        self.index = index
        self.last = stamp
        self.digest.update(raw)
        return stamp

    def sync(self) -> None:
        if self.dirty:
            os.fsync(self.stream.fileno())
            self.dirty = False

    def seal(self) -> str:
        if self.index * self._record.size != self.length:
            raise ValueError("Projection receipts exceed verified journal")
        self.sync()
        return self.digest.hexdigest()

    def close(self) -> None:
        self.stream.close()
