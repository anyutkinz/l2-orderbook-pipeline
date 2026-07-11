from __future__ import annotations

from dataclasses import dataclass

from l2_pipeline.book.types import DiffEvent


@dataclass(frozen=True, slots=True)
class TimestampedEvent:
    """A parsed DiffEvent plus the local receive timestamp.

    ts_local_ns is captured immediately after the websocket frame is
    received, before any parsing -- see the M3 DECISIONS.md entry on
    timestamp semantics for what this does and does not measure. Kept
    out of book.types so BookEngine's DiffEvent stays free of any
    feed-specific concept.
    """

    ts_local_ns: int
    event: DiffEvent
