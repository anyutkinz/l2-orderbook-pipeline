from __future__ import annotations

from dataclasses import dataclass

from l2_pipeline.book.types import DiffEvent


@dataclass(frozen=True, slots=True)
class InstrumentId:
    """Canonical cross-exchange instrument identity. symbol stays in each
    exchange's own native format ("BTCUSDT" vs "BTC-USDT") -- no
    cross-exchange "these are the same instrument" mapping here, that's
    M5's job. Raw exchange-native symbols never leak past the feed
    client's own parsing layer except through this pairing.
    """

    exchange: str
    symbol: str


@dataclass(frozen=True, slots=True)
class TimestampedEvent:
    """A parsed DiffEvent plus the local receive timestamp and which
    exchange/instrument it came from.

    ts_local_ns is captured immediately after the websocket frame is
    received, before any parsing -- see the M3 DECISIONS.md entry on
    timestamp semantics for what this does and does not measure. Kept
    out of book.types so BookEngine's DiffEvent stays free of any
    feed-specific concept.
    """

    ts_local_ns: int
    instrument: InstrumentId
    event: DiffEvent
