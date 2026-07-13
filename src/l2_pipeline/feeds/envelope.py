from __future__ import annotations

from dataclasses import dataclass

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import DiffEvent
from l2_pipeline.sinks.parquet_sink import SnapshotRow


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
    """A parsed DiffEvent plus the local receive timestamps and which
    exchange/instrument it came from.

    Two local clocks, captured back-to-back at receipt, deliberately never
    interchanged:
    - ts_local_ns (time.monotonic_ns()): immune to NTP adjustments and
      guaranteed non-decreasing, so it's the only correct choice for
      duration/latency math (see M6's processing_latency Histogram) --
      but its reference point is undefined (may be near-zero, relative to
      boot), so it is NOT a real calendar time and must never be fed to
      anything that interprets it as epoch time.
    - ts_wall_ns (time.time_ns()): real epoch time, for anything that
      needs to know *when* in the real world this happened -- Parquet
      Hive-partition dates/hours, cross-process/cross-machine comparison.

    (A real M5 bug, found by M7's stress test: _hour_key_for() was fed
    ts_local_ns and silently produced nonsense partition dates like
    "1970-01-19" on every run, live traffic included, since a monotonic
    clock reinterpreted as epoch time doesn't error, it just lies. Fixed
    by adding this field rather than repurposing ts_local_ns, since the
    latency-measurement use of the monotonic clock was and remains
    correct -- see DECISIONS.md M7.)

    Kept out of book.types so BookEngine's DiffEvent stays free of any
    feed-specific concept.
    """

    ts_local_ns: int
    ts_wall_ns: int
    instrument: InstrumentId
    event: DiffEvent
    ts_exchange_ms: int | None = None


def build_snapshot_row(
    engine: BookEngine,
    instrument: InstrumentId,
    ts_local_ns: int,
    ts_wall_ns: int,
    ts_exchange_ms: int | None,
) -> SnapshotRow:
    """Shared by both feed clients so "what a row looks like" can't drift
    between exchanges -- only called right after an ApplyStatus.APPLIED
    result, so last_applied_id is guaranteed set. bids/asks come from
    top_levels() with no explicit n, which defaults to the engine's own
    configured depth_levels -- the same depth the engine was constructed
    with, not a second, independently-specified value here.
    """
    last_applied_id = engine.last_applied_id
    assert last_applied_id is not None
    bids, asks = engine.top_levels()
    return SnapshotRow(
        exchange=instrument.exchange,
        symbol=instrument.symbol,
        ts_exchange_ms=ts_exchange_ms,
        ts_local_ns=ts_local_ns,
        ts_wall_ns=ts_wall_ns,
        last_applied_id=last_applied_id,
        bids=bids,
        asks=asks,
    )
