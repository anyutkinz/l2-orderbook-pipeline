from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from l2_pipeline.book.types import PriceLevel
from l2_pipeline.sinks.parquet_sink import (
    SCHEMA_VERSION,
    BoundedRowQueue,
    SnapshotRow,
    _RotatingWriter,
    build_schema,
    rows_to_table,
)


def _ts_ns(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp() * 1e9)


def _row(
    ts_local_ns: int,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    *,
    exchange: str = "binance",
    symbol: str = "BTCUSDT",
    last_applied_id: int = 1,
) -> SnapshotRow:
    return SnapshotRow(
        exchange=exchange,
        symbol=symbol,
        ts_exchange_ms=ts_local_ns // 1_000_000,
        ts_local_ns=ts_local_ns,
        last_applied_id=last_applied_id,
        bids=[PriceLevel(Decimal(p), Decimal(q)) for p, q in bids],
        asks=[PriceLevel(Decimal(p), Decimal(q)) for p, q in asks],
    )


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


# P1: schema round-trip
def test_p1_schema_round_trip_byte_exact_prices(tmp_path: Path) -> None:
    depth_levels = 3
    schema = build_schema(depth_levels)
    rows = [
        _row(
            _ts_ns("2026-07-13T10:00:00"),
            bids=[("64175.83000000", "0.11117000"), ("64175.82000000", "0.00074000")],
            asks=[("64175.84000000", "9.38033000")],
            last_applied_id=101,
        ),
        _row(
            _ts_ns("2026-07-13T10:00:01"),
            bids=[],  # thin book: fewer than depth_levels -- must pad with nulls, not zeros
            asks=[("64175.84000000", "0.00000001")],  # smallest representable unit at scale=8
            last_applied_id=102,
        ),
    ]

    table = rows_to_table(rows, depth_levels, schema)
    path = tmp_path / "test.parquet"
    pq.write_table(table, str(path), compression="zstd")

    read_back = pq.read_table(str(path))
    assert read_back.schema.metadata[b"schema_version"] == SCHEMA_VERSION.encode()

    col = read_back.column
    assert col("bid_price_0").to_pylist() == [Decimal("64175.83000000"), None]
    assert col("bid_qty_0").to_pylist() == [Decimal("0.11117000"), None]
    assert col("bid_price_1").to_pylist() == [Decimal("64175.82000000"), None]
    assert col("bid_price_2").to_pylist() == [None, None]  # never populated -- null, not zero
    assert col("ask_price_0").to_pylist() == [Decimal("64175.84000000"), Decimal("64175.84000000")]
    assert col("ask_qty_0").to_pylist() == [Decimal("9.38033000"), Decimal("0.00000001")]
    assert col("last_applied_id").to_pylist() == [101, 102]
    assert col("exchange").to_pylist() == ["binance", "binance"]


# P2: rotation & atomicity
def test_p2_hour_boundary_creates_two_finalized_files(tmp_path: Path) -> None:
    clock = _FakeClock()
    writer = _RotatingWriter(tmp_path, "binance", "BTCUSDT", build_schema(1), 1, 300.0, clock)

    writer.write_batch([_row(_ts_ns("2026-07-13T10:59:58"), [("1", "1")], [])])
    writer.write_batch([_row(_ts_ns("2026-07-13T11:00:02"), [("2", "2")], [])])
    writer.close()

    hour10 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0000.parquet"
    hour11 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=11/part-0000.parquet"
    assert hour10.exists()
    assert hour11.exists()
    assert not list(tmp_path.rglob("*.tmp"))  # both finalized, nothing orphaned

    assert pq.read_table(str(hour10)).column("bid_price_0").to_pylist() == [Decimal("1")]
    assert pq.read_table(str(hour11)).column("bid_price_0").to_pylist() == [Decimal("2")]


def test_p2_checkpoint_interval_finalizes_within_a_single_hour(tmp_path: Path) -> None:
    clock = _FakeClock()
    checkpoint_interval = 300.0
    writer = _RotatingWriter(
        tmp_path, "binance", "BTCUSDT", build_schema(1), 1, checkpoint_interval, clock
    )
    base = _ts_ns("2026-07-13T10:00:00")

    writer.write_batch([_row(base, [("1", "1")], [])])
    clock.now += checkpoint_interval + 1.0  # elapse past the checkpoint interval, same hour
    writer.write_batch([_row(base + 1_000_000_000, [("2", "2")], [])])
    writer.close()

    part0 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0000.parquet"
    part1 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0001.parquet"
    assert part0.exists()
    assert part1.exists()
    assert pq.read_table(str(part0)).column("bid_price_0").to_pylist() == [Decimal("1")]
    assert pq.read_table(str(part1)).column("bid_price_0").to_pylist() == [Decimal("2")]


def test_p2_ungraceful_crash_loses_at_most_one_checkpoint_interval(tmp_path: Path) -> None:
    """Simulates kill -9 / power loss: the writer is never close()d. Asserts
    the guarantee this design actually provides -- data before the last
    checkpoint survives as a finalized, readable file; only data written
    *after* the last checkpoint (bounded by checkpoint_interval_seconds)
    is at risk, sitting in an orphaned, non-Hive-visible .tmp file."""
    clock = _FakeClock()
    checkpoint_interval = 300.0
    writer = _RotatingWriter(
        tmp_path, "binance", "BTCUSDT", build_schema(1), 1, checkpoint_interval, clock
    )
    base = _ts_ns("2026-07-13T10:00:00")

    # checkpoint 1: written and later superseded by checkpoint 2 -> finalized
    writer.write_batch([_row(base, [("1", "1")], [])])
    clock.now += checkpoint_interval + 1.0
    # this write finalizes checkpoint 1 and opens checkpoint 2
    writer.write_batch([_row(base + 1_000_000_000, [("2", "2")], [])])
    # checkpoint 2 gets more data but the process dies before its own
    # checkpoint interval elapses -- writer.close() is deliberately never called
    writer.write_batch([_row(base + 2_000_000_000, [("3", "3")], [])])

    directory = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10"
    finalized = sorted(directory.glob("part-*.parquet"))
    orphaned_tmp = sorted(directory.glob("*.tmp"))

    assert len(finalized) == 1  # only checkpoint 1 was ever finalized
    assert pq.read_table(str(finalized[0])).column("bid_price_0").to_pylist() == [Decimal("1")]

    assert len(orphaned_tmp) == 1  # checkpoint 2's data: at risk, but bounded
    # the orphaned .tmp is not a valid standalone Parquet file (no footer
    # written -- ParquetWriter.close() was never called) and is invisible
    # to any Hive-style glob reader (*.parquet), which is the actual
    # guarantee: "at most one checkpoint interval of data at risk, and a
    # partial file never masquerades as a valid final one"
    assert orphaned_tmp[0].suffix == ".tmp"
    assert not list(directory.glob("*.parquet.tmp.parquet"))  # sanity: no naming confusion


# P3: backpressure
def test_p3_backpressure_drops_oldest_and_bounds_enqueue_latency() -> None:
    """No separate async consumer task is needed to simulate "slow sink" --
    put() never awaits anything, so its latency is independent of whether
    or how fast anyone calls get(). Simply never draining during the puts
    below stands in for an arbitrarily slow/stalled writer."""
    queue = BoundedRowQueue(maxsize=3)
    rows = [_row(_ts_ns("2026-07-13T10:00:00"), [], [], last_applied_id=i) for i in range(5)]

    start = time.perf_counter()
    evicted = [queue.put(row) for row in rows]
    elapsed = time.perf_counter() - start

    assert evicted == [False, False, False, True, True]
    assert queue.rows_dropped == 2
    assert queue.qsize() == 3
    assert elapsed < 0.1  # sanity bound: 5 synchronous puts, no I/O, should be near-instant


async def test_p3_backpressure_newest_rows_survive_in_fifo_order() -> None:
    queue = BoundedRowQueue(maxsize=3)
    rows = [_row(_ts_ns("2026-07-13T10:00:00"), [], [], last_applied_id=i) for i in range(5)]
    for row in rows:
        queue.put(row)

    survivors = [await queue.get() for _ in range(3)]
    assert [row.last_applied_id for row in survivors] == [2, 3, 4]
